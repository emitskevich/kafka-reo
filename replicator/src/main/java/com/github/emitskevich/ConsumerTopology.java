package com.github.emitskevich;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.core.utils.SimpleScheduler;
import com.github.emitskevich.kafka.KafkaClients;
import com.github.emitskevich.kafka.KafkaClients.ConsumerPair;
import com.github.emitskevich.utils.LagMonitor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConsumerTopology implements Initializable, Shutdownable, Startable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTopology.class);
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

  protected final String sourceName;
  private final String destinationName;
  protected final LagMonitor lagMonitor;

  private final SimpleScheduler scheduler = SimpleScheduler.createSingleThreaded();
  private final Duration delay;
  protected final AppConfig appConfig;

  private KafkaConsumer<byte[], byte[]> consumer;
  private ConsumerRebalanceListener consumerRebalanceListener;
  private KafkaProducer<byte[], byte[]> producer;
  protected String sourceTopic;
  private String destinationTopic;

  protected ConsumerTopology(AppConfig appConfig, String sourceName, String destinationName,
      Duration delay) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.lagMonitor = new LagMonitor(sourceName, destinationName, appConfig);
    this.delay = delay;
    this.appConfig = appConfig;
  }

  @Override
  public void initialize(ServerContext context) throws ExecutionException, InterruptedException {
    KafkaClients kafkaClients = context.getInstance(KafkaClients.class);
    String applicationId = appConfig.getString("application.name");

    ConsumerPair<byte[], byte[]> consumerPair = kafkaClients
        .buildConsumerPair(sourceName, applicationId);
    this.consumer = consumerPair.consumer();
    this.consumerRebalanceListener = consumerPair.listener();

    this.producer = kafkaClients.getProducer(destinationName);
    this.sourceTopic = appConfig.getString("kafka.source.name");
    this.destinationTopic = appConfig.getString("kafka.destination.name");

    lagMonitor.setGroupId(applicationId);
    lagMonitor.setAssignmentSupplier(
        () -> kafkaClients.getAssignment(sourceName, applicationId)
    );
    lagMonitor.initialize(context);
  }

  @Override
  public void start() {
    lagMonitor.reportLag();

    consumer.subscribe(List.of(sourceTopic), consumerRebalanceListener);

    scheduler.scheduleWithFixedDelay(this::iteration, delay);
    lagMonitor.start();
  }

  private void iteration() {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);

    Set<TopicPartition> activePartitions = records.partitions();
    List<PartitionFuture> futuresToWait = new ArrayList<>(activePartitions.size());

    for (TopicPartition topicPartition : activePartitions) {
      List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(topicPartition);
      PartitionFuture future = produceAndGetLastFuture(topicPartition, partitionRecords);
      if (future != null) {
        futuresToWait.add(future);
      }
    }

    for (PartitionFuture future : futuresToWait) {
      PartitionBatch batch = future.batch();
      try {
        future.future().get();
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Exception producing {}, all batches will be retried", batch, e);
        return;
      }
    }

    consumer.commitSync();
  }

  @Nullable
  private PartitionFuture produceAndGetLastFuture(TopicPartition topicPartition,
      List<ConsumerRecord<byte[], byte[]>> partitionRecords) {
    if (partitionRecords.isEmpty()) {
      return null;
    }

    int batchSize = partitionRecords.size();
    long firstOffset = partitionRecords.get(0).offset();
    long lastOffset = partitionRecords.get(batchSize - 1).offset();
    PartitionBatch batch = new PartitionBatch(topicPartition, batchSize, firstOffset, lastOffset);

    PartitionFuture future = null;
    for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
      byte[] key = modifyKey(topicPartition, record);
      byte[] value = modifyValue(topicPartition, record);
      var producerRecord = new ProducerRecord<>(destinationTopic, key, value);
      Future<RecordMetadata> recordFuture = producer.send(producerRecord);
      future = new PartitionFuture(batch, recordFuture);
    }
    return future;
  }

  @Nullable
  protected byte[] modifyKey(TopicPartition topicPartition, ConsumerRecord<byte[], byte[]> record) {
    return record.key();
  }

  protected byte[] modifyValue(TopicPartition topicPartition,
      ConsumerRecord<byte[], byte[]> record) {
    return record.value();
  }

  @Override
  public void shutdown() {
    scheduler.stop();
    consumer.unsubscribe();
    lagMonitor.reportLag();
    lagMonitor.shutdown();
  }

  private record PartitionBatch(TopicPartition topicPartition, int size, long first, long last) {

  }

  private record PartitionFuture(PartitionBatch batch, Future<RecordMetadata> future) {

  }
}
