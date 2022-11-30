package com.github.emitskevich.topology;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.core.utils.SimpleScheduler;
import com.github.emitskevich.kafka.config.ConsumerConfig;
import com.github.emitskevich.kafka.config.ProducerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

  private final SimpleScheduler scheduler = SimpleScheduler.createSingleThreaded();
  private final Duration delay;
  protected final AppConfig appConfig;

  private KafkaConsumer<byte[], byte[]> consumer;
  private KafkaProducer<byte[], byte[]> producer;
  private final String sourceTopic;
  private final String destinationTopic;

  protected ConsumerTopology(AppConfig appConfig, String sourceTopic, String destinationTopic,
      Duration delay) {
    this.appConfig = appConfig;
    this.sourceTopic = sourceTopic;
    this.destinationTopic = destinationTopic;
    this.delay = delay;
  }

  @Override
  public void initialize(ServerContext context) throws ExecutionException, InterruptedException {
    String applicationId = appConfig.getString("application.name");

    ConsumerConfig consumerConfig = new ConsumerConfig(appConfig);
    this.consumer = new KafkaConsumer<>(consumerConfig.packConfig("source", applicationId));

    ProducerConfig producerConfig = new ProducerConfig(appConfig);
    this.producer = new KafkaProducer<>(producerConfig.packConfig("destination"));
  }

  @Override
  public void start() {
    consumer.subscribe(List.of(sourceTopic));
    scheduler.scheduleWithFixedDelay(this::iteration, delay);
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
    consumer.close();
    producer.close();
  }

  private record PartitionBatch(TopicPartition topicPartition, int size, long first, long last) {

  }

  private record PartitionFuture(PartitionBatch batch, Future<RecordMetadata> future) {

  }
}
