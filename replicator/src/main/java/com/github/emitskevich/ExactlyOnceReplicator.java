package com.github.emitskevich;

import com.adx.proto.Kafka.ReplicatedKey;
import com.adx.proto.Kafka.ReplicatedValue;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.core.utils.StringUtils;
import com.github.emitskevich.deduplication.KeySequenceDeduplicationWrapper;
import com.github.emitskevich.serde.ReplicatedKeySerde;
import com.github.emitskevich.serde.ReplicatedValueSerde;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.jetbrains.annotations.Nullable;

public class ExactlyOnceReplicator implements Initializable, Startable, Shutdownable {

  private final List<String> sourceNames;
  private final AppConfig appConfig;
  private final String intermediateName;
  private final String destinationName;

  private List<Replicator> internalReplicators;
  private Deduplicator deduplicator;

  public ExactlyOnceReplicator(AppConfig appConfig, List<String> sourceNames,
      String intermediateName, String destinationName) {
    this.sourceNames = sourceNames;
    this.appConfig = appConfig;
    this.intermediateName = intermediateName;
    this.destinationName = destinationName;
  }

  @Override
  public void initialize(ServerContext context) throws Exception {
    this.internalReplicators = sourceNames
        .stream()
        .map(sourceName -> new Replicator(appConfig, sourceName, intermediateName))
        .collect(Collectors.toList());
    for (Replicator internalReplicator : internalReplicators) {
      internalReplicator.initialize(context);
    }
    this.deduplicator = new Deduplicator(appConfig, intermediateName, destinationName);
    deduplicator.initialize(context);
  }

  protected boolean filter(TopicPartition topicPartition, ConsumerRecord<byte[], byte[]> record) {
    return true;
  }

  @Override
  public void start() {
    internalReplicators.forEach(ConsumerTopology::start);
    deduplicator.start();
  }

  @Override
  public void shutdown() {
    internalReplicators.forEach(ConsumerTopology::shutdown);
    deduplicator.shutdown();
  }

  private class Replicator extends ConsumerTopology {

    private String sourceHosts;

    private Replicator(AppConfig appConfig, String sourceName, String destinationName) {
      super(appConfig, sourceName, destinationName, Duration.ofMillis(1));
    }

    @Override
    public void initialize(ServerContext context) throws ExecutionException, InterruptedException {
      super.initialize(context);
      this.sourceHosts = appConfig.getString("kafka." + sourceName + ".bootstrap-servers");
    }

    @Override
    protected boolean filter(TopicPartition topicPartition, ConsumerRecord<byte[], byte[]> record) {
      return ExactlyOnceReplicator.this.filter(topicPartition, record);
    }

    @Override
    protected @Nullable byte[] modifyKey(TopicPartition topicPartition,
        ConsumerRecord<byte[], byte[]> record) {
      return ReplicatedKey
          .newBuilder()
          .setSourceHost(sourceHosts)
          .setSourceTopic(topicPartition.topic())
          .setSourcePartition(topicPartition.partition())
          .build()
          .toByteArray();
    }

    @Override
    protected byte[] modifyValue(TopicPartition topicPartition,
        ConsumerRecord<byte[], byte[]> record) {
      return ReplicatedValue
          .newBuilder()
          .setSourceOffset(record.offset())
          .setKey(ByteString.copyFrom(record.key()))
          .setValue(ByteString.copyFrom(record.value()))
          .build()
          .toByteArray();
    }
  }

  private static class Deduplicator extends
      StreamsTopology<ReplicatedKey, ReplicatedValue, byte[], byte[]> {

    private final KeySequenceDeduplicationWrapper<ReplicatedKey, ReplicatedValue> dedupWrapper;

    private Deduplicator(AppConfig appConfig, String sourceName, String destinationName) {
      super(
          appConfig, sourceName, destinationName,
          new ReplicatedKeySerde(), new ReplicatedValueSerde(),
          Serdes.ByteArray(), Serdes.ByteArray()
      );
      String sourceNameCut = StringUtils.afterLastOccurrence(sourceName, ".");
      this.dedupWrapper = new KeySequenceDeduplicationWrapper<>(
          sourceNameCut + "-deduplication", new ReplicatedKeySerde(), new ReplicatedValueSerde()
      );
    }

    @Override
    protected KStream<byte[], byte[]> buildTransitTopology(
        KStream<ReplicatedKey, ReplicatedValue> inputStream) {
      return dedupWrapper
          .wrap(inputStream, ReplicatedValue::getSourceOffset)
          .selectKey((rk, rv) -> rv.getKey().toByteArray())
          .mapValues(rv -> rv.getValue().toByteArray());
    }
  }
}
