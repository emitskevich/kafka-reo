package com.github.emitskevich.topology;

import com.adx.proto.Kafka.ReplicatedKey;
import com.adx.proto.Kafka.ReplicatedValue;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.utils.TopicManager;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.Nullable;

public class ReplicatorWrapperTopology extends ConsumerTopology {

  private String sourceHosts;

  public ReplicatorWrapperTopology(AppConfig appConfig) {
    super(appConfig, TopicManager.getSourceTopic(appConfig), TopicManager.getProxyTopic(appConfig),
        Duration.ofMillis(1));
  }

  @Override
  public void initialize(ServerContext context) throws ExecutionException, InterruptedException {
    super.initialize(context);
    this.sourceHosts = appConfig.getString("kafka.source.bootstrap-servers");
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
