package com.github.emitskevich.serde;

import com.adx.proto.Kafka.ReplicatedKey;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public class ReplicatedKeySerde extends WrapperSerde<ReplicatedKey> {

  public ReplicatedKeySerde() {
    super(new ReplicatedKeySerializer(), new ReplicatedKeyDeserializer());
  }

  private static class ReplicatedKeySerializer implements Serializer<ReplicatedKey> {

    @Override
    public byte[] serialize(String topic, ReplicatedKey data) {
      return data.toByteArray();
    }
  }

  private static class ReplicatedKeyDeserializer implements Deserializer<ReplicatedKey> {

    @Override
    public ReplicatedKey deserialize(String topic, byte[] data) {
      try {
        return ReplicatedKey.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
