package com.github.emitskevich.serde;

import com.adx.proto.Kafka.ReplicatedValue;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public class ReplicatedValueSerde extends WrapperSerde<ReplicatedValue> {

  public ReplicatedValueSerde() {
    super(new ReplicatedValueSerializer(), new ReplicatedValueDeserializer());
  }

  private static class ReplicatedValueSerializer implements Serializer<ReplicatedValue> {

    @Override
    public byte[] serialize(String topic, ReplicatedValue data) {
      return data.toByteArray();
    }
  }

  private static class ReplicatedValueDeserializer implements Deserializer<ReplicatedValue> {

    @Override
    public ReplicatedValue deserialize(String topic, byte[] data) {
      try {
        return ReplicatedValue.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
