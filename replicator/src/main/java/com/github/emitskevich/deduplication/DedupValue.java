package com.github.emitskevich.deduplication;

import com.adx.proto.Kafka.DedupValueProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public record DedupValue<V>(boolean duplicate, long prevMaxPosition, V value) {

  public static class DedupValueSerde<V> extends WrapperSerde<DedupValue<V>> {

    public DedupValueSerde(Serde<V> valueSerde) {
      super(
          new DedupValueSerializer<>(valueSerde.serializer()),
          new DedupValueDeserializer<>(valueSerde.deserializer())
      );
    }

    private record DedupValueSerializer<V>(Serializer<V> valueSerializer) implements
        Serializer<DedupValue<V>> {

      @Override
      public byte[] serialize(String topic, DedupValue<V> data) {
        return DedupValueProto
            .newBuilder()
            .setDuplicate(data.duplicate())
            .setPrevMaxPosition(data.prevMaxPosition())
            .setValue(ByteString.copyFrom(valueSerializer.serialize(null, data.value())))
            .build()
            .toByteArray();
      }
    }

    private record DedupValueDeserializer<V>(Deserializer<V> valueDeserializer)
        implements Deserializer<DedupValue<V>> {

      @Override
      public DedupValue<V> deserialize(String topic, byte[] data) {
        try {
          DedupValueProto dedupValueProto = DedupValueProto.parseFrom(data);
          V value = valueDeserializer
              .deserialize(null, dedupValueProto.getValue().toByteArray());
          return new DedupValue<>(
              dedupValueProto.getDuplicate(), dedupValueProto.getPrevMaxPosition(), value
          );
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
