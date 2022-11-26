package com.github.emitskevich.deduplication;

import com.adx.proto.Kafka.DedupAggregateProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public record DedupAggregate<K, V>(boolean duplicate, long prevMaxPosition,
                                   DedupMessage<K, V> message) {

  public static class DedupAggregateSerde<K, V> extends WrapperSerde<DedupAggregate<K, V>> {

    public DedupAggregateSerde(Serde<DedupMessage<K, V>> messageSerde) {
      super(
          new DedupAggregateSerializer<>(messageSerde.serializer()),
          new DedupAggregateDeserializer<>(messageSerde.deserializer())
      );
    }

    private record DedupAggregateSerializer<K, V>(
        Serializer<DedupMessage<K, V>> messageSerializer)
        implements Serializer<DedupAggregate<K, V>> {

      @Override
      public byte[] serialize(String topic, DedupAggregate<K, V> data) {
        return DedupAggregateProto
            .newBuilder()
            .setDuplicate(data.duplicate())
            .setPrevMaxPosition(data.prevMaxPosition())
            .setMessage(ByteString.copyFrom(messageSerializer.serialize(null, data.message())))
            .build()
            .toByteArray();
      }
    }

    private record DedupAggregateDeserializer<K, V>(
        Deserializer<DedupMessage<K, V>> messageDeserializer)
        implements Deserializer<DedupAggregate<K, V>> {

      @Override
      public DedupAggregate<K, V> deserialize(String topic, byte[] data) {
        try {
          DedupAggregateProto aggregateProto = DedupAggregateProto.parseFrom(data);
          DedupMessage<K, V> message = messageDeserializer
              .deserialize(null, aggregateProto.getMessage().toByteArray());
          return new DedupAggregate<>(
              aggregateProto.getDuplicate(), aggregateProto.getPrevMaxPosition(), message
          );
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
