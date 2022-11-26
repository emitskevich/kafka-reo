package com.github.emitskevich.deduplication;

import com.adx.proto.Kafka.DedupMessageProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public record DedupMessage<K, V>(K key, V value) {

  public static class DedupMessageSerde<K, V> extends WrapperSerde<DedupMessage<K, V>> {

    public DedupMessageSerde(Serde<K> keySerde, Serde<V> valueSerde) {
      super(
          new DedupMessageSerializer<>(keySerde.serializer(), valueSerde.serializer()),
          new DedupMessageDeserializer<>(keySerde.deserializer(), valueSerde.deserializer())
      );
    }

    private record DedupMessageSerializer<K, V>(Serializer<K> keySerializer,
                                                Serializer<V> valueSerializer)
        implements Serializer<DedupMessage<K, V>> {

      @Override
      public byte[] serialize(String topic, DedupMessage<K, V> data) {
        return DedupMessageProto
            .newBuilder()
            .setKey(ByteString.copyFrom(keySerializer.serialize(null, data.key())))
            .setValue(ByteString.copyFrom(valueSerializer.serialize(null, data.value())))
            .build()
            .toByteArray();
      }
    }

    private record DedupMessageDeserializer<K, V>(Deserializer<K> keyDeserializer,
                                                  Deserializer<V> valueDeserializer)
        implements Deserializer<DedupMessage<K, V>> {

      @Override
      public DedupMessage<K, V> deserialize(String topic, byte[] data) {
        try {
          DedupMessageProto messageProto = DedupMessageProto.parseFrom(data);
          K k = keyDeserializer.deserialize(null, messageProto.getKey().toByteArray());
          V v = valueDeserializer.deserialize(null, messageProto.getValue().toByteArray());
          return new DedupMessage<>(k, v);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}


