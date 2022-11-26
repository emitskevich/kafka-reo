package com.github.emitskevich.deduplication;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.state.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicationTopologyWrapper<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicationTopologyWrapper.class);
  private final Serde<K> keySerde;
  private final Serde<DedupValue<V>> dedupValueSerde;

  public DeduplicationTopologyWrapper(Serde<K> keySerde, Serde<V> valueSerde) {
    this.keySerde = keySerde;
    this.dedupValueSerde = new DedupValueSerde<>(valueSerde);
  }

  public KStream<K, V> wrap(KStream<K, V> inputStream, Duration windowSize) {
    return inputStream
        .mapValues(v -> new DedupValue<>(v, false))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(windowSize))
        .reduce(
            (value1, value2) -> new DedupValue<>(value1.value(), true),
            Materialized
                .<K, DedupValue<V>, SessionStore<Bytes, byte[]>>with(keySerde, dedupValueSerde)
                .withLoggingEnabled(Map.of("segment.bytes", "52428800"))
                .withCachingDisabled()
        )
        .toStream()
        .filter((wk, dv) -> dv != null)
        .filter((wk, dv) -> {
          if (dv.duplicate()) {
            Window window = wk.window();
            LOGGER.warn(
                "Duplicated value for key {} inside {}/{} window, skipped",
                wk.key(), Instant.ofEpochMilli(window.start()), Instant.ofEpochMilli(window.end())
            );
            return false;
          }
          return true;
        })
        .selectKey((wk, dv) -> wk.key())
        .mapValues(DedupValue::value)
        ;
  }

  private record DedupValue<V>(V value, boolean duplicate) {

  }

  private static class DedupValueSerde<V> extends WrapperSerde<DedupValue<V>> {

    private DedupValueSerde(Serde<V> vSerde) {
      super(new DvSerializer<>(vSerde.serializer()), new DvDeserializer<>(vSerde.deserializer()));
    }

    private record DvSerializer<V>(Serializer<V> vSerializer) implements Serializer<DedupValue<V>> {

      @Override
      public byte[] serialize(String topic, DedupValue<V> data) {
        byte[] vBytes = vSerializer.serialize(topic, data.value());
        return ByteBuffer
            .allocate(vBytes.length + 1)
            .put(data.duplicate() ? (byte) 1 : (byte) 0)
            .put(vBytes)
            .array();
      }
    }

    private record DvDeserializer<V>(Deserializer<V> vDeserializer) implements
        Deserializer<DedupValue<V>> {

      @Override
      public DedupValue<V> deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        boolean duplicate = buffer.get() == (byte) 1;
        int remainingSize = buffer.remaining();
        byte[] vBytes = new byte[remainingSize];
        buffer.get(vBytes);
        V value = vDeserializer.deserialize(topic, vBytes);
        return new DedupValue<>(value, duplicate);
      }
    }
  }
}
