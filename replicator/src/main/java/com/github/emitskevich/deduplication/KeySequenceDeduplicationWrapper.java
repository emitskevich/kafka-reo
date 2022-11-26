package com.github.emitskevich.deduplication;

import static java.lang.Math.max;

import com.github.emitskevich.deduplication.DedupValue.DedupValueSerde;
import java.util.function.ToLongFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySequenceDeduplicationWrapper<K, V> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KeySequenceDeduplicationWrapper.class);

  private final String name;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final DedupValueSerde<V> dedupSerde;

  public KeySequenceDeduplicationWrapper(String name, Serde<K> keySerde, Serde<V> valueSerde) {
    this.name = name;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.dedupSerde = new DedupValueSerde<>(this.valueSerde);
  }

  public KStream<K, V> wrap(KStream<K, V> input, ToLongFunction<V> sequenceExtractor) {
    return input
        .groupByKey(Grouped
            .with(keySerde, valueSerde)
            .withName(name)
        )
        .aggregate(
            () -> new DedupValue<>(false, 0, null),
            (k, v, dv) -> {
              long messagePosition = sequenceExtractor.applyAsLong(v);
              long prevMaxPosition = dv.prevMaxPosition();
              boolean duplicate = (messagePosition <= prevMaxPosition);
              long maxPosition = max(messagePosition, prevMaxPosition);
              return new DedupValue<>(duplicate, maxPosition, v);
            },
            Materialized
                .<K, DedupValue<V>, KeyValueStore<Bytes, byte[]>>as(name)
                .withKeySerde(keySerde)
                .withValueSerde(dedupSerde)
                .withCachingDisabled()
        )
        .toStream()
        .filter((k, agg) -> {
          if (agg.duplicate()) {
            long prevMaxPosition = agg.prevMaxPosition();
            long messagePosition = sequenceExtractor.applyAsLong(agg.value());
            LOGGER.warn(""
                    + "{}: duplicated position #{} under key {}, "
                    + "while #{} was already processed, skipped",
                name, messagePosition, k, prevMaxPosition
            );
            return false;
          }
          return true;
        })
        .mapValues(DedupValue::value);
  }
}
