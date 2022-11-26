package com.github.emitskevich.deduplication;

import static java.lang.Math.max;

import com.github.emitskevich.deduplication.DedupAggregate.DedupAggregateSerde;
import com.github.emitskevich.deduplication.DedupMessage.DedupMessageSerde;
import java.util.function.ToLongFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionDeduplicationTopologyWrapper<K, V> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PartitionDeduplicationTopologyWrapper.class);

  private final String name;
  private final DedupMessageSerde<K, V> dedupMessageSerde;
  private final DedupAggregateSerde<K, V> dedupAggregateSerde;

  public PartitionDeduplicationTopologyWrapper(String name, Serde<K> keySerde,
      Serde<V> valueSerde) {
    this.name = name;
    this.dedupMessageSerde = new DedupMessageSerde<>(keySerde, valueSerde);
    this.dedupAggregateSerde = new DedupAggregateSerde<>(dedupMessageSerde);
  }

  public KStream<K, V> wrap(KStream<K, V> input, ToLongFunction<V> sequenceExtractor) {
    return input
        .mapValues(DedupMessage::new)
        .transform(() -> new PartitionKeyTransformer<>())
        .groupByKey(Grouped
            .with(Serdes.String(), dedupMessageSerde)
            .withName(name)
        )
        .aggregate(
            () -> new DedupAggregate<>(false, 0, null),
            (fakeKey, message, aggregate) -> {
              long messagePosition = sequenceExtractor.applyAsLong(message.value());
              long prevMaxPosition = aggregate.prevMaxPosition();
              boolean duplicate = (messagePosition <= prevMaxPosition);
              long maxPosition = max(messagePosition, prevMaxPosition);
              return new DedupAggregate<>(duplicate, maxPosition, message);
            },
            Materialized
                .<String, DedupAggregate<K, V>, KeyValueStore<Bytes, byte[]>>as(name)
                .withKeySerde(Serdes.String())
                .withValueSerde(dedupAggregateSerde)
                .withCachingDisabled()
        )
        .toStream()
        .filter((k, agg) -> {
          if (agg.duplicate()) {
            long prevMaxPosition = agg.prevMaxPosition();
            long messagePosition = sequenceExtractor.applyAsLong(agg.message().value());
            LOGGER.warn(""
                    + "Duplicated position #{} under key {}, "
                    + "while #{} was already processed, skipped",
                messagePosition, k, prevMaxPosition
            );
            return false;
          }
          return true;
        })
        .mapValues(DedupAggregate::message)
        .map((fakeKey, message) -> new KeyValue<>(message.key(), message.value()))
        ;
  }

  private static class PartitionKeyTransformer<K, V> implements
      Transformer<K, V, KeyValue<String, V>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
    }

    @Override
    public KeyValue<String, V> transform(K key, V value) {
      String newKey = context.topic() + ":" + context.partition();
      return new KeyValue<>(newKey, value);
    }

    @Override
    public void close() {

    }
  }
}
