package com.github.emitskevich.deduplication;

import static java.lang.Math.max;

import com.adx.proto.Kafka.ReplicatedKey;
import com.adx.proto.Kafka.ReplicatedValue;
import com.github.emitskevich.StreamsTopology;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.deduplication.DedupValue.DedupValueSerde;
import com.github.emitskevich.serde.ReplicatedKeySerde;
import com.github.emitskevich.serde.ReplicatedValueSerde;
import java.util.function.ToLongFunction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicatorUnwrapperTopology extends
    StreamsTopology<ReplicatedKey, ReplicatedValue, byte[], byte[]> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicatorUnwrapperTopology.class);
  private final DedupValueSerde<ReplicatedValue> dedupValueSerde;

  public DeduplicatorUnwrapperTopology(AppConfig appConfig, String sourceName,
      String destinationName) {
    super(
        appConfig, sourceName, destinationName,
        new ReplicatedKeySerde(), new ReplicatedValueSerde(),
        Serdes.ByteArray(), Serdes.ByteArray()
    );
    this.dedupValueSerde = new DedupValueSerde<>(sourceValueSerde);
  }

  @Override
  protected KStream<byte[], byte[]> buildTransitTopology(
      KStream<ReplicatedKey, ReplicatedValue> inputStream) {
    ToLongFunction<ReplicatedValue> sequenceExtractor = ReplicatedValue::getSourceOffset;
    return inputStream
        .groupByKey(Grouped
            .with(sourceKeySerde, sourceValueSerde)
            .withName("deduplication")
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
                .<ReplicatedKey, DedupValue<ReplicatedValue>, KeyValueStore<Bytes, byte[]>>as(
                    "deduplication")
                .withKeySerde(sourceKeySerde)
                .withValueSerde(dedupValueSerde)
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
                applicationId, messagePosition, k, prevMaxPosition
            );
            return false;
          }
          return true;
        })
        .mapValues(DedupValue::value)
        .selectKey((rk, rv) -> rv.getKey().toByteArray())
        .mapValues(rv -> rv.getValue().toByteArray());
  }
}
