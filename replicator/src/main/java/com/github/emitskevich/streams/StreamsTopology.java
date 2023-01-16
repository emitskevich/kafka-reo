package com.github.emitskevich.streams;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamsTopology<SK, SV, DK, DV> implements Initializable, Startable,
    Shutdownable {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamsTopology.class);

  private final StreamConfig streamConfig;
  protected final Serde<SK> sourceKeySerde;
  protected final Serde<SV> sourceValueSerde;
  protected final Serde<DK> destinationKeySerde;
  protected final Serde<DV> destinationValueSerde;
  private final String clusterName;
  private final String sourceTopic;
  private final String destinationTopic;

  private KafkaStreams kafkaStreams;

  protected StreamsTopology(AppConfig appConfig, String clusterName, String sourceTopic, String destinationTopic,
      Serde<SK> sourceKeySerde, Serde<SV> sourceValueSerde, Serde<DK> destinationKeySerde,
      Serde<DV> destinationValueSerde) {
    this.streamConfig = new StreamConfig(appConfig);
    this.clusterName = clusterName;
    this.sourceTopic = sourceTopic;
    this.destinationTopic = destinationTopic;
    this.sourceKeySerde = sourceKeySerde;
    this.sourceValueSerde = sourceValueSerde;
    this.destinationKeySerde = destinationKeySerde;
    this.destinationValueSerde = destinationValueSerde;
  }

  @Override
  public void initialize(ServerContext context) {

  }

  @Override
  public void start() {
    Properties streamProperties = streamConfig.packConfig(clusterName);
    this.kafkaStreams = buildStreams(streamProperties);
    kafkaStreams.start();
  }

  private KafkaStreams buildStreams(Properties streamProperties) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<SK, SV> inputStream = streamsBuilder
        .stream(sourceTopic, Consumed.with(sourceKeySerde, sourceValueSerde));
    this
        .buildTransitTopology(inputStream)
        .to(destinationTopic, Produced.with(destinationKeySerde, destinationValueSerde));
    Topology topology = streamsBuilder.build();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
    kafkaStreams.setUncaughtExceptionHandler(new UncaughtExceptionHandler(REPLACE_THREAD));
    StreamsStateListener listener =
        new StreamsStateListener(kafkaStreams::state);
    kafkaStreams.setStateListener(listener);

    return kafkaStreams;
  }

  protected abstract KStream<DK, DV> buildTransitTopology(KStream<SK, SV> inputStream);

  @Override
  public void shutdown() {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }
  }
}
