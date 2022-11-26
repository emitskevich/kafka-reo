package com.github.emitskevich;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.kafka.config.DefaultStreamConfig;
import com.github.emitskevich.kafka.config.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamsTopology<SK, SV, DK, DV> implements Initializable, Startable,
    Shutdownable {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamsTopology.class);

  protected final AppConfig appConfig;
  protected final String sourceName;
  protected final String destinationName;
  private final StreamConfig streamConfig;
  //  protected final NamingLogic namingLogic;
  protected final LagMonitor lagMonitor;
  protected final Serde<SK> sourceKeySerde;
  protected final Serde<SV> sourceValueSerde;
  protected final Serde<DK> destinationKeySerde;
  protected final Serde<DV> destinationValueSerde;
  //  private final List<MessageValidator<SK, SV>> messageValidators;
  protected String sourceTopic;
  protected String destinationTopic;

  protected KafkaStreams kafkaStreams;

  protected StreamsTopology(AppConfig appConfig, String sourceName, String destinationName,
      Serde<SK> sourceKeySerde, Serde<SV> sourceValueSerde,
      Serde<DK> destinationKeySerde, Serde<DV> destinationValueSerde) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.appConfig = appConfig;
    this.streamConfig = new DefaultStreamConfig(sourceName, appConfig);
    this.lagMonitor = new LagMonitor(sourceName, destinationName, appConfig);
    this.sourceKeySerde = sourceKeySerde;
    this.sourceValueSerde = sourceValueSerde;
    this.destinationKeySerde = destinationKeySerde;
    this.destinationValueSerde = destinationValueSerde;
  }

  @Override
  public void initialize(ServerContext context) throws Exception {
    this.sourceTopic = appConfig.getString("kafka." + sourceName + ".name");
    this.destinationTopic = appConfig.getString("kafka." + destinationName + ".name");

    String applicationId = appConfig.getString("application.name");
    lagMonitor.setGroupId(applicationId);
    StreamsAssignmentProvider assignmentProvider = new StreamsAssignmentProvider();
    lagMonitor.setAssignmentSupplier(
        () -> assignmentProvider.getAssignment(kafkaStreams, sourceTopic));
    lagMonitor.initialize(context);
  }

  @Override
  public void start() {
    Properties streamProperties = streamConfig.packConfig();
    lagMonitor.reportLag();
    this.kafkaStreams = buildStreams(streamProperties);
    kafkaStreams.start();
    lagMonitor.start();
  }

  private KafkaStreams buildStreams(Properties streamProperties) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<SK, SV> inputStream = streamsBuilder
        .stream(sourceTopic, Consumed.with(sourceKeySerde, sourceValueSerde));
    this
        .buildTransitTopology(inputStream)
        .to(destinationTopic, Produced.with(destinationKeySerde, destinationValueSerde));
    Topology topology = streamsBuilder.build();

    LOGGER.info("Topology: {}", topology.describe());

    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
    kafkaStreams.setUncaughtExceptionHandler(new UncaughtExceptionHandler(REPLACE_THREAD));
    StreamsStateListener listener =
        new StreamsStateListener(sourceName, destinationName, kafkaStreams::state);
    kafkaStreams.setStateListener(listener);

    return kafkaStreams;
  }

  protected abstract KStream<DK, DV> buildTransitTopology(KStream<SK, SV> inputStream);

  @Override
  public void shutdown() {
    kafkaStreams.close();
    lagMonitor.reportLag();
    lagMonitor.shutdown();
  }
}
