package com.github.emitskevich.kafka.config;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.topicPrefix;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import com.github.emitskevich.CustomDeserializationExceptionHandler;
import com.github.emitskevich.CustomProductionExceptionHandler;
import com.github.emitskevich.core.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStreamConfig implements StreamConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStreamConfig.class);
  private final String sourceName;
  private final AppConfig appConfig;

  public DefaultStreamConfig(String sourceName, AppConfig appConfig) {
    this.sourceName = sourceName;
    this.appConfig = appConfig;
  }

  @Override
  public Properties packConfig() {
    Properties props = new Properties();

    String applicationId = appConfig.getString("application.name");
    LOGGER.info("Using kafka streams application.id={}...", applicationId);
    props.put(APPLICATION_ID_CONFIG, applicationId);

    String hosts = appConfig.getString("kafka." + sourceName + ".bootstrap-servers");
    props.put(BOOTSTRAP_SERVERS_CONFIG, hosts);
    props.put(REPLICATION_FACTOR_CONFIG, appConfig.getInt("streams.replication-factor"));
    props.put(NUM_STANDBY_REPLICAS_CONFIG, appConfig.getInt("streams.num-standby-replicas"));
    props.put(NUM_STREAM_THREADS_CONFIG, appConfig.getInt("streams.num-stream-threads"));
    Duration commitInterval = appConfig.getDuration("streams.commit-interval");
    props.put(COMMIT_INTERVAL_MS_CONFIG, commitInterval.toMillis());
    props.put(PROCESSING_GUARANTEE_CONFIG, appConfig.getString("streams.processing-guarantee"));

    props.put(
        DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UsePartitionTimeOnInvalidTimestamp.class.getName()
    );
    props.put(
        DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        CustomDeserializationExceptionHandler.class.getName()
    );
    props.put(
        DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        CustomProductionExceptionHandler.class.getName()
    );

    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, DefaultConsumerConfig.FETCH_MIN_BYTES);
    props.put(ProducerConfig.LINGER_MS_CONFIG, DefaultProducerConfig.LINGER.toMillis());
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DefaultProducerConfig.COMPRESSION_TYPE);

    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) Duration.ofDays(7).toMillis());
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    String autoReset = appConfig.getString("application.auto-reset");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoReset);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    props.put(topicPrefix(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG), "LogAppendTime");

    return props;
  }
}
