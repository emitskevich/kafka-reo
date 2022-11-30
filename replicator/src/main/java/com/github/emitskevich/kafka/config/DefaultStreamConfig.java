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

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.utils.CustomDeserializationExceptionHandler;
import com.github.emitskevich.utils.CustomProductionExceptionHandler;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStreamConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStreamConfig.class);
  private final AppConfig appConfig;

  public DefaultStreamConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public Properties packConfig(String clusterName) {
    Properties props = new Properties();

    String applicationId = appConfig.getString("application.name");
    LOGGER.info("Using kafka streams application.id={}...", applicationId);
    props.put(APPLICATION_ID_CONFIG, applicationId);

    String hosts = appConfig.getString("kafka." + clusterName + ".bootstrap-servers");
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
    props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, ProducerConfig.LINGER.toMillis());
    props.put(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerConfig.COMPRESSION_TYPE);

    props.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) Duration.ofDays(7).toMillis());
    props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");

    String autoReset = appConfig.getString("application.auto-reset");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoReset);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    return props;
  }
}
