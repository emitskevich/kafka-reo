package com.github.emitskevich.streams;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
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
import java.util.Properties;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConfig.class);
  private final AppConfig appConfig;

  public StreamConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public Properties packConfig(String clusterName) {
    Properties props = new Properties();

    String applicationId = appConfig.getString("application.name");
    LOGGER.info("Using kafka streams application.id={}...", applicationId);
    props.put(APPLICATION_ID_CONFIG, applicationId);

    String hosts = appConfig.getString("kafka.clusters." + clusterName + ".bootstrap-servers");
    props.put(BOOTSTRAP_SERVERS_CONFIG, hosts);
    int replicationFactor = appConfig.getInt("streams.replication-factor");
    props.put(REPLICATION_FACTOR_CONFIG, replicationFactor);
    int numStandbyReplicas = appConfig.getInt("streams.num-standby-replicas");
    props.put(NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
    int numStreamThreads = appConfig.getInt("streams.num-stream-threads");
    props.put(NUM_STREAM_THREADS_CONFIG, numStreamThreads);
    int commitIntervalMs = appConfig.getInt("streams.commit-interval-ms");
    props.put(COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
    String processingGuarantee = appConfig.getString("streams.processing-guarantee");
    props.put(PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

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

    // consumer config
    props.put(FETCH_MIN_BYTES_CONFIG, appConfig.getInt("kafka.config.consumer.fetch-min-bytes"));
    props.put(AUTO_OFFSET_RESET_CONFIG, appConfig.getString("kafka.config.consumer.auto-reset"));
    props.put(ISOLATION_LEVEL_CONFIG, "read_committed");

    // producer config
    int lingerMs = appConfig.getInt("kafka.config.producer.linger-ms");
    props.put(LINGER_MS_CONFIG, lingerMs);
    String compressionType = appConfig.getString("kafka.config.producer.compression-type");
    props.put(COMPRESSION_TYPE_CONFIG, compressionType);
    int acks = appConfig.getInt("kafka.config.producer.acks");
    props.put(ACKS_CONFIG, acks);
    int deliveryTimeoutMs = appConfig.getInt("kafka.config.producer.delivery-timeout-ms");
    props.put(DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
    props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(ENABLE_IDEMPOTENCE_CONFIG, true);

    return props;
  }
}
