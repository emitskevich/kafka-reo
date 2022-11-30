package com.github.emitskevich.kafka.config;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import com.github.emitskevich.core.config.AppConfig;

public class ProducerConfig {

  public static final Duration LINGER = Duration.ofMillis(10);
  public static final String COMPRESSION_TYPE = "gzip";

  private final AppConfig appConfig;

  public ProducerConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public Properties packConfig(String clusterName) {
    String bootstrapServers = appConfig.getString("kafka." + clusterName + ".bootstrap-servers");

    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(LINGER_MS_CONFIG, LINGER.toMillis());
    props.put(COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);

    props.put(DELIVERY_TIMEOUT_MS_CONFIG, (int) Duration.ofDays(7).toMillis());
    props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ACKS_CONFIG, "all");

    return props;
  }
}
