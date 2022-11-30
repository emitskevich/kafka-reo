package com.github.emitskevich.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.github.emitskevich.core.config.AppConfig;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerConfig {

  private final AppConfig appConfig;

  public ProducerConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public Properties packConfig(String clusterName) {
    String bootstrapServers = appConfig.getString("kafka.clusters." + clusterName + ".bootstrap-servers");

    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

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
