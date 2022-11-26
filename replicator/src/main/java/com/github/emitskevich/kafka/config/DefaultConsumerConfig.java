package com.github.emitskevich.kafka.config;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.github.emitskevich.core.config.AppConfig;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConsumerConfig implements ConsumerConfig {

  public static final int FETCH_MIN_BYTES = 1024 * 1024;

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsumerConfig.class);
  private final AppConfig appConfig;

  public DefaultConsumerConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  public Properties packConfig(String clusterName, String groupId) {
    String bootstrapServers = appConfig.getString("kafka." + clusterName + ".bootstrap-servers");

    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTES);
    props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
    String autoReset = appConfig.getString("application.auto-reset");
    props.put(AUTO_OFFSET_RESET_CONFIG, autoReset);
    props.put(ISOLATION_LEVEL_CONFIG, "read_committed");

    LOGGER.info("Using kafka consumer group.id={}...", groupId);
    props.put(GROUP_ID_CONFIG, groupId);
    return props;
  }
}
