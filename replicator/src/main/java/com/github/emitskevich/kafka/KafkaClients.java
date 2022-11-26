package com.github.emitskevich.kafka;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.kafka.config.AdminConfig;
import com.github.emitskevich.kafka.config.ConsumerConfig;
import com.github.emitskevich.kafka.config.DefaultAdminConfig;
import com.github.emitskevich.kafka.config.DefaultConsumerConfig;
import com.github.emitskevich.kafka.config.DefaultProducerConfig;
import com.github.emitskevich.kafka.config.ProducerConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClients implements Initializable, Shutdownable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClients.class);

  private final List<String> clusters;
  private final AdminConfig adminConfig;
  private final ConsumerConfig consumerConfig;
  private final ProducerConfig producerConfig;

  private final Map<String, AdminClient> admins = new ConcurrentHashMap<>();
  private final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
  private final Map<ConsumerKey, ConsumerPair<byte[], byte[]>> consumers = new ConcurrentHashMap<>();

  public KafkaClients(AppConfig appConfig) {
    this(new DefaultAdminConfig(appConfig), new DefaultConsumerConfig(appConfig),
        new DefaultProducerConfig(appConfig));
  }

  KafkaClients(AdminConfig adminConfig, ConsumerConfig consumerConfig,
      ProducerConfig producerConfig) {
    this.clusters = List.of("source", "destination");
    this.adminConfig = adminConfig;
    this.consumerConfig = consumerConfig;
    this.producerConfig = producerConfig;
  }

  @Override
  public void initialize(ServerContext context) {
    for (String clusterName : clusters) {
      admins.put(clusterName, AdminClient.create(adminConfig.packConfig(clusterName)));
      producers.put(clusterName, new KafkaProducer<>(producerConfig.packConfig(clusterName)));
    }
  }

  public AdminClient getAdmin(String clientName) {
    return select(admins, clientName);
  }

  public ConsumerPair<byte[], byte[]> buildConsumerPair(String clientName, String groupId) {
    String clusterName = extractClusterName(clientName);
    ConsumerKey key = new ConsumerKey(clusterName, groupId);
    return consumers.computeIfAbsent(
        key, k -> new ConsumerPair<>(
            new KafkaConsumer<>(consumerConfig.packConfig(k.clusterName(), k.groupId())),
            new AssignmentListener()
        )
    );
  }

  public Set<TopicPartition> getAssignment(String clientName, String groupId) {
    String clusterName = extractClusterName(clientName);
    ConsumerKey key = new ConsumerKey(clusterName, groupId);
    ConsumerPair<byte[], byte[]> consumerPair = consumers.get(key);
    if (consumerPair == null) {
      return Set.of();
    }
    return consumerPair.listener().getAssignment();
  }

  public KafkaProducer<byte[], byte[]> getProducer(String clientName) {
    return select(producers, clientName);
  }

  @Override
  public void shutdown() {
    admins.values().forEach(Admin::close);
    consumers.values().stream().map(ConsumerPair::consumer).forEach(KafkaConsumer::close);
    producers.values().forEach(KafkaProducer::close);
  }

  public String extractClusterName(String clientName) {
    return clientName.split("\\.")[0];
  }

  private <T> T select(Map<String, T> map, String clientName) {
    String clusterName = extractClusterName(clientName);
    return map.get(clusterName);
  }

  public record ConsumerKey(String clusterName, String groupId) {

  }

  public record ConsumerPair<K, V>(KafkaConsumer<K, V> consumer,
                                   AssignmentListener listener) {

  }
}
