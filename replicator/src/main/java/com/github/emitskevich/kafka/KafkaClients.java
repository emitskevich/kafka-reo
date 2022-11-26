package com.github.emitskevich.kafka;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import com.github.emitskevich.kafka.config.ProducerConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.kafka.config.AdminConfig;
import com.github.emitskevich.kafka.config.ConsumerConfig;
import com.github.emitskevich.kafka.config.DefaultAdminConfig;
import com.github.emitskevich.kafka.config.DefaultConsumerConfig;
import com.github.emitskevich.kafka.config.DefaultProducerConfig;
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
    this(appConfig, new DefaultAdminConfig(appConfig), new DefaultConsumerConfig(appConfig),
        new DefaultProducerConfig(appConfig));
  }

  KafkaClients(AppConfig appConfig, AdminConfig adminConfig, ConsumerConfig consumerConfig,
      ProducerConfig producerConfig) {
    this.clusters = appConfig.getStringList("kafka.clusters");
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

  public void ensureTopicConf(String clientName, String topic,
      Map<String, String> config) throws ExecutionException, InterruptedException {
    LOGGER.info(
        "Ensuring topic {} {} has proper config {}...",
        clientName, topic, config
    );
    Map<String, String> topicConf = getTopicConf(clientName, topic);
    for (Entry<String, String> e : config.entrySet()) {
      String propertyName = e.getKey();
      String expectedValue = e.getValue();
      String actualValue = topicConf.get(propertyName);
      if (!expectedValue.equals(actualValue)) {
        LOGGER.info(
            "{} {}: trying to change {} ({} -> {})...",
            clientName, topic, propertyName, actualValue, expectedValue
        );
        setTopicConf(clientName, topic, propertyName, expectedValue);
        LOGGER.info("Done.");
      }
    }
    LOGGER.info("Topic {} {} checked.", clientName, topic);
  }

  public void ensureTopicExistence(String clientName, String topic, int numPartitions,
      int replicationFactor) throws ExecutionException, InterruptedException {
    ListTopicsOptions options = new ListTopicsOptions();
    AdminClient admin = select(admins, clientName);
    Set<String> topics = admin.listTopics(options).names().get();
    boolean topicExists = topics.contains(topic);
    if (topicExists) {
      ensureNumPartitions(clientName, topic, numPartitions);
      ensureReplicationFactor(clientName, topic, replicationFactor);
      return;
    }
    createTopic(clientName, topic, numPartitions, replicationFactor);
  }

  private void createTopic(String clientName, String topic, int numPartitions,
      int replicationFactor) throws ExecutionException, InterruptedException {
    LOGGER.info(
        "Topic {} {} doesn't exist, creating with numPartitions={} and and RF={}...",
        clientName, topic, numPartitions, replicationFactor
    );
    NewTopic newTopic = new NewTopic(topic, numPartitions, (short) replicationFactor);
    AdminClient admin = select(admins, clientName);
    admin.createTopics(Collections.singletonList(newTopic)).all().get();
    LOGGER.info("Topic {} {} created.", clientName, topic);
  }

  private Map<String, String> getTopicConf(String clientName, String topic)
      throws ExecutionException, InterruptedException {
    ConfigResource resource = new ConfigResource(TOPIC, topic);
    AdminClient admin = select(admins, clientName);
    Map<ConfigResource, Config> configObj = admin.describeConfigs(List.of(resource)).all().get();
    return configObj
        .get(resource)
        .entries()
        .stream()
        .collect(toMap(
            ConfigEntry::name,
            ConfigEntry::value
        ));
  }

  private void setTopicConf(String clientName, String topic,
      String propertyName, String propertyValue) throws ExecutionException, InterruptedException {
    ConfigResource resource = new ConfigResource(TOPIC, topic);
    List<AlterConfigOp> config =
        List.of(new AlterConfigOp(new ConfigEntry(propertyName, propertyValue), SET));
    Map<ConfigResource, Collection<AlterConfigOp>> configs =
        Collections.singletonMap(resource, config);
    AdminClient admin = select(admins, clientName);
    admin.incrementalAlterConfigs(configs).all().get();
  }

  public void resetOffsetsToEarliest(String clientName, String topic, String groupId)
      throws ExecutionException, InterruptedException {
    while (true) {
      try {
        resetOffsetsToEarliestInternal(clientName, topic, groupId);
        return;
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownMemberIdException)) {
          throw e;
        }
        Duration toWait = Duration.ofSeconds(1);
        LOGGER.warn(
            "Requested offsets for {} {} are not available, retrying in {}...",
            clientName, topic, toWait
        );
        Thread.sleep(toWait.toMillis());
      }
    }
  }

  private void resetOffsetsToEarliestInternal(String clientName, String topic, String groupId)
      throws ExecutionException, InterruptedException {
    KafkaProducer<byte[], byte[]> producer = select(producers, clientName);
    Map<TopicPartition, OffsetSpec> offsetsRequest = producer
        .partitionsFor(topic).stream()
        .map(p -> new TopicPartition(topic, p.partition()))
        .collect(toMap(p -> p, p -> OffsetSpec.earliest()));

    AdminClient admin = select(admins, clientName);
    Map<TopicPartition, OffsetAndMetadata> offsets = admin
        .listOffsets(offsetsRequest).all().get()
        .entrySet().stream()
        .collect(toMap(
            Entry::getKey,
            e -> new OffsetAndMetadata(e.getValue().offset())
        ));
    LOGGER.warn(
        "Resetting offsets for {} {} to earliest: {}...",
        clientName, topic, offsets
    );
    admin.alterConsumerGroupOffsets(groupId, offsets).all().get();
  }

  public void deleteTopic(String clientName, String topic)
      throws ExecutionException, InterruptedException {
    AdminClient admin = select(admins, clientName);
    admin.deleteTopics(Collections.singletonList(topic)).all().get();
  }

  private void ensureNumPartitions(String clientName, String topic,
      int numPartitions) {
    int actualPartitions = getNumPartitions(clientName, topic);
    if (actualPartitions != numPartitions) {
      String message = "Topic " + clientName + " " + topic + " "
          + "has " + actualPartitions + " partitions, "
          + "while " + numPartitions + " partitions expected";
      throw new IllegalStateException(message);
    }
  }

  public int getNumPartitions(String clientName, String topic) {
    KafkaProducer<byte[], byte[]> producer = select(producers, clientName);
    return producer.partitionsFor(topic).size();
  }

  private void ensureReplicationFactor(String clientName, String topic, int replicationFactor) {
    int actualReplicationFactor = getReplicationFactor(clientName, topic);
    if (actualReplicationFactor != replicationFactor) {
      String message = "Topic " + clientName + " " + topic + " "
          + "has RF=" + actualReplicationFactor + ", "
          + "while RF=" + replicationFactor + " expected";
      throw new IllegalStateException(message);
    }
  }

  public int getReplicationFactor(String clientName, String topic) {
    KafkaProducer<byte[], byte[]> producer = select(producers, clientName);
    return producer.partitionsFor(topic).get(0).replicas().length;
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
