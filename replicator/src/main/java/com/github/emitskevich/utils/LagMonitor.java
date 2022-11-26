package com.github.emitskevich.utils;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.admin.OffsetSpec.latest;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.core.utils.MathUtils;
import com.github.emitskevich.core.utils.SimpleScheduler;
import com.github.emitskevich.kafka.KafkaClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LagMonitor implements Initializable, Startable, Shutdownable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LagMonitor.class);

  private final String sourceName;
  private final String destinationName;
  private final AppConfig appConfig;
  private final SimpleScheduler scheduler = SimpleScheduler.createSingleThreaded();

  private String groupId;
  private Supplier<Set<TopicPartition>> assignmentSupplier;
  private AdminClient admin;
  private Duration delay;

  public LagMonitor(String sourceName, String destinationName, AppConfig appConfig) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.appConfig = appConfig;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setAssignmentSupplier(Supplier<Set<TopicPartition>> assignmentSupplier) {
    this.assignmentSupplier = assignmentSupplier;
  }

  @Override
  public void initialize(ServerContext context) {
    KafkaClients kafkaClients = context.getInstance(KafkaClients.class);
    this.admin = kafkaClients.getAdmin(sourceName);
    this.delay = appConfig.getDuration("utils.monitor-interval.lag");
  }

  @Override
  public void start() {
    scheduler.scheduleWithFixedDelay(this::reportLag, delay);
  }

  public void reportLag() {
    try {
      Map<Integer, Long> lags = computeLags();

      List<Integer> assignment = assignmentSupplier.get().stream()
          .map(TopicPartition::partition)
          .sorted()
          .collect(Collectors.toList());

      OptionalDouble localAvg = assignment
          .stream()
          .filter(lags::containsKey)
          .map(lags::get)
          .mapToLong(Long::valueOf)
          .average();
      OptionalDouble externalAvg = lags
          .keySet().stream()
          .filter(p -> !assignment.contains(p))
          .map(lags::get)
          .mapToLong(Long::valueOf)
          .average();

      LOGGER.info(""
              + "\"{} -> {}\" avg. lag/partition: local {}, external {}. "
              + "Assignment size {}/{}: {}. All partitions: {}",
          sourceName, destinationName,
          localAvg.isPresent() ? MathUtils.round(localAvg.getAsDouble(), 2) : "-",
          externalAvg.isPresent() ? MathUtils.round(externalAvg.getAsDouble(), 2) : "-",
          assignment.size(), lags.size(),
          assignment, lags
      );

    } catch (Throwable t) {
      LOGGER.error("\"{} -> {}\" lag computation error", sourceName, destinationName, t);
    }
  }

  private Map<Integer, Long> computeLags() throws ExecutionException, InterruptedException {
    Map<TopicPartition, Long> currentOffsets = getCurrentOffsets(groupId);
    Map<TopicPartition, Long> endOffsets = getEndOffsets(currentOffsets);
    Map<Integer, Long> lags = new HashMap<>();
    for (Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
      Long endOffset = endOffsets.get(entry.getKey());
      Long currentOffset = currentOffsets.get(entry.getKey());
      long lag = endOffset - currentOffset;
      lags.put(entry.getKey().partition(), lag);
    }
    return lags;
  }

  private Map<TopicPartition, Long> getCurrentOffsets(String groupId)
      throws ExecutionException, InterruptedException {
    ListConsumerGroupOffsetsResult info = admin.listConsumerGroupOffsets(groupId);
    Map<TopicPartition, OffsetAndMetadata> map = info.partitionsToOffsetAndMetadata().get();

    Map<TopicPartition, Long> groupOffset = new HashMap<>();
    for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
      groupOffset.put(entry.getKey(), entry.getValue().offset());
    }
    return groupOffset;
  }

  private Map<TopicPartition, Long> getEndOffsets(Map<TopicPartition, Long> currentOffsets)
      throws ExecutionException, InterruptedException {
    Map<TopicPartition, OffsetSpec> topicPartitionOffsets = currentOffsets
        .keySet().stream()
        .collect(toMap(p -> p, p -> latest()));
    Map<TopicPartition, ListOffsetsResultInfo> results = admin
        .listOffsets(topicPartitionOffsets).all().get();
    return results
        .entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().offset()
        ));
  }

  @Override
  public void shutdown() {
    scheduler.stop();
  }
}
