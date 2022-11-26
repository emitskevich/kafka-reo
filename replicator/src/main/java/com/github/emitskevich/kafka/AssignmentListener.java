package com.github.emitskevich.kafka;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentListener implements ConsumerRebalanceListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentListener.class);
  private final Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    LOGGER.info("Revoked partitions {}", partitions);
    assignment.removeAll(partitions);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOGGER.info("Assigned partitions {}", partitions);
    assignment.addAll(partitions);
  }

  public Set<TopicPartition> getAssignment() {
    return assignment;
  }
}
