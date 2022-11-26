package com.github.emitskevich.utils;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.jetbrains.annotations.Nullable;

public class StreamsAssignmentProvider {

  public Set<TopicPartition> getAssignment(@Nullable KafkaStreams kafkaStreams,
      String sourceTopic) {
    if (kafkaStreams == null) {
      return Set.of();
    }
    return kafkaStreams
        .metadataForLocalThreads()
        .stream()
        .flatMap(md -> md.activeTasks().stream())
        .flatMap(tm -> tm.topicPartitions().stream())
        .filter(tp -> tp.topic().equals(sourceTopic))
        .collect(Collectors.toSet());
  }
}
