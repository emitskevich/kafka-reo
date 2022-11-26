package com.github.emitskevich.kafka.config;

import java.util.Properties;

public interface ConsumerConfig {

  Properties packConfig(String clusterName, String groupId);
}
