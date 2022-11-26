package com.github.emitskevich.kafka.config;

import java.util.Properties;

public interface ProducerConfig {

  Properties packConfig(String clusterName);
}
