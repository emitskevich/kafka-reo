package com.github.emitskevich.kafka.config;

import java.util.Properties;

public interface AdminConfig {

  Properties packConfig(String clusterName);
}
