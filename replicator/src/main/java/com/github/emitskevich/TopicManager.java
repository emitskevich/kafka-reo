package com.github.emitskevich;

import com.github.emitskevich.core.config.AppConfig;

public class TopicManager {

  private TopicManager() {
  }

  public static String getSourceTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.clusters.source.topic");
  }

  public static String getProxyTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.clusters.destination.topic") + "-proxy";
  }

  public static String getDestinationTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.clusters.destination.topic");
  }
}
