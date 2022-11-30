package com.github.emitskevich.utils;

import com.github.emitskevich.core.config.AppConfig;

public class TopicManager {

  private TopicManager() {
  }

  public static String getSourceTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.source.topic");
  }

  public static String getProxyTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.destination.topic") + "-proxy";
  }

  public static String getDestinationTopic(AppConfig appConfig) {
    return appConfig.getString("kafka.destination.topic");
  }
}
