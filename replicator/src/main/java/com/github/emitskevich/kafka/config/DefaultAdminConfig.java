package com.github.emitskevich.kafka.config;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import com.github.emitskevich.core.config.AppConfig;

public class DefaultAdminConfig implements AdminConfig {

  private final AppConfig appConfig;

  public DefaultAdminConfig(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  public Properties packConfig(String clusterName) {
    String bootstrapServers = appConfig.getString("kafka." + clusterName + ".bootstrap-servers");

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return props;
  }
}
