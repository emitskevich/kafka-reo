package com.github.emitskevich;

import com.github.emitskevich.kafka.KafkaClients;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.LauncherBase;
import com.github.emitskevich.core.server.ServerContext;

public class Launcher {

  public static void main(String[] args) {
    LauncherBase.main(args, Launcher::registerInstances);
  }

  private static void registerInstances(ServerContext context, AppConfig appConfig) {
    context.register(KafkaClients.class, new KafkaClients(appConfig));
  }
}
