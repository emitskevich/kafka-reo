package com.github.emitskevich.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Application {

  protected static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
  protected final ServerContext context;

  Application(ServerContext context) {
    this.context = context;
  }

  void initialize() {
    try {
      context.initialize();
    } catch (Exception e) {
      LOGGER.error("Error during app initialization", e);
      System.exit(1);
    }
  }

  void start() {
    for (Startable startable : context.getStartables()) {
      try {
        LOGGER.info("Starting " + startable.getClass().getSimpleName() + "...");
        startable.start();
        LOGGER.info("Started.");
      } catch (Exception e) {
        LOGGER.error("Uncaught error during " + startable.getClass().getSimpleName() + " start", e);
        System.exit(1);
      }
    }
  }

  void registerShutdownHook() {
    Runnable shutdownAction = () -> {
      LOGGER.info("Registered shutdown hook, waiting...");
      shutdown();
    };
    Runtime.getRuntime().addShutdownHook(new Thread(shutdownAction));
  }

  private void shutdown() {
    context.shutdown();
  }
}
