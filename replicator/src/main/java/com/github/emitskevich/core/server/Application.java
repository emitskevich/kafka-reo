package com.github.emitskevich.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  protected static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
  protected final ServerContext context;

  public Application(ServerContext context) {
    this.context = context;
  }

  public void initialize() {
    try {
      context.initialize();
    } catch (Exception e) {
      LOGGER.error("Error during app initialization", e);
      System.exit(1);
    }
  }

  public void start() {
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

  public void registerShutdownHook() {
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
