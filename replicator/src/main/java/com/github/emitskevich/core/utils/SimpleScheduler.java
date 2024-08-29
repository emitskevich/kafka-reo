package com.github.emitskevich.core.utils;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;

public class SimpleScheduler {

  private final ScheduledExecutorService executor;
  private final Logger logger;

  private SimpleScheduler(ScheduledExecutorService executor, Logger logger) {
    this.executor = executor;
    this.logger = logger;
  }

  public static SimpleScheduler createSingleThreaded(Logger logger) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    return new SimpleScheduler(scheduler, logger);
  }

  public void scheduleWithFixedDelay(Runnable runnable, Duration delay) {
    Runnable task = decorateRunnable(runnable);
    executor.scheduleWithFixedDelay(task, delay.toNanos(), delay.toNanos(), NANOSECONDS);
  }

  private Runnable decorateRunnable(Runnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        logger.error("Error occurred in SimpleScheduler task", t);
      }
    };
  }

  public void stop() {
    executor.shutdown();
  }
}
