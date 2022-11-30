package com.github.emitskevich.core.utils;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SimpleScheduler {

  private final ScheduledExecutorService executor;

  private SimpleScheduler(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public static SimpleScheduler createSingleThreaded() {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    return new SimpleScheduler(scheduler);
  }

  public void scheduleWithFixedDelay(Runnable runnable, Duration delay) {
    executor.scheduleWithFixedDelay(runnable, delay.toNanos(), delay.toNanos(), NANOSECONDS);
  }

  public void stop() {
    executor.shutdown();
  }
}
