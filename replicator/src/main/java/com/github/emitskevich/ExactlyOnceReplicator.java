package com.github.emitskevich;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;

public class ExactlyOnceReplicator implements Initializable, Startable, Shutdownable {

  private final AppConfig appConfig;

  private ReplicatorWrapperTopology replicator;
  private DeduplicatorUnwrapperTopology deduplicator;

  public ExactlyOnceReplicator(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  public void initialize(ServerContext context) throws Exception {
    this.replicator = new ReplicatorWrapperTopology(appConfig);
    replicator.initialize(context);
    this.deduplicator = new DeduplicatorUnwrapperTopology(appConfig);
    deduplicator.initialize(context);
  }

  @Override
  public void start() {
    replicator.start();
    deduplicator.start();
  }

  @Override
  public void shutdown() {
    replicator.shutdown();
    deduplicator.shutdown();
  }
}
