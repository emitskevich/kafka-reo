package com.github.emitskevich;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnceReplicator implements Initializable, Startable, Shutdownable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceReplicator.class);

  private final AppConfig appConfig;
  private ReplicatorWrapperTopology replicator;
  private DeduplicationUnwrapperTopology deduplicator;

  public ExactlyOnceReplicator(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  public void initialize(ServerContext context) throws Exception {
    this.replicator = new ReplicatorWrapperTopology(appConfig);
    replicator.initialize(context);
    this.deduplicator = new DeduplicationUnwrapperTopology(appConfig);
    deduplicator.initialize(context);
  }

  @Override
  public void start() {
    logCoordinates();
    replicator.start();
    deduplicator.start();
  }

  private void logCoordinates() {
    String fromHosts = appConfig.getString("kafka.clusters.source.bootstrap-servers");
    String fromTopic = appConfig.getString("kafka.clusters.source.topic");
    String toHosts = appConfig.getString("kafka.clusters.destination.bootstrap-servers");
    String toTopic = appConfig.getString("kafka.clusters.destination.topic");
    LOGGER.info("Starting replication from\n"
            + "hosts: {}\n"
            + "topic: {}\n"
            + "to\n"
            + "hosts: {}\n"
            + "topic: {}",
        fromHosts, fromTopic, toHosts, toTopic
    );
  }

  @Override
  public void shutdown() {
    replicator.shutdown();
    deduplicator.shutdown();
  }
}
