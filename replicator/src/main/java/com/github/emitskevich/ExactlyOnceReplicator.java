package com.github.emitskevich;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.Initializable;
import com.github.emitskevich.core.server.ServerContext;
import com.github.emitskevich.core.server.Shutdownable;
import com.github.emitskevich.core.server.Startable;
import com.github.emitskevich.deduplication.DeduplicatorUnwrapperTopology;
import com.github.emitskevich.deduplication.ReplicatorWrapperTopology;
import java.util.List;
import java.util.stream.Collectors;

public class ExactlyOnceReplicator implements Initializable, Startable, Shutdownable {

  private final List<String> sourceNames;
  private final AppConfig appConfig;
  private final String intermediateName;
  private final String destinationName;

  private List<ReplicatorWrapperTopology> internalReplicators;
  private DeduplicatorUnwrapperTopology deduplicator;

  public ExactlyOnceReplicator(AppConfig appConfig, List<String> sourceNames,
      String intermediateName, String destinationName) {
    this.sourceNames = sourceNames;
    this.appConfig = appConfig;
    this.intermediateName = intermediateName;
    this.destinationName = destinationName;
  }

  @Override
  public void initialize(ServerContext context) throws Exception {
    this.internalReplicators = sourceNames
        .stream()
        .map(sourceName -> new ReplicatorWrapperTopology(appConfig, sourceName, intermediateName))
        .collect(Collectors.toList());
    for (ReplicatorWrapperTopology internalReplicator : internalReplicators) {
      internalReplicator.initialize(context);
    }
    this.deduplicator = new DeduplicatorUnwrapperTopology(appConfig, intermediateName, destinationName);
    deduplicator.initialize(context);
  }

  @Override
  public void start() {
    internalReplicators.forEach(ConsumerTopology::start);
    deduplicator.start();
  }

  @Override
  public void shutdown() {
    internalReplicators.forEach(ConsumerTopology::shutdown);
    deduplicator.shutdown();
  }

}
