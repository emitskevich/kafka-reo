package com.github.emitskevich.core.server;

import com.github.emitskevich.core.utils.ClassUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerContext.class);

  private final Map<Class<?>, Object> instanceMap = new HashMap<>();
  private final List<Object> instanceList = new ArrayList<>();
  private final Set<Object> initialized = new HashSet<>();

  public <T> void register(Class<T> type, T instance) {
    instanceMap.put(type, instance);
    instanceList.add(instance);
  }

  public <T> T getInstance(Class<T> type) {
    Object object = instanceMap.get(type);
    if (object == null) {
      throw new IllegalStateException("Instance of type " + type + " wasn't registered");
    }
    return ClassUtils.castUnchecked(object);
  }

  void initialize() throws Exception {
    for (Object instance : instanceList) {
      if (instance instanceof Initializable initializable) {
        LOGGER.info("Initializing {}...", getName(initializable));
        initializable.initialize(this);
        initialized.add(initializable);
      }
    }
    LOGGER.info("Initialized.");
  }

  List<Startable> getStartables() {
    return instanceList
        .stream()
        .filter(i -> i instanceof Startable)
        .map(i -> (Startable) i)
        .collect(Collectors.toList());
  }

  void shutdown() {
    List<Object> instancesReversed = new ArrayList<>(instanceList);
    Collections.reverse(instancesReversed);
    for (Object instance : instancesReversed) {
      if (instance instanceof Shutdownable shutdownable) {
        if (initialized.contains(shutdownable)) {
          LOGGER.info("Shutting down {}...", getName(shutdownable));
          shutdownable.shutdown();
        }
      }
    }
    LOGGER.info("Stopped.");
  }

  private String getName(Object instance) {
    return instance.getClass().getSimpleName();
  }
}
