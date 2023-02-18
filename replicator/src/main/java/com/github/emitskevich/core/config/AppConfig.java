package com.github.emitskevich.core.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class AppConfig {

  private final List<ConfigProvider> providers;

  private final Map<String, String> strings = new ConcurrentHashMap<>();
  private final Map<String, Integer> ints = new ConcurrentHashMap<>();
  private final Map<String, Long> longs = new ConcurrentHashMap<>();
  private final Map<String, Double> doubles = new ConcurrentHashMap<>();
  private final Map<String, Boolean> booleans = new ConcurrentHashMap<>();
  private final Map<String, Duration> durations = new ConcurrentHashMap<>();
  private final Map<String, List<String>> stringLists = new ConcurrentHashMap<>();
  private final Map<String, Set<String>> children = new ConcurrentHashMap<>();
  private final Map<String, Boolean> presence = new ConcurrentHashMap<>();

  public AppConfig(ConfigProvider... providers) {
    this.providers = Arrays.asList(providers);
  }

  public String getString(String name) {
    return get(strings, name, this::getStringUncached);
  }

  public int getInt(String name) {
    return get(ints, name, k -> Integer.parseInt(getStringUncached(k)));
  }

  public long getLong(String name) {
    return get(longs, name, k -> Long.parseLong(getStringUncached(k)));
  }

  public double getDouble(String name) {
    return get(doubles, name, k -> Double.parseDouble(getStringUncached(k)));
  }

  public boolean getBoolean(String name) {
    return get(booleans, name, k -> Boolean.parseBoolean(getStringUncached(k)));
  }

  public Duration getDuration(String name) {
    return get(durations, name, k -> Duration.parse(getStringUncached(k)));
  }

  public List<String> getStringList(String name) {
    return get(stringLists, name, k -> List.of(getStringUncached(k).split(",")));
  }

  public Set<String> getChildren(String name) {
    return get(children, name, this::getChildrenUncached);
  }

  public boolean hasEntry(String name) {
    return get(presence, name, this::hasEntryUncached);
  }

  private <T> T get(Map<String, T> settings, String key, Function<String, T> loader) {
    T result = settings.get(key);
    if (result != null) {
      return result;
    }
    return settings.computeIfAbsent(key, loader);
  }

  private String getStringUncached(String name) {
    for (ConfigProvider provider : providers) {
      String value = provider.getValue(name);
      if (value != null) {
        return value;
      }
    }
    String message = "No config provider returned value for key \"" + name + "\"";
    throw new IllegalArgumentException(message);
  }

  private Set<String> getChildrenUncached(String name) {
    for (ConfigProvider provider : providers) {
      Set<String> value = provider.getChildren(name);
      if (value != null) {
        return value;
      }
    }
    String message = "No config provider returned children for key \"" + name + "\"";
    throw new IllegalArgumentException(message);
  }

  private boolean hasEntryUncached(String name) {
    for (ConfigProvider provider : providers) {
      if (provider.hasEntry(name)) {
        return true;
      }
    }
    return false;
  }
}
