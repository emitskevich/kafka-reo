package com.github.emitskevich.core.config;

import java.util.Set;
import org.jetbrains.annotations.Nullable;

public class EnvConfigProvider implements ConfigProvider {

  @Override
  public boolean hasEntry(String key) {
    return getValue(key) != null;
  }

  @Override
  public @Nullable String getValue(String key) {
    String envName = toEnvName(key);
    return System.getenv(envName);
  }

  @Override
  public @Nullable Set<String> getChildren(String key) {
    return null;
  }

  private String toEnvName(String name) {
    return name
        .replace(".", "_")
        .replace("-", "_")
        .toUpperCase();
  }
}
