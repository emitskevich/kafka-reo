package com.github.emitskevich.core.config;

import java.util.Set;
import org.jetbrains.annotations.Nullable;

public interface ConfigProvider {

  boolean hasEntry(String key);

  @Nullable
  String getValue(String key);

  @Nullable Set<String> getChildren(String key);
}
