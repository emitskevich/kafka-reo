package com.github.emitskevich.core.config;

import com.github.emitskevich.core.utils.ResourceReader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.yaml.snakeyaml.Yaml;

public class FileConfigProvider implements ConfigProvider {

  private final Map<String, Object> raw;

  public FileConfigProvider(String filePath) {
    String yamlContent = ResourceReader.read(filePath);
    Yaml yaml = new Yaml();
    this.raw = yaml.load(yamlContent);
  }

  @Override
  public boolean hasEntry(String key) {
    String[] keyParts = key.split("\\.");
    Map<String, Object> source = raw;
    for (int i = 0; i < keyParts.length - 1; i++) {
      Object nextSource = source.get(keyParts[i]);
      if (nextSource instanceof Map<?, ?>) {
        source = castUnchecked(nextSource);
      } else {
        return false;
      }
    }
    return source.containsKey(keyParts[keyParts.length - 1]);
  }

  @Override
  public @Nullable String getValue(String key) {
    String[] keyParts = key.split("\\.");
    Map<String, Object> source = raw;
    for (int i = 0; i < keyParts.length - 1; i++) {
      Object nextSource = source.get(keyParts[i]);
      if (nextSource instanceof Map<?, ?>) {
        source = castUnchecked(nextSource);
      } else {
        return null;
      }
    }
    Object value = source.get(keyParts[keyParts.length - 1]);
    if (value instanceof Map<?, ?>) {
      throw new IllegalArgumentException("Requested config key \"" + key + "\" is not leaf entry");
    }
    return (String) value;
  }

  @Override
  public @Nullable Set<String> getChildren(String key) {
    String[] keyParts = key.split("\\.");
    Map<String, Object> source = raw;
    for (String keyPart : keyParts) {
      Object nextSource = source.get(keyPart);
      if (nextSource == null) {
        return Collections.emptySet();
      }
      if (!(nextSource instanceof Map<?, ?>)) {
        return null;
      }

      source = castUnchecked(nextSource);
    }
    return source.keySet();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> castUnchecked(Object object) {
    return (Map<String, Object>)object;
  }
}
