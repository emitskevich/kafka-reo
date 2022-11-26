package com.github.emitskevich.core.utils;

import java.io.InputStream;

public class ResourceReader {

  public static String read(String path) {
    InputStream inputStream = ResourceReader.class.getClassLoader().getResourceAsStream(path);
    if (inputStream == null) {
      throw new IllegalArgumentException("No such resource: " + path);
    }
    return StringUtils.toString(inputStream);
  }
}
