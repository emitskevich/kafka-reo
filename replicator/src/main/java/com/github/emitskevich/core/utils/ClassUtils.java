package com.github.emitskevich.core.utils;

public class ClassUtils {

  private ClassUtils() {
  }

  @SuppressWarnings("unchecked")
  public static <T> T castUnchecked(Object object) {
    return (T) object;
  }
}
