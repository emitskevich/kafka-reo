package com.github.emitskevich.core.utils;

public class MathUtils {

  private MathUtils() {
  }

  public static double round(double value, int decimals) {
    long base = (long) Math.pow(10, decimals);
    return (double) Math.round(value * base) / base;
  }
}
