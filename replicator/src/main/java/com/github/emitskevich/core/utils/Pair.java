package com.github.emitskevich.core.utils;

import java.util.Objects;

public class Pair<T1, T2> {

  private final T1 left;
  private final T2 right;

  public Pair(T1 left, T2 right) {
    this.left = left;
    this.right = right;
  }

  public static <T1, T2> Pair<T1, T2> of(T1 first, T2 second) {
    return new Pair<>(first, second);
  }

  public T1 getLeft() {
    return left;
  }

  public T2 getRight() {
    return right;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Pair<?, ?> pair = (Pair<?, ?>) o;

    if (!Objects.equals(left, pair.left)) {
      return false;
    }
    return Objects.equals(right, pair.right);
  }

  @Override
  public int hashCode() {
    int result = left != null ? left.hashCode() : 0;
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Pair{" +
        "left=" + left +
        ", right=" + right +
        '}';
  }
}
