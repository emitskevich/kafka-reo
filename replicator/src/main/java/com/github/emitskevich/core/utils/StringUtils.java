package com.github.emitskevich.core.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StringUtils {

  public static String toString(InputStream inputStream) {
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

      String read;
      while ((read = br.readLine()) != null) {
        sb.append("\n").append(read);
      }

      br.close();
      return sb.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String beforeFirstOccurrence(String source, String delimiter) {
    int index = source.indexOf(delimiter);
    if (index == -1) {
      return source;
    }
    return source.substring(0, index);
  }

  public static String beforeFirstOccurrences(String source, String... delimiters) {
    String result = source;
    for (String delimiter : delimiters) {
      result = beforeFirstOccurrence(result, delimiter);
    }
    return result;
  }

  public static String afterFirstOccurrence(String source, String delimiter) {
    int index = source.indexOf(delimiter);
    if (index == -1) {
      return source;
    }
    return source.substring(index + delimiter.length());
  }

  public static String afterLastOccurrence(String source, String delimiter) {
    int index = source.lastIndexOf(delimiter);
    if (index == -1) {
      return source;
    }
    return source.substring(index + delimiter.length());
  }
}