package com.github.emitskevich.utils;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public final class UncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

  private final StreamThreadExceptionResponse response;

  public UncaughtExceptionHandler(StreamThreadExceptionResponse response) {
    this.response = response;
  }

  @Override
  public StreamThreadExceptionResponse handle(Throwable exception) {
    return response;
  }
}
