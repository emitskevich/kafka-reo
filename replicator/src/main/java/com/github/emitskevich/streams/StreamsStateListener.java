package com.github.emitskevich.streams;

import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

import com.github.emitskevich.core.utils.SimpleScheduler;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsStateListener implements StateListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamsStateListener.class);
  private final Supplier<State> stateSupplier;

  public StreamsStateListener(Supplier<State> stateSupplier) {
    this.stateSupplier = stateSupplier;
    Duration delay = Duration.ofSeconds(30);
    SimpleScheduler scheduler = SimpleScheduler.createSingleThreaded();
    scheduler.scheduleWithFixedDelay(this::logWaiting, delay);
  }


  private void logWaiting() {
    State state = stateSupplier.get();
    if (state != RUNNING) {
      LOGGER.info("App is in {} state, please wait...", state);
    }
  }

  @Override
  public void onChange(State newState, State oldState) {
    LOGGER.info("App state changed: {} -> {}", oldState, newState);
  }
}
