package com.github.emitskevich;

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
  private final String sourceName;
  private final String destinationName;
  private final Supplier<State> stateSupplier;

  public StreamsStateListener(String sourceName, String destinationName,
      Supplier<State> stateSupplier) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.stateSupplier = stateSupplier;

    Duration delay = Duration.ofSeconds(30);
    SimpleScheduler scheduler = SimpleScheduler.createSingleThreaded();
    scheduler.scheduleWithFixedDelay(this::logWaiting, delay);
  }


  private void logWaiting() {
    State state = stateSupplier.get();
    if (state != RUNNING) {
      LOGGER.info(
          "\"{} -> {}\" app is in {} state, please wait...",
          sourceName, destinationName, state
      );
    }
  }

  @Override
  public void onChange(State newState, State oldState) {
    LOGGER.info(
        "\"{} -> {}\" app state changed: {} -> {}",
        sourceName, destinationName, oldState, newState
    );
  }
}
