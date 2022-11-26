package com.github.emitskevich;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomProductionExceptionHandler implements ProductionExceptionHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CustomProductionExceptionHandler.class);

  @Override
  public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
      Exception exception) {
    LOGGER.error(
        "Exception caught during Production, topic: {}, partition: {}",
        record.topic(), record.partition(), exception
    );
    return ProductionExceptionHandlerResponse.FAIL;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    // ignore
  }
}
