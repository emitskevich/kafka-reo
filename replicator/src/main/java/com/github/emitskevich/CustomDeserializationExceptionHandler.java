package com.github.emitskevich;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CustomDeserializationExceptionHandler.class);

  @Override
  public DeserializationHandlerResponse handle(ProcessorContext context,
      ConsumerRecord<byte[], byte[]> record, Exception exception) {
    LOGGER.error(
        "Exception caught during Deserialization, taskId: {}, topic: {}, partition: {}, offset: {}",
        context.taskId(), record.topic(), record.partition(), record.offset(), exception
    );
    return DeserializationHandlerResponse.FAIL;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    // ignore
  }
}
