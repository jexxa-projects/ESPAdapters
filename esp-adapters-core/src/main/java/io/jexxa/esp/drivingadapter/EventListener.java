package io.jexxa.esp.drivingadapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface EventListener {
    void onEvent(ConsumerRecord<?, ?> onEvent);
    Class<?> keyType();
    Class<?> valueType();
    String getTopic();
}
