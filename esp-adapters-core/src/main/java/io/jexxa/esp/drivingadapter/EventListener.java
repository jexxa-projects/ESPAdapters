package io.jexxa.esp.drivingadapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface EventListener<K, V> {
    void onEvent(ConsumerRecord<K, V> record);

    String getTopic();
}
