package io.jexxa.esp.drivingadapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

public abstract class TypedEventListener<K, V> implements EventListener {

    @Override
    public void onEvent(ConsumerRecord<?, ?> onEvent) {
        ConsumerRecord<K, V> consumerRecord = (ConsumerRecord<K,V>)onEvent;
        onEvent(consumerRecord.key(), consumerRecord.value(), consumerRecord.headers());
    }

    protected abstract void onEvent(V value);

    protected void onEvent(K key, V value)
    {
        onEvent(value);
    }

    protected void onEvent(K key, V value, Headers headers)
    {
        onEvent(key, value);
    }

}
