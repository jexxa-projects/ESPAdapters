package io.jexxa.esp.drivenadapter;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

import java.util.Properties;

public abstract class EventSender<K,V> {

    @CheckReturnValue
    public EventStreamBuilder<K,V> send(V eventData){
        return new EventStreamBuilder<>(eventData, this);
    }

    @CheckReturnValue
    public EventStreamBuilder<K,V> send(K key, V eventData){
        return new EventStreamBuilder<>(key, eventData, this);
    }

    protected void sendAsJSON(K key, V eventData, String topic, Long timestamp)
    {
        sendAsJSON(key, eventData, topic,timestamp, new Properties());
    }

    protected void sendAsAVRO(K key, V eventData, String topic, Long timestamp)
    {
        sendAsAVRO(key, eventData, topic, timestamp, new Properties());
    }

    protected void sendAsText(K key, V eventData, String topic, Long timestamp)
    {
        sendAsText(key, eventData, topic, timestamp, new Properties());
    }

    protected abstract void sendAsJSON(K key, V eventData, String topic, Long timestamp, Properties headers);

    protected abstract void sendAsAVRO(K key, V eventData, String topic, Long timestamp, Properties headers);

    protected abstract void sendAsText(K key, V eventData, String topic, Long timestamp, Properties headers);

}
