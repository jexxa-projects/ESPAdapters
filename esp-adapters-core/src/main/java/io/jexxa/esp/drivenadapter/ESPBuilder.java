package io.jexxa.esp.drivenadapter;

import io.jexxa.common.facade.utils.annotation.CheckReturnValue;

import java.time.Instant;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ESPBuilder<K,V> {
    private final K key;
    private final V event;
    private final ESPProducer<K,V> espProducer;
    private final Properties headers = new Properties();

    private Long timestamp = null;
    private String topic;

    protected ESPBuilder(V event, ESPProducer<K,V> espProducer){
        this(null, event, espProducer);
    }

    protected ESPBuilder(K key, V event, ESPProducer<K,V> espProducer){
        this.key = key;
        this.event = event;
        this.espProducer = espProducer;
    }

    @CheckReturnValue
    public ESPBuilder<K,V> withTimestamp(Instant timestamp){
        this.timestamp = timestamp.toEpochMilli();
        return this;
    }

    @CheckReturnValue
    public ESPBuilder<K,V> toTopic(String topic){
        this.topic = requireNonNull(topic);
        return this;
    }

    @CheckReturnValue
    public ESPBuilder<K,V>  addHeader(String key, String value)
    {
        headers.put(key, value);

        return this;
    }

    public void asJSON(){
        espProducer.sendAsJSON(key,
                event,
                requireNonNull(topic),
                timestamp, headers);
    }

    public void asAVRO(){
        espProducer.sendAsAVRO(key,
                event,
                requireNonNull(topic),
                timestamp, headers);
    }

    public void asText(){
        espProducer.sendAsText(key,
                event,
                requireNonNull(topic),
                timestamp, headers);
    }
}
