package io.jexxa.esp.drivenadapter.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.jexxa.esp.drivenadapter.ESPProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Properties;

import static io.jexxa.esp.drivenadapter.kafka.KafkaPool.kafkaProducer;
import static java.util.Objects.requireNonNull;

public class KafkaESPProducer<K,V> extends ESPProducer<K,V> {
    private final Properties filterProperties;

    public static <K, V> KafkaESPProducer<K,V> kafkaESPProducer(Class<K> keyClazz,
                                                                Class<V> valueClazz,
                                                                Properties filterProperties)
    {
        requireNonNull(keyClazz);
        requireNonNull(valueClazz);
        return new KafkaESPProducer<>(filterProperties);
    }

    protected KafkaESPProducer(Properties filterProperties) {
        this.filterProperties = filterProperties;
    }

    @Override
    protected void sendAsJSON(K key, V eventData, String topic, Long timestamp, Properties headers) {
        setSerializerIfAbsent(KafkaJsonSchemaSerializer.class);
        internalSend(key, eventData, topic, timestamp, headers);
    }

    @Override
    protected void sendAsAVRO(K key, V eventData, String topic, Long timestamp, Properties headers) {
        //Not implemented yet
    }

    @Override
    protected void sendAsText(K key, V eventData, String topic, Long timestamp, Properties headers) {
        setSerializerIfAbsent(GenericStringSerializer.class);
        internalSend(key, eventData, topic, timestamp, headers);
    }

    private <T> void setSerializerIfAbsent(Class<T> clazz)
    {
        this.filterProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                filterProperties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clazz.getName())
        );

        this.filterProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                filterProperties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, clazz.getName())
        );
    }

    private void internalSend(Object key, Object eventData, String topic, Long timestamp, Properties headers)
    {
        var producer = kafkaProducer(filterProperties);
        var producerRecord = new ProducerRecord<>(topic, null, timestamp, key, eventData);

        headers.forEach( (hKey, hValue) -> producerRecord.headers().add(
                new RecordHeader((String)hKey, ((String)hValue).getBytes()))
        );

        producer.send(producerRecord);
        producer.flush();
    }

}
