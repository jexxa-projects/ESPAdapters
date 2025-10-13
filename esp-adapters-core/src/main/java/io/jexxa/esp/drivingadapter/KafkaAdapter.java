package io.jexxa.esp.drivingadapter;

import io.jexxa.adapterapi.drivingadapter.IDrivingAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAdapter<K, V> implements IDrivingAdapter, Runnable, AutoCloseable{

    private final KafkaConsumer<K, V> consumer;
    private boolean running = false;

    private EventListener<K, V> eventListener;

    public KafkaAdapter(Properties props) {
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void register(Object port) {
        this.eventListener = (EventListener<K,V>)(port);
    }

    @Override
    public void start() {
        consumer.subscribe(Collections.singletonList(eventListener.getTopic()));

        System.out.println("Listening for messages on topic: " + eventListener.getTopic());

        while (running) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<K, V> record : records) {
                System.out.printf("Received message: key=%s value=%s partition=%d offset=%d%n",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }


    @Override
    public void stop() {
        running = false;
        consumer.wakeup();
    }


    @Override
    public void run() {
        while (running) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<K, V> record : records) {
                System.out.printf("Received: %s%n", record.value());
                eventListener.onEvent(record);
            }
        }
        consumer.close();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
