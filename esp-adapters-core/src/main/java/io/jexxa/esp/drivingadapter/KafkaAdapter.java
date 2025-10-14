package io.jexxa.esp.drivingadapter;

import io.jexxa.adapterapi.drivingadapter.IDrivingAdapter;
import io.jexxa.common.facade.logger.SLF4jLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class KafkaAdapter implements IDrivingAdapter {

    private KafkaConsumer<?, ?> consumer;
    private boolean running = false;
    private final Properties properties;

    private EventListener eventListener;
    private final ExecutorService executor = newSingleThreadExecutor();

    public KafkaAdapter(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void register(Object port) {
        this.eventListener = (EventListener)(port);

        properties.put("json.key.type", eventListener.keyType().getName());
        properties.put("json.value.type", eventListener.valueType().getName());
        consumer = new KafkaConsumer<>(properties);

    }

    @Override
    synchronized public void start() {
        consumer.subscribe(Collections.singletonList(eventListener.getTopic()));

        SLF4jLogger.getLogger(KafkaAdapter.class).info("Listening for messages on topic: {}", eventListener.getTopic());

        running = true;
        executor.submit(this::run);
    }


    @Override
    synchronized public void stop() {
        running = false;
        if (consumer != null ) {
            consumer.wakeup();
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                SLF4jLogger.getLogger(KafkaConsumer.class).warn("Force shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void run() {
        while (running) {
            ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<?, ?> record : records) {
                eventListener.onEvent(record);
            }
        }
        consumer.close();
    }

}
