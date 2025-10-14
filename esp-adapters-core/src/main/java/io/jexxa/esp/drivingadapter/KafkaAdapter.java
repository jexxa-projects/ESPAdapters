package io.jexxa.esp.drivingadapter;

import io.jexxa.adapterapi.drivingadapter.IDrivingAdapter;
import io.jexxa.common.facade.logger.SLF4jLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class KafkaAdapter implements IDrivingAdapter {
    private static final String JSON_KEY_TYPE = "json.key.type";
    private static final String JSON_VALUE_TYPE = "json.value.type";
    private final List<InnerKafkaStruct> eventListener = new ArrayList<>();

    private final Properties properties;

    private ExecutorService executor;

    public KafkaAdapter(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void register(Object port) {
        var eventListener = (EventListener)(port);
        var listenerProperties = new Properties();
        listenerProperties.putAll(properties);

        // Add type information for JSON deserialization
        if (!listenerProperties.containsKey(JSON_KEY_TYPE)) {
            listenerProperties.put(JSON_KEY_TYPE, eventListener.keyType().getName());
        }
        if (!listenerProperties.containsKey(JSON_VALUE_TYPE)) {
            listenerProperties.put(JSON_VALUE_TYPE, eventListener.valueType().getName());
        }

        var consumer = new KafkaConsumer<>(listenerProperties);
        consumer.subscribe(singletonList(eventListener.getTopic()));
        this.eventListener.add(new InnerKafkaStruct(eventListener, consumer));
        SLF4jLogger.getLogger(KafkaAdapter.class).info("Listening for messages on topic: {}", eventListener.getTopic());
    }

    @Override
    synchronized public void start() {
        executor = Executors.newFixedThreadPool(eventListener.size());
        eventListener.forEach( element -> executor.submit( element::run ));
    }


    @Override
    synchronized public void stop() {
        eventListener.forEach(InnerKafkaStruct::stop);
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


    private static class InnerKafkaStruct
    {
        private final KafkaConsumer<?, ?> kafkaConsumer;
        private final EventListener eventListener;
        private boolean isRunning = false;

        InnerKafkaStruct(EventListener eventListener, KafkaConsumer<?, ?> kafkaConsumer)
        {
            this.kafkaConsumer = kafkaConsumer;
            this.eventListener = eventListener;
        }

        public void run(){
            isRunning = true;
            while (isRunning) {
                ConsumerRecords<?, ?> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<?, ?> record : records) {
                    eventListener.onEvent(record);
                }
            }
            kafkaConsumer.close();
        }

        public void stop()
        {
            isRunning = false;
            kafkaConsumer.wakeup();
        }
    }

}
