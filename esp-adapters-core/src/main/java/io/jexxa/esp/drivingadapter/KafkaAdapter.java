package io.jexxa.esp.drivingadapter;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.jexxa.adapterapi.drivingadapter.IDrivingAdapter;
import io.jexxa.adapterapi.invocation.InvocationManager;
import io.jexxa.adapterapi.invocation.JexxaInvocationHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.jexxa.common.facade.logger.SLF4jLogger.getLogger;
import static io.jexxa.common.facade.utils.properties.PropertiesPrefix.globalPrefix;
import static io.jexxa.common.facade.utils.properties.PropertiesUtils.removePrefixFromKeys;
import static java.util.Collections.singletonList;

public class KafkaAdapter implements IDrivingAdapter {
    private static final String JSON_KEY_TYPE = "json.key.type";
    private static final String JSON_VALUE_TYPE = "json.value.type";
    private final List<InnerKafkaStruct> eventListeners = new ArrayList<>();

    private final Properties properties;

    private ExecutorService executor;

    public KafkaAdapter(Properties properties) {
        this.properties = removePrefixFromKeys(properties, globalPrefix());
    }

    @Override
    public void register(Object port) {
        var eventListener = (EventListener)(port);
        var listenerProperties = createListenerProperties(properties, eventListener);
        var consumer = new KafkaConsumer<>(listenerProperties);
        var topic = eventListener.topic();

        consumer.subscribe(singletonList(topic));
        eventListeners.add(new InnerKafkaStruct(eventListener, consumer));

        getLogger(KafkaAdapter.class).info("Listening for messages on topic: {}", topic);
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(eventListeners.size());
        eventListeners.forEach(element -> executor.submit( element::run ));
    }


    @Override
    public synchronized void stop() {
        eventListeners.forEach(InnerKafkaStruct::stop);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                getLogger(KafkaConsumer.class).warn("Force shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(e);
        }
    }

    private Properties createListenerProperties(Properties properties, EventListener eventListener)
    {
        var listenerProperties = new Properties();
        listenerProperties.putAll(properties);

        // Add type information for JSON deserialization
        listenerProperties.putIfAbsent(JSON_KEY_TYPE, eventListener.keyType().getName());
        listenerProperties.putIfAbsent(JSON_VALUE_TYPE, eventListener.valueType().getName());
        listenerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, eventListener.groupID());
        listenerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        listenerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());

        //Configure autocommit
        listenerProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if(!Objects.equals(listenerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false")) {
            getLogger(KafkaAdapter.class).warn("{} is not set to false -> This can cause message lost in case of an exception during processing the message", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        }

        return listenerProperties;
    }


    private static class InnerKafkaStruct
    {
        private final KafkaConsumer<?, ?> kafkaConsumer;
        private final EventListener eventListener;
        private final JexxaInvocationHandler invocationHandler;
        private boolean isRunning = false;

        InnerKafkaStruct(EventListener eventListener, KafkaConsumer<?, ?> kafkaConsumer)
        {
            this.kafkaConsumer = kafkaConsumer;
            this.eventListener = eventListener;
            this.invocationHandler = InvocationManager.getInvocationHandler(eventListener);
        }

        public void run(){
            isRunning = true;
            while (isRunning) {
                ConsumerRecords<?, ?> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<?, ?> consumerRecord : records) {
                    processRecord(consumerRecord);
                }
            }
            kafkaConsumer.close();
        }

        private void processRecord(ConsumerRecord<?,?> consumerRecord) {
            var retryCounter = 0;
            while (retryCounter < 3) {
                try {
                    invocationHandler.invoke(eventListener, eventListener::onEvent, consumerRecord);
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1)
                    ));
                    return;
                } catch (Exception e) {
                    getLogger(KafkaAdapter.class).warn("Could not process record, try again");
                    ++retryCounter;
                }
                try {
                    Thread.sleep(10L * retryCounter); // <-- 10 ms warten
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    //Ignore
                }

            }
            getLogger(KafkaAdapter.class).error("Could not process record. Giving up after 3 retries. Message is discarded ...");
        }

        public void stop()
        {
            isRunning = false;
            kafkaConsumer.wakeup();
        }
    }

}
