package io.jexxa.esp.drivingadapter;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.jexxa.esp.digispine.DigiSpine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.jexxa.esp.drivenadapter.kafka.KafkaSender.kafkaSender;
import static java.time.Instant.now;
import static org.awaitility.Awaitility.await;

class KafkaAdapterTest {

    private static final String TEST_JSON_TOPIC = "test-json-topic";

    private static final DigiSpine DIGI_SPINE = new DigiSpine();


    @BeforeEach
    void resetDigiSpine() {
        DIGI_SPINE.reset();
    }

    @AfterAll
    static void stopDigiSpine() {
        DIGI_SPINE.stop();
    }

    @Test
    void startAdapter() {
        //Arrange
        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");

        Properties consumerProperties = DIGI_SPINE.kafkaProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaAdapterTest.class.getSimpleName());


        var objectUnderTest = new KafkaAdapter(consumerProperties);
        var listener = new MyKafkaListener();
        objectUnderTest.register(listener);

        var sender = kafkaSender( String.class, KafkaTestMessage.class, DIGI_SPINE.kafkaProperties());
        //Act
        objectUnderTest.start();
        sender.send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_JSON_TOPIC)
                .asJSON();

        //Assert/Await
        await().atMost(15, TimeUnit.SECONDS).until( () -> (!listener.getResult().isEmpty()));
        Assertions.assertEquals(expectedResult, listener.getResult().get(0));
        objectUnderTest.stop();
    }

    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

    public static class MyKafkaListener extends TypedEventListener<String, KafkaTestMessage>
    {
        private final List<KafkaTestMessage> result = new ArrayList<>();
        MyKafkaListener() {
            super(String.class, KafkaTestMessage.class);
        }

        @Override
        protected void onEvent(KafkaTestMessage value) {
            result.add(value);
        }

        @Override
        public String getTopic() {
            return TEST_JSON_TOPIC;
        }

        public List<KafkaTestMessage> getResult() {
            return result;
        }


    }
}