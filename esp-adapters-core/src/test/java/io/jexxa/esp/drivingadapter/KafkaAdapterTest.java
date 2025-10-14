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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.jexxa.esp.drivenadapter.kafka.KafkaSender.kafkaSender;
import static java.time.Instant.now;
import static org.awaitility.Awaitility.await;

class KafkaAdapterTest {

    private static final String TEST_MESSAGE1_JSON_TOPIC = "test-message1-json-topic";
    private static final String TEST_MESSAGE2_JSON_TOPIC = "test-message2-json-topic";

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
    void receiveFromAdapterTest() {
        //Arrange
        var expectedResult = new KafkaFirstTestMessage(1, Instant.now(), "test message");

        Properties consumerProperties = DIGI_SPINE.kafkaProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var objectUnderTest = new KafkaAdapter(consumerProperties);
        var listener = new KafkaListenerFirstTestMessage();
        objectUnderTest.register(listener);

        var sender = kafkaSender( String.class, KafkaFirstTestMessage.class, DIGI_SPINE.kafkaProperties());
        //Act
        objectUnderTest.start();
        sender.send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_MESSAGE1_JSON_TOPIC)
                .asJSON();

        //Assert/Await
        await().atMost(15, TimeUnit.SECONDS).until( () -> (!listener.getResult().isEmpty()));
        Assertions.assertEquals(expectedResult, listener.getResult().get(0));
        objectUnderTest.stop();
    }


    @Test
    void multipleTopicsTest() {
        //Arrange
        var expectedResult1 = new KafkaFirstTestMessage(1, Instant.now(), "test message");
        var expectedResult2 = new KafkaSecondTestMessage("Hello", Instant.now(), "test message");

        Properties consumerProperties = DIGI_SPINE.kafkaProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var objectUnderTest = new KafkaAdapter(consumerProperties);
        var firstListener = new KafkaListenerFirstTestMessage();
        var secondListener = new KafkaListenerSecondTestMessage();

        objectUnderTest.register(firstListener);
        objectUnderTest.register(secondListener);

        //Act
        objectUnderTest.start();
        kafkaSender( String.class, KafkaFirstTestMessage.class, DIGI_SPINE.kafkaProperties())
                .send("test", expectedResult1)
                .withTimestamp(now())
                .toTopic(TEST_MESSAGE1_JSON_TOPIC)
                .asJSON();

        kafkaSender( String.class, KafkaSecondTestMessage.class, DIGI_SPINE.kafkaProperties())
                .send("test", expectedResult2)
                .withTimestamp(now())
                .toTopic(TEST_MESSAGE2_JSON_TOPIC)
                .asJSON();


        //Assert/Await
        await().atMost(15, TimeUnit.SECONDS).until( () -> (!firstListener.getResult().isEmpty()));
        await().atMost(15, TimeUnit.SECONDS).until( () -> (!secondListener.getResult().isEmpty()));

        Assertions.assertEquals(expectedResult1, firstListener.getResult().get(0));
        Assertions.assertEquals(expectedResult2, secondListener.getResult().get(0));

        objectUnderTest.stop();
    }



    record KafkaFirstTestMessage(int counter, Instant timestamp, String message) { }
    record KafkaSecondTestMessage(String message1, Instant timestamp, String message2) { }


    static class KafkaListenerFirstTestMessage extends TypedEventListener<String, KafkaFirstTestMessage>
    {
        private final List<KafkaFirstTestMessage> result = new ArrayList<>();
        private final String groupID = UUID.randomUUID().toString(); // Since we use this listener in multiple tests, we need a unique groupid for each test
        KafkaListenerFirstTestMessage() {
            super(String.class, KafkaFirstTestMessage.class);
        }

        @Override
        protected void onEvent(KafkaFirstTestMessage value) {
            result.add(value);
        }

        @Override
        public String topic() {
            return TEST_MESSAGE1_JSON_TOPIC;
        }

        @Override
        public String groupID() {
            return groupID;
        }

        public List<KafkaFirstTestMessage> getResult() {
            return result;
        }
    }


    static class KafkaListenerSecondTestMessage extends TypedEventListener<String, KafkaSecondTestMessage>
    {
        private final List<KafkaSecondTestMessage> result = new ArrayList<>();
        KafkaListenerSecondTestMessage() {
            super(String.class, KafkaSecondTestMessage.class);
        }

        @Override
        protected void onEvent(KafkaSecondTestMessage value) {
            result.add(value);
        }

        @Override
        public String topic() {
            return TEST_MESSAGE2_JSON_TOPIC;
        }

        public List<KafkaSecondTestMessage> getResult() {
            return result;
        }


    }

}