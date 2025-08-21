package io.jexxa.esp.drivenadapter.kafka;

import io.jexxa.esp.digispine.DigiSpine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static io.jexxa.esp.drivenadapter.kafka.KafkaESPProducer.kafkaESPProducer;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaESPProducerTest {
    private static final String TEST_TEXT_TOPIC = "test-text-topic";
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
    void sendAsJSON() {
        //Arrange
        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");

        var objectUnderTest = kafkaESPProducer( String.class,
                KafkaTestMessage.class,
                DIGI_SPINE.kafkaProperties());

        //Act
        objectUnderTest
                .send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_JSON_TOPIC)
                .asJSON();

        var result = DIGI_SPINE.latestMessageFromJSON(TEST_JSON_TOPIC, Duration.ofMillis(500), KafkaTestMessage.class);

        assertTrue(result.isPresent());
        assertEquals(expectedResult, result.get());
    }


    @Test
    void sendAsText() {
        // Arrange
        var objectUnderTest = kafkaESPProducer( String.class,
                KafkaTestMessage.class,
                DIGI_SPINE.kafkaProperties());

        var expectedResult = new KafkaTestMessage(1, Instant.now(), "test message");

        //Act
        objectUnderTest
                .send("test", expectedResult)
                .withTimestamp(now())
                .toTopic(TEST_TEXT_TOPIC)
                .asText();

        var result = DIGI_SPINE.latestMessage(TEST_TEXT_TOPIC, Duration.ofMillis(500));

        assertTrue(result.isPresent());
        assertEquals(expectedResult.toString(), result.get());
    }


    public record KafkaTestMessage(int counter, Instant timestamp, String message) { }

}