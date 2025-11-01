package io.jexxa.esp.drivenadapter.kafka;

import io.jexxa.esp.DigiSpine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.jexxa.esp.drivenadapter.kafka.KafkaPool.createTopic;
import static io.jexxa.esp.drivenadapter.kafka.KafkaPool.topicExists;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaPoolTest {

    static final DigiSpine DIGI_SPINE = new DigiSpine();


    @AfterAll
    static void stopKafka() {
        DIGI_SPINE.stop();
    }

    @AfterEach
    void resetKafka()
    {
        DIGI_SPINE.deleteTopics();
    }

    @Test
    void failFastSuccess()
    {
        //Arrange
        var filterProperties = DIGI_SPINE.kafkaProperties();

                //Act / Assert
        assertDoesNotThrow(() -> KafkaPool.validateKafkaConnection(filterProperties));
    }

    @Test
    void failFastFailure()
    {
        //Arrange
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:12345");

        //Act / Assert
        assertThrows(RuntimeException.class, () -> KafkaPool.validateKafkaConnection(consumerProps));
    }
    @Test
    void testCreateTopic()
    {
        //Arrange
        var filterProperties = DIGI_SPINE.kafkaProperties();
        var testTopic = "TestTopic";

        //Act
        createTopic(filterProperties, testTopic, 1, 1);

        //Assert
        assertTrue(topicExists(filterProperties, testTopic));
        assertTrue(DIGI_SPINE.topicExist(testTopic));
    }

    @Test
    void testRetentionTime()
    {
        //Arrange
        var filterProperties = DIGI_SPINE.kafkaProperties();
        var testTopic = "TestTopic";
        createTopic(filterProperties, testTopic, 1, 1);

        //Act
        KafkaPool.setRetentionForTopic(filterProperties, testTopic, 365, TimeUnit.DAYS);

        //Assert
        assertTrue(DIGI_SPINE.topicExist(testTopic));
    }

}