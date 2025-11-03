package io.jexxa.esp.drivenadapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.jexxa.esp.BrokerUtilities.deleteTopics;
import static io.jexxa.esp.BrokerUtilities.getBootstrapServers;
import static io.jexxa.esp.BrokerUtilities.kafkaProperties;
import static io.jexxa.esp.BrokerUtilities.topicExist;
import static io.jexxa.esp.drivenadapter.kafka.KafkaPool.createTopic;
import static io.jexxa.esp.drivenadapter.kafka.KafkaPool.topicExists;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaPoolIT {

    @BeforeEach
    void initTest()
    {
        deleteTopics();
    }

    @Test
    void failFastSuccess()
    {
        //Arrange
        Properties consumerProps = kafkaProperties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        consumerProps.put("schema.registry.url", "http://127.0.0.1:8081");


        //Act / Assert
        assertDoesNotThrow(() -> KafkaPool.validateKafkaConnection(consumerProps));
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
        var filterProperties = kafkaProperties();
        var testTopic = "TestTopic";

        //Act
        createTopic(filterProperties, testTopic, 1, 1);

        //Assert
        assertTrue(topicExists(filterProperties, testTopic));
        assertTrue(topicExist(testTopic));
    }

    @Test
    void testRetentionTime()
    {
        //Arrange
        var filterProperties = kafkaProperties();
        var testTopic = "TestTopic";
        createTopic(filterProperties, testTopic, 1, 1);

        //Act
        KafkaPool.setRetentionForTopic(filterProperties, testTopic, 365, TimeUnit.DAYS);

        //Assert
        assertTrue(topicExist(testTopic));
    }


}