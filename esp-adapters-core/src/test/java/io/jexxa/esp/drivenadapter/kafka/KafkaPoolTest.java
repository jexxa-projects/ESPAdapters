package io.jexxa.esp.drivenadapter.kafka;

import io.jexxa.esp.digispine.DigiSpine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaPoolTest {

    static final DigiSpine DIGI_SPINE = new DigiSpine();


    @AfterAll
    static void stopKafka() {
        DIGI_SPINE.stop();
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
}