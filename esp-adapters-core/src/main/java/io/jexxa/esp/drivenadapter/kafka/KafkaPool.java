package io.jexxa.esp.drivenadapter.kafka;

import io.jexxa.adapterapi.JexxaContext;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;


public class KafkaPool {
    @SuppressWarnings("unused")
    private static final KafkaPool INSTANCE = new KafkaPool();

    private static final Map<Properties, KafkaProducer<Object,Object>> producerMap = Collections.synchronizedMap(new ConcurrentHashMap<>());

    public static KafkaProducer<Object,Object> kafkaProducer(Properties properties)
    {
        return producerMap.computeIfAbsent(properties, entry -> new KafkaProducer<>(properties));
    }

    public void cleanup()
    {
        producerMap.values().forEach(KafkaProducer::flush);
        producerMap.values().forEach(KafkaProducer::close);
        producerMap.clear();
    }

    public static void validateKafkaConnection(Properties filterProperties)
    {
        if (filterProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        {
            var properties = new Properties();
            properties.putAll(filterProperties);
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            properties.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 5000);
            properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
            try( AdminClient adminClient = AdminClient.create(properties) )
            {
                var result = adminClient.describeCluster().nodes().get();
                if (result == null || result.isEmpty()) {
                    throw new RuntimeException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
            }
            catch ( ExecutionException e)
            {
                throw new RuntimeException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
            }
        }
    }

    private KafkaPool()
    {
      /*  registerFailFastHandler(properties -> cleanup());
        registerFailFastHandler(this::validateKafkaConnection);*/
        JexxaContext.registerCleanupHandler(this::cleanup);
    }


}
