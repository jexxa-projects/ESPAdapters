package io.jexxa.esp.drivenadapter.kafka;

import io.jexxa.adapterapi.JexxaContext;
import io.jexxa.common.facade.logger.SLF4jLogger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@SuppressWarnings({"java:S6548"})
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

    public static void setRetentionForTopic(Properties properties, String topic, long duration, TimeUnit timeUnit) {
        var adminClientProperties = getAdminClientProperties(properties);
        long retentionMs = timeUnit.toMillis(duration);

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry retentionEntry = new ConfigEntry("retention.ms", String.valueOf(retentionMs));
        AlterConfigOp retentionOp = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);

        try (AdminClient admin = AdminClient.create(adminClientProperties)) {
            admin.incrementalAlterConfigs(
                    Collections.singletonMap(topicResource, Collections.singleton(retentionOp))
            ).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        } catch (ExecutionException e){
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        }
    }
    public static void createTopic(Properties properties, String topic) {
        createTopic(properties, topic, 1, 3);
    }

    public static boolean topicExists(Properties properties, String topic) {
        var adminClientProperties = getAdminClientProperties(properties);
        try (AdminClient admin = AdminClient.create(adminClientProperties)) {
            return topicExists(admin, topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        } catch (ExecutionException e){
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        }
    }

    public static void createTopic(Properties properties, String topic, int numPartitions, int replicationFactor)
    {
        var broker = properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        var adminClientProperties = getAdminClientProperties(properties);

        try (AdminClient admin = AdminClient.create(adminClientProperties)) {
            if (topicExists(admin, topic))
            {
                SLF4jLogger.getLogger(KafkaPool.class).info("Kafka topic {} already exists on {} ", topic, broker);
                return;
            }

            NewTopic newTopic = new NewTopic(topic, numPartitions, (short)replicationFactor);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        } catch (ExecutionException e){
            throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
        }
    }


    private static Set<String> getTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        return adminClient.listTopics().names().get();
    }

    private static boolean topicExists(AdminClient adminClient, String topic) throws ExecutionException, InterruptedException {
        return getTopics(adminClient).contains(topic);
    }


    public static void validateKafkaConnection(Properties filterProperties)
    {
        if (filterProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        {
            var properties = getAdminClientProperties(filterProperties);
            try( AdminClient adminClient = AdminClient.create(properties) )
            {
                var result = adminClient.describeCluster().nodes().get();
                if (result == null || result.isEmpty()) {
                    throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
            } catch (ExecutionException e){
                throw new IllegalArgumentException("Could not connect to Kafka bootstrap servers " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), e);
            }
        }
    }

    private static Properties getAdminClientProperties(Properties properties)
    {
        var adminProperties = new Properties();
        adminProperties.putAll(properties);
        adminProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        adminProperties.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 5000);
        adminProperties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        return adminProperties;
    }

    private KafkaPool()
    {
        JexxaContext.registerCleanupHandler(this::cleanup);
    }


}
