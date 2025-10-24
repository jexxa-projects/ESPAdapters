package io.jexxa.esp.drivenadapter.logging;

import io.jexxa.common.drivenadapter.messaging.logging.MessageLogger;
import io.jexxa.esp.drivenadapter.EventSender;
import org.slf4j.Logger;

import java.util.Properties;

import static io.jexxa.common.facade.logger.SLF4jLogger.getLogger;


@SuppressWarnings("unused")
public class EventLogger extends EventSender
{
    private static final Logger EVENT_LOGGER = getLogger(EventLogger.class);



    @Override
    protected <K, V> void sendAsJSON(K key, V eventData, String topic, Long timestamp, Properties headers) {
        logEvent(key, eventData, topic, timestamp, headers);
    }

    @Override
    protected <K, V> void sendAsAVRO(K key, V eventData, String topic, Long timestamp, Properties headers) {
        logEvent(key, eventData, topic, timestamp, headers);
    }

    @Override
    protected <K, V> void sendAsText(K key, V eventData, String topic, Long timestamp, Properties headers) {
        logEvent(key, eventData, topic, timestamp, headers);
    }

    private <K, V> void logEvent(K key, V eventData, String topic, Long timestamp, Properties headers) {
        EVENT_LOGGER.info("Begin> Send event");
        EVENT_LOGGER.info("Key               : {}", key);
        EVENT_LOGGER.info("Event             : {}", eventData);
        EVENT_LOGGER.info("Properties        : {}", headers);
        EVENT_LOGGER.info("Topic             : {}", topic);
        EVENT_LOGGER.info("End> Send event");
    }
}
