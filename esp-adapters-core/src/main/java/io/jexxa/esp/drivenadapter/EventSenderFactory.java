package io.jexxa.esp.drivenadapter;

import io.jexxa.common.drivenadapter.messaging.MessageSenderFactory;
import io.jexxa.common.facade.factory.ClassFactory;
import io.jexxa.common.facade.logger.ApplicationBanner;
import io.jexxa.common.facade.utils.annotation.CheckReturnValue;
import io.jexxa.esp.drivenadapter.kafka.KafkaSender;
import io.jexxa.esp.drivenadapter.logging.EventLogger;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.jexxa.common.facade.jms.JMSProperties.jmsStrategy;
import static io.jexxa.common.facade.logger.SLF4jLogger.getLogger;
import static io.jexxa.common.facade.utils.properties.PropertiesPrefix.globalPrefix;
import static io.jexxa.esp.drivenadapter.EventProperties.eventStrategy;


@SuppressWarnings("java:S6548")
public final class EventSenderFactory
{
    private static final EventSenderFactory EVENT_SENDER_FACTORY = new EventSenderFactory();

    private static final Map<Class<?> , Class<? extends EventSender>> STRATEGY_MAP = new HashMap<>();
    private static Class<? extends EventSender> defaultEventSender = null;

    @SuppressWarnings("unused")
    public static Class<?> getDefaultDefaultSender(Properties properties)
    {
        return createEventSender(null, properties).getClass();
    }

    @SuppressWarnings("unused")
    public static <U extends EventSender, T > void setEventSender(Class<U> eventSender, Class<T> aggregateType)
    {
        STRATEGY_MAP.put(aggregateType, eventSender);
    }

    public static void setDefaultEventSender(Class<? extends EventSender> defaultEventSender)
    {
        Objects.requireNonNull(defaultEventSender);

        EventSenderFactory.defaultEventSender = defaultEventSender;
    }


    public static <T>  EventSender createEventSender(Class<T> sendingClass, Properties properties)
    {
        try
        {
            var strategy = EVENT_SENDER_FACTORY.eventSenderType(sendingClass, properties);

            var result = ClassFactory.newInstanceOf(strategy, new Object[]{properties});
            if (result.isEmpty()) //Try a factory method with properties
            {
                result = ClassFactory.newInstanceOf(EventSender.class, strategy,new Object[]{properties});
            }
            if (result.isEmpty()) //Try default constructor
            {
                result = ClassFactory.newInstanceOf(strategy);
            }
            if (result.isEmpty()) //Try a factory method without properties
            {
                result = ClassFactory.newInstanceOf(EventSender.class, strategy);
            }

            return result.orElseThrow();
        }
        catch (ReflectiveOperationException e)
        {
            if ( e.getCause() != null)
            {
                throw new IllegalArgumentException(e.getCause().getMessage(), e);
            }

            throw new IllegalArgumentException("No suitable default MessageSender available", e);
        }
    }

    public void bannerInformation(Properties properties)
    {
        getLogger(ApplicationBanner.class).info("Used Message Sender Strategie  : [{}]", EVENT_SENDER_FACTORY.eventSenderType(null, properties).getSimpleName());
    }

    private EventSenderFactory()
    {
        ApplicationBanner.addConfigBanner(this::bannerInformation);
    }

    @CheckReturnValue
    @SuppressWarnings("unchecked")
    private <T> Class<? extends EventSender> eventSenderType(Class<T> aggregateClazz, Properties properties)
    {
        // 1. Check if a dedicated strategy is registered for aggregateClazz
        var result = STRATEGY_MAP
                .entrySet()
                .stream()
                .filter( element -> element.getKey().equals(aggregateClazz))
                .filter( element -> element.getValue() != null )
                .findFirst();

        if (result.isPresent())
        {
            return result.get().getValue();
        }

        // 2. If a default strategy is available, return this one
        if (defaultEventSender != null)
        {
            return defaultEventSender;
        }

        // 3. Check explicit configuration
        if (properties.containsKey(eventStrategy()))
        {
            try {
                return (Class<? extends EventSender>) Class.forName(properties.getProperty(eventStrategy()));
            } catch (ClassNotFoundException e)
            {
                getLogger(MessageSenderFactory.class).warn("Unknown or invalid message sender {} -> Ignore setting", properties.getProperty(jmsStrategy()));
                getLogger(MessageSenderFactory.class).warn(String.valueOf(e));
            }
        }

        if (properties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) ||
                properties.containsKey(globalPrefix() + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
        {
            return KafkaSender.class;
        }

        // 4. In all other cases (including simulation mode) return a MessageLogger
        return EventLogger.class;
    }



}
