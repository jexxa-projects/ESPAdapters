package io.jexxa.esp.plugins.eventstream;

import io.jexxa.adapterapi.JexxaContext;
import io.jexxa.esp.drivenadapter.EventSender;
import io.jexxa.jlegmed.core.FailFastException;
import io.jexxa.jlegmed.core.filter.FilterContext;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.jexxa.esp.drivenadapter.EventSenderFactory.createEventSender;

@SuppressWarnings("java:S6548")
public class EventSenderPool {
    private static final EventSenderPool INSTANCE = new EventSenderPool();

    private final Map<FilterContext, EventSender> messageSenderMap = new ConcurrentHashMap<>();

    public static EventSender eventSender(FilterContext filterContext)
    {
        return INSTANCE.internalEventSender(filterContext);
    }


    private void initConnections(Properties properties)
    {
        try {
            if (properties.containsKey("bootstrap.servers"))
            {
                createEventSender(EventSenderPool.class, properties);
            }
        } catch ( RuntimeException e) {
            throw new FailFastException("Could not init JMS connection for filter properties "
                    + ". Reason: " + e.getMessage(), e );
        }

    }

    private EventSender internalEventSender(FilterContext filterContext)
    {
        return INSTANCE.messageSenderMap.computeIfAbsent(filterContext,
                key ->  createEventSender(EventSenderPool.class, filterContext.properties()));

    }


    private EventSenderPool()
    {
        JexxaContext.registerCleanupHandler(messageSenderMap::clear);
        JexxaContext.registerValidationHandler(this::initConnections);
    }
}
