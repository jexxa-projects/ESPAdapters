package io.jexxa.esp.drivenadapter;


import static io.jexxa.common.facade.utils.properties.PropertiesPrefix.globalPrefix;

public class EventProperties {
    public static String eventStrategy() { return globalPrefix() + "event.strategy"; }
    private EventProperties() {

    }
}
