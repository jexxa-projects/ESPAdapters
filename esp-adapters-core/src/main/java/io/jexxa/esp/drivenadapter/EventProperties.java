package io.jexxa.esp.drivenadapter;

import io.jexxa.common.facade.utils.properties.PropertiesPrefix;

public class EventProperties {
    public static String eventStrategy() { return PropertiesPrefix.globalPrefix() + "event.strategy"; }
    private EventProperties() {
        
    }
}
