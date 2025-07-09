package io.jexxa.commons;

import io.jexxa.jlegmed.core.JLegMed;
import java.util.concurrent.TimeUnit;
import static org.slf4j.LoggerFactory.getLogger;

public final class ESPAdapters
{
    public static void main(String[] args)
    {
        var jLegMed = new JLegMed(ESPAdapters.class);

        jLegMed.newFlowGraph("HelloWorld")
                .every(1, TimeUnit.SECONDS)

                .receive(String.class).from( () -> "Hello " )
                .and().processWith(data -> data + "World" )
                .and().consumeWith(data -> getLogger(ESPAdapters.class).info(data));

        jLegMed.run();
    }
}
