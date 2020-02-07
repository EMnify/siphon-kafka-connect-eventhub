package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

public class EventHubSinkConfig extends AbstractConfig {
    /**
     * The connection string for the Event Hub. This can be retrieved from the
     * Azure Portal -> Event Hubs -> Your Event Hub -> Overview -> Connection Strings
     */
    public static final String CONNECTION_STRING = "eventhub.connection.string";

    /**
     * The number of Azure EventHubClient objects to use per task.
     * Each client will create its own TCP connection, which helps with gaining more throughput
     */
    public static final String CLIENTS_PER_TASK = "eventhub.clients.per.task";

    /**
     * EventHub serialization type for structured events with schema. By default all structured
     * events are converted to JSON.
     */
    public static final String SERIALIZATION_TYPE = "eventhub.serialization";

    /**
     * Send all structured events to EventHub as JSON
     */
    public static final String SERIALIZATION_JSON = "json";

    private static final short defaultClientsPerTask = 1;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_STRING, Type.STRING, Importance.HIGH,
                    "EventHub Connection String")
            .define(CLIENTS_PER_TASK, Type.SHORT, defaultClientsPerTask, Importance.HIGH,
                    "Number of Event Hub clients to use per task")
            .define(SERIALIZATION_TYPE, Type.STRING, SERIALIZATION_JSON, Importance.LOW,
                    "Method of serialization of structured events with schema to EventHub");

    public EventHubSinkConfig(Map<String, String> configValues) {
        super(CONFIG_DEF, configValues);
    }
}
