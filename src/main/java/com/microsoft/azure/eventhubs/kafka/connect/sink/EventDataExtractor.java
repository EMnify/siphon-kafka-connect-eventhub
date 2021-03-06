package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.EventData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class EventDataExtractor {

    private OutputJsonFormatter valueConvertor = new OutputJsonFormatter();

    public EventDataExtractor() {
    }

    private EventData structToEventData(String topic, Schema schema, Struct struct) {
        try {
            byte[] bytes = valueConvertor.fromConnectData(topic, schema, struct);
            return EventData.create(bytes);
        } catch (Exception ex) {
            throw new ConnectException("Unable to process record="+ struct.toString(), ex);
        }
    }

    public EventData extractEventData(SinkRecord record) {
        EventData eventData = null;
        if (record.value() instanceof byte[]) {
            eventData = EventData.create((byte[]) record.value());
        } else if (record.value() instanceof EventData) {
            eventData = (EventData) record.value();
        } else if (record.value() instanceof Struct) {
            eventData = structToEventData(record.topic(), record.valueSchema(), (Struct)record.value());
        } else if (record.value() != null) {
            throw new ConnectException("Data format is unsupported for EventHubSinkType [class=" + record.value().getClass().getCanonicalName()+"]");
        }
        return eventData;
    }

    public void configureJson(Map<String, ?> jsonConfigMap) {
        valueConvertor.configure(jsonConfigMap);
    }
}
