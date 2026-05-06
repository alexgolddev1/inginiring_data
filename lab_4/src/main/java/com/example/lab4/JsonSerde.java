package com.example.lab4;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper objectMapper;
    private final Class<T> targetClass;

    public JsonSerde(Class<T> targetClass) {
        this.targetClass = targetClass;
        this.objectMapper = new ObjectMapper()
                .findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception exception) {
                throw new RuntimeException("Failed to serialize value for topic " + topic, exception);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null || data.length == 0) {
                return null;
            }

            try {
                return objectMapper.readValue(data, targetClass);
            } catch (Exception exception) {
                throw new RuntimeException("Failed to deserialize value for topic " + topic, exception);
            }
        };
    }
}
