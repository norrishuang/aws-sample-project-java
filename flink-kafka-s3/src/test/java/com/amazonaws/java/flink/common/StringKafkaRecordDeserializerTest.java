package com.amazonaws.java.flink.common;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringKafkaRecordDeserializerTest {

    @Test
    void deserializeShouldCollectUtf8StringValue() throws Exception {
        StringKafkaRecordDeserializer deserializer = new StringKafkaRecordDeserializer();
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "hello world".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 0, 0L, key, value);

        List<String> collected = new ArrayList<>();
        Collector<String> collector = new Collector<String>() {
            @Override
            public void collect(String record) {
                collected.add(record);
            }

            @Override
            public void close() {
                // no-op
            }
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collected.size());
        assertEquals("hello world", collected.get(0));
    }

    @Test
    void getProducedTypeShouldReturnStringTypeInformation() {
        StringKafkaRecordDeserializer deserializer = new StringKafkaRecordDeserializer();

        assertEquals(String.class, deserializer.getProducedType().getTypeClass());
    }
}
