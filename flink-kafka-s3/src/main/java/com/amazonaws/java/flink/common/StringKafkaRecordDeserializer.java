package com.amazonaws.java.flink.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;


//
public class StringKafkaRecordDeserializer implements KafkaRecordDeserializer<String> {


    private static final long serialVersionUID = -7597850905033462872L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<String> collector) throws Exception {
        collector.collect(new String(consumerRecord.value(), StandardCharsets.UTF_8));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}
