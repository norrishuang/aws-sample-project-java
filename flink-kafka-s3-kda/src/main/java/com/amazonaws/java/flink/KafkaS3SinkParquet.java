// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

package com.amazonaws.java.flink;

import com.amazonaws.java.flink.common.CDCRecords;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.metrics2.sink.FileSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.SslConfigs;

/**
 * Consumer data from MSK(via TLS) to S3
 */
public class KafkaS3SinkParquet {

    private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties properties from runtime properties");
            }
            Map<String, String> map = new HashMap<>(flinkProperties.size());
            flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
            return ParameterTool.fromMap(map);
        }
    }


//    private static StreamingFileSink<CDCRecords> createS3SinkFromStaticConfig(String s3SinkPath) {
//
//
//        return StreamingFileSink
//                .forBulkFormat(new Path(s3SinkPath), ParquetAvroWriters.forReflectRecord(CDCRecords.class))
//                // Use hive style partitioning
//                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH/"))
//                .withOutputFileConfig(OutputFileConfig.builder()
//                        .withPartSuffix(".parquet")
//                        .build())
//                .build();
//    }

    private static StreamingFileSink<CDCRecords> createS3SinkFromStaticConfig(
            String s3SinkPath
    ) {
        return StreamingFileSink
                .forBulkFormat(new Path(s3SinkPath), AvroParquetWriters.forReflectRecord(CDCRecords.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH/"))
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".parquet")
                        .build())
                .build();
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
        configuration.setString("execution.checkpointing.interval", "1 min");
        //get parameters from args via ParameterTool
        ParameterTool applicationProperties = loadApplicationParameters(args, env);
        applicationProperties = applicationProperties.mergeWith(ParameterTool.fromSystemProperties());


        String kafka_bootstrap_servers = applicationProperties.get("kafka_bootstrap_servers");
        String s3Path = applicationProperties.get("s3.path");
        String gruopid = applicationProperties.get("group.id","flink-workshop-group-test-tb1");
        // ingest data from kafka to s3 with flinksql and used glue data catalog
        final String kafka_topic = applicationProperties.get("topic");

        Properties properties = new Properties();
        properties.put("group.id", gruopid);
        properties.put("bootstrap.servers", kafka_bootstrap_servers);


        boolean isSSL = Boolean.parseBoolean(applicationProperties.get("enablessl","false"));
        if (isSSL) {
              // for msk tls
              properties.setProperty("security.protocol", "SSL");

              // TLS/SSL configuration
              properties.setProperty(
                  SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                  applicationProperties.get("ssl.truststore.location", "/tmp/kafka.client.truststore.jks"));
              properties.setProperty(
                  SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                  applicationProperties.get("ssl.keystore.location", "/tmp/kafka.client.keystore.jks"));
              properties.setProperty(
                  SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                  applicationProperties.get("ssl.keystore.password", "amazon123"));
              properties.setProperty(
                  SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                  applicationProperties.get("ssl.key.password", "amazon123"));
        }

        // Flink version 1.17 above
         KafkaSource<String> dataSource = KafkaSource.<String>builder()
                 .setProperties(properties)
                 //从commit开始，没有则从最早
                 .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                 .setValueOnlyDeserializer(new SimpleStringSchema())
                 .setTopics(kafka_topic)
                 .build();

         DataStream<String> dataStreamSource = env.fromSource(dataSource, WatermarkStrategy.noWatermarks(), "Kafka source");

     
        // Create Kafka Consumer (flink 1.12.1)
        // Flink version 1.12.1
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                kafka_topic,                // topic
//                new SimpleStringSchema(),    // deserializer
//                properties                   // properties
//        );
//        consumer.setStartFromLatest();    // start from latest
//        DataStream<String> dataStreamSource = env.addSource(dataSource);



        ObjectMapper jsonParser = new ObjectMapper();

        dataStreamSource.map(value -> { // Parse the JSON
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    CDCRecords records = new CDCRecords(
                            jsonNode.get("before").toString(),
                            jsonNode.get("after").toString(),
                            jsonNode.get("source").toString(),
                            jsonNode.get("op").toString(),
                            jsonNode.get("ts_ms").toString(),
                            jsonNode.get("transaction").toString());

                    return records;
        })
                .addSink(createS3SinkFromStaticConfig(s3Path))
                .name("S3 Parquet Sink");

        env.execute("Flink S3 Streaming Sink Job");
    }
}
