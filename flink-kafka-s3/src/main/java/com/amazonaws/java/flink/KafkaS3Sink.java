package com.amazonaws.java.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;  // Add this import
import java.util.Properties;
import java.util.UUID;

public class KafkaS3Sink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaS3Sink.class);

    public static class SerializableSchema implements Serializable {
        private final String schemaString;
        private transient Schema schema;

        public SerializableSchema(String schemaString) {
            this.schemaString = schemaString;
        }

        public Schema getSchema() {
            if (schema == null) {
                schema = new Schema.Parser().parse(schemaString);
            }
            return schema;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool applicationProperties = ParameterTool.fromArgs(args);

        env.enableCheckpointing(60000);

        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        org.apache.flink.configuration.Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
        configuration.setString("execution.checkpointing.interval", "1 min");

        //            String aos_host =
        // "https://vpc-vector-db-demo-tzkscvrlfcdid4ykyrkanxgmri.us-east-1.es.amazonaws.com";

        String kafka_bootstrap_servers = applicationProperties.get("kafka_bootstrap_servers");
        String s3Path = applicationProperties.get("s3.path");

        // ingest data from kafka to s3 with flinksql and used glue data catalog
        final String kafka_topic = applicationProperties.get("topic");


        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafka_bootstrap_servers);
        kafkaProps.setProperty("group.id", "flink-workshop-group-test-tb2");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                kafka_topic,
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Define Avro schema
        String avroSchemaString = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"DataRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"before\", \"type\": \"string\"}," +
                "  {\"name\": \"after\", \"type\": \"string\"}," +
                "  {\"name\": \"source\", \"type\": \"string\"}," +
                "  {\"name\": \"op\", \"type\": \"string\"}," +
                "  {\"name\": \"ts_ms\", \"type\": \"long\"}," +
                "  {\"name\": \"transaction\", \"type\": \"string\"}" +
                "]}";

        SerializableSchema serializableSchema = new SerializableSchema(avroSchemaString);

//        Schema avroSchema = new Schema.Parser().parse(serializableSchema);

        // Transform and write to S3
        kafkaStream.process(new ParquetWriterFunction(serializableSchema, s3Path));

        env.execute("Kafka to S3 Parquet Job");
    }

    private static class ParquetWriterFunction extends ProcessFunction<String, Void> {
        private final SerializableSchema serializableSchema;
        private transient ObjectMapper jsonParser;

        private transient ParquetWriter<GenericRecord> writer;
        private transient long recordCount;
        private static final long MAX_RECORDS_PER_FILE = 2000; // Adjust based on your needs
        private String s3Path;

        public ParquetWriterFunction(SerializableSchema serializableSchema, String s3path) {
            this.serializableSchema = serializableSchema;

            this.s3Path = s3path;
        }


        @SuppressWarnings("unused")
        public void open(Configuration parameters) {
            jsonParser = new ObjectMapper();
            recordCount = 0;
            createNewWriter();
        }

        private void createNewWriter() {
            try {
                String fileName = String.format("data_%s.parquet", UUID.randomUUID());
                Path outputPath = new Path(s3Path + fileName);

                // Configure Hadoop to use S3
//                org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
//                hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

                // Create Parquet writer
                writer = AvroParquetWriter
                        .<GenericRecord>builder(outputPath)
                        .withSchema(serializableSchema.getSchema())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
//                        .withConf(hadoopConfig)
                        .withPageSize(4 * 1024 * 1024)      // 4MB page size
                        .withRowGroupSize(128 * 1024 * 1024) // 128MB row group size
                        .build();

            } catch (Exception e) {
                LOG.error("Error creating Parquet writer", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
            try {
                JsonNode jsonNode = jsonParser.readTree(value);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(serializableSchema.getSchema());

                // Map JSON fields to Avro record
                recordBuilder.set("before", jsonNode.get("before").asText());
                recordBuilder.set("after", jsonNode.get("after").asText());
                recordBuilder.set("source", jsonNode.get("source").asText());
                recordBuilder.set("op", jsonNode.get("op").asText());
                recordBuilder.set("ts_ms", jsonNode.get("ts_ms").asLong());
                recordBuilder.set("transaction", jsonNode.get("transaction").asText());

                writer.write(recordBuilder.build());
                recordCount++;

                // Create new file after MAX_RECORDS_PER_FILE
                if (recordCount >= MAX_RECORDS_PER_FILE) {
                    writer.close();
                    createNewWriter();
                    recordCount = 0;
                }
            } catch (Exception e) {
                LOG.error("Error processing record: " + value, e);
            }
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.close();
            }
        }
    }

}
