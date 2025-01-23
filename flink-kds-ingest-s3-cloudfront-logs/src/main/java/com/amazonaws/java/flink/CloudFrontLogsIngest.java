package com.amazonaws.java.flink;

import java.io.IOException;
import java.nio.file.ClosedDirectoryStreamException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.java.flink.common.CloudFrontRecord;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.Base64;


public class CloudFrontLogsIngest {
    private static final Logger LOG = LoggerFactory.getLogger(CloudFrontLogsIngest.class);

    private static final String DEFAULT_KINESIS_STREAM = "ev_station_data";

    private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {

        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else if (env instanceof StreamExecutionEnvironment) {
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

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
        configuration.setString("execution.checkpointing.interval", "1 min");
        //get parameters from args via ParameterTool
        ParameterTool applicationProperties = loadApplicationParameters(args, env);
        applicationProperties = applicationProperties.mergeWith(ParameterTool.fromSystemProperties());

        //Process stream
        CloudFrontLogs.createAndDeployJob(env, applicationProperties);
    }


    // Custom MapFunction to decode base64 data
    public static class Base64DecodeFunction implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            try {
                byte[] decodedBytes = Base64.getDecoder().decode(value);
                return new String(decodedBytes);
            } catch (IllegalArgumentException e) {
                // Log the error and return the original value
                LOG.error("Invalid Base64 input: " + value);
                return value;
            }
        }
    }

    public static class CloudFrontLogs {

        public static void createAndDeployJob(StreamExecutionEnvironment env, ParameterTool applicationProperties) throws Exception {
            StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().build());




            String kinesisStreamName = applicationProperties.get("kinesis.stream.name", DEFAULT_KINESIS_STREAM);
            String awsRegion = applicationProperties.get("aws.region",DEFAULT_KINESIS_STREAM);
            String s3Path = applicationProperties.get("s3.path",DEFAULT_KINESIS_STREAM);

            // Set up Kinesis consumer properties
            Properties kinesisConsumerConfig = new Properties();
            kinesisConsumerConfig.put(AWSConfigConstants.AWS_REGION, awsRegion);
            kinesisConsumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");



            Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
            configuration.setString("execution.checkpointing.interval", "1 min");

            FlinkKinesisConsumer<String> kinesisConsumer = new FlinkKinesisConsumer<>(
                    kinesisStreamName, new SimpleStringSchema(), kinesisConsumerConfig);


            // Add Kinesis source to the execution environment
            DataStream<String> kinesisStream = env.addSource(kinesisConsumer);

            // Apply base64 decoding
            DataStream<String> decodedStream = kinesisStream.map(new Base64DecodeFunction());

            ObjectMapper jsonParser = new ObjectMapper();

            decodedStream.map(value -> { // Parse the JSON
                //98.83.115.86\u0009GET\u0009https\u0009d3rxko7474b7sx.cloudfront.net\u0009/index_files/a.js\u000936\u0009HTTP/2.0\u0009IPv4\u0009Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010.15;%20rv:133.0)%20Gecko/20100101%20Firefox/133.0\u0009https://d3rxko7474b7sx.cloudfront.net/index.html\u0009-\u0009-\u0009-\u0009-\u000953094\u0009US\u0009gzip,%20deflate,%20br,%20zstd\u0009*/*\u0009*\u0009host:d3rxko7474b7sx.cloudfront.net%0Auser-agent:Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010.15;%20rv:133.0)%20Gecko/20100101%20Firefox/133.0%0Aaccept:*/*%0Aaccept-language:en-US,en;q=0.5%0Aaccept-e
                //split by \u0009
                    String[] cols = value.split("\u0009");
                    CloudFrontRecord record = new CloudFrontRecord();
                    record.setTimestamp(Float.parseFloat(cols[0].equals("-") ? "0" : cols[0]));
                    record.setCip(cols[1]);
                    record.setSip(cols[2]);
                    record.setTimeToFirstByte(Float.parseFloat(cols[3].equals("-") ? "0" : cols[3]));
                    record.setScStatus(Integer.parseInt(cols[4].equals("-") ? "0" : cols[4]));
                    record.setScBytes(Integer.parseInt(cols[5].equals("-") ? "0" : cols[5]));
                    record.setCsMethod(cols[6]);
                    record.setCsProto(cols[7]);
                    record.setCsHost(cols[8]);
                    record.setCsUrl(cols[9]);
                    record.setCsBytes(Integer.parseInt(cols[10].equals("-") ? "0" : cols[10]));
                    record.setXEdgeLocation(cols[11]);
                    record.setXEdgeRequestId(cols[12]);
                    record.setXHostHeader(cols[13]);
                    record.setTimeTaken(Float.parseFloat(cols[14].equals("-") ? "0" : cols[14]));
                    record.setCsVersion(cols[15]);
                    record.setCipVersion(cols[16]);
                    record.setCsUserAgent(cols[17]);
                    record.setCsRefer(cols[18]);
                    record.setCsCookie(cols[19]);
                    record.setCsUriQuery(cols[20]);
                    record.setXEdgeResponseResultType(cols[21]);
                    record.setXForwardedFor(cols[22]);
                    record.setSslProtocol(cols[23]);
                    record.setSslCipher(cols[24]);
                    record.setXEdgeResultType(cols[25]);
                    record.setFleEncryptedFields(cols[26]);
                    record.setFleStatus(cols[27]);
                    record.setScContentType(cols[28]);
                    record.setScContentLen(Integer.parseInt(cols[29].equals("-") ? "0" : cols[29]));
                    record.setScRangeStart(Integer.parseInt(cols[30].equals("-") ? "0" : cols[30]));
                    record.setScRangeEnd(Integer.parseInt(cols[31].equals("-") ? "0" : cols[31]));
                    record.setCPort(Integer.parseInt(cols[32].equals("-") ? "0" : cols[32]));
                    record.setXEdgeDetailedResultType(cols[33]);
                    record.setCipCountry(cols[34]);
                    record.setCsAcceptEncoding(cols[35]);
                    record.setCsAccept(cols[36]);
                    record.setCacheBehaviorPathPattern(cols[37]);
                    record.setCsHeaders(cols[38]);
                    record.setCsHeaderNames(cols[39]);
                    record.setCsHeadersCount(Integer.parseInt(cols[40].equals("-") ? "0" : cols[40]));
                    return record;
            })
                    .addSink(createS3SinkFromStaticConfig(s3Path))
                    .name("S3 Parquet Sink");

            env.execute("Cloudfront Logs Ingestion Job(Paruqet)");

        }

        private static StreamingFileSink<CloudFrontRecord> createS3SinkFromStaticConfig(String s3SinkPath) {
            return StreamingFileSink
                    .forBulkFormat(new Path(s3SinkPath), ParquetAvroWriters.forReflectRecord(CloudFrontRecord.class))
                    // Use hive style partitioning
                    .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH/"))
                    .withOutputFileConfig(OutputFileConfig.builder()
                            .withPartSuffix(".parquet")
                            .build())
                    .build();
        }
    }

}
