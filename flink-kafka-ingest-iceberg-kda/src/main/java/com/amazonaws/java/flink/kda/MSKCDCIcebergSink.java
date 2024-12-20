package com.amazonaws.java.flink.kda;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MSKCDCIcebergSink {

        private static String _topics = "topic01";
        private static String _kafkaBootstrapServers = "b-1.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092,b-2.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092,b-3.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092";
        // private static String _s3SinkPath = "s3a://ka-app-code-<username>/data";

        private static String _warehousePath = "s3a://ka-app-code-<username>/warehouse";

        private static final Logger LOG = LoggerFactory.getLogger(MSKCDCIcebergSink.class);

        private static int _Parallelism = 3;

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

        public static void main(String[] args) throws Exception {
                // set up the streaming execution environment

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                ParameterTool applicationProperties = loadApplicationParameters(args, env);
                applicationProperties = applicationProperties.mergeWith(ParameterTool.fromSystemProperties());


                _kafkaBootstrapServers = applicationProperties.get("bootstrap.servers");
                _topics = applicationProperties.get("topics");
                _warehousePath = applicationProperties.get("s3.path");
                _Parallelism = Integer.parseInt(applicationProperties.get("parallelism","1"));

                IcebergSink.createAndDeployJob(env);

        }

        public static class IcebergSink {

                public static void createAndDeployJob(StreamExecutionEnvironment env) {
                        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                                        env, EnvironmentSettings.newInstance().build());

                        Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();

                        configuration.setString("execution.checkpointing.interval", "1 min");
                        env.setParallelism(_Parallelism);

                        final String icebergCatalog = String.format("CREATE CATALOG glue_catalog WITH ( \n" +
                                        "'type'='iceberg', \n" +
                                        "'warehouse'='%s', \n" +
                                        "'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', \n" +
                                        "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO');", _warehousePath);

                        LOG.info(icebergCatalog);
                        streamTableEnvironment.executeSql(icebergCatalog);

                        // Source
                        final String sourceKafkaSQL = String.format("CREATE TABLE kafka_source_table (\n" +
                                        "id INT,\n" +
                                        "uuid STRING,\n" +
                                        "user_name STRING,\n" +
                                        "phone_number BIGINT,\n" +
                                        "product_id STRING,\n" +
                                        "product_name STRING,\n" +
                                        "product_type STRING,\n" +
                                        "manufacturing_date INT,\n" +
                                        "price FLOAT,\n" +
                                        "unit INT,\n" +
                                        "created_at STRING,\n" +
                                        "updated_at STRING,\n" +
                                        "PRIMARY KEY (id) NOT ENFORCED\n" +
                                        ") with (\n" +
                                        "'connector' = 'kafka',\n" +
                                        "'topic' = '%s',\n" +
                                        "'properties.bootstrap.servers' = '%s',\n" +
                                        "'scan.startup.mode' = 'earliest-offset', \n" +
                                        "'properties.group.id' = 'kafka_user_order_list_iceberg_01',\n" +
                                        "'format' = 'debezium-json'\n" +
                                        ");", _topics, _kafkaBootstrapServers);
                        LOG.info(sourceKafkaSQL);
                        streamTableEnvironment.executeSql(sourceKafkaSQL);

                        final String s3SinkSql = String
                                        .format("CREATE TABLE IF NOT EXISTS glue_catalog.icebergdb.user_order_list_flink_iceberg (\n"
                                                        +
                                                        "id INT,\n" +
                                                        "uuid STRING,\n" +
                                                        "user_name STRING,\n" +
                                                        "phone_number BIGINT,\n" +
                                                        "product_id STRING,\n" +
                                                        "product_name STRING,\n" +
                                                        "product_type STRING,\n" +
                                                        "manufacturing_date INT,\n" +
                                                        "price FLOAT,\n" +
                                                        "unit INT,\n" +
                                                        "created_at STRING,\n" +
                                                        "updated_at STRING\n" +
                                                        ") WITH (\n" +
                                                        "'type'='iceberg', \n" +
                                                        "'catalog-name'='glue_catalog', \n" +
                                                        "'write.metadata.delete-after-commit.enabled'='true', \n" +
                                                        "'write.metadata.previous-versions-max'='5', \n" +
                                                        "'format-version'='2');", _warehousePath);

                        streamTableEnvironment.executeSql(s3SinkSql);

                        final String Insert_Iceberg = "INSERT INTO  " +
                                        "glue_catalog.icebergdb.user_order_list_flink_iceberg " +
                                        "SELECT * FROM kafka_source_table;";

                        streamTableEnvironment.executeSql(Insert_Iceberg);

                }
        }
}
