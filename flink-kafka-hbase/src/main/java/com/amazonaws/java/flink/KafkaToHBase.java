package com.amazonaws.java.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink application that consumes JSON data from Kafka and writes to HBase
 * using Flink SQL with the HBase connector.
 *
 * <h2>Architecture</h2>
 * <pre>
 *   Kafka Topic (JSON / Debezium-JSON)
 *       │
 *       ▼
 *   Flink SQL (Kafka Source Table)
 *       │
 *       ▼
 *   Flink SQL (HBase Sink Table)
 *       │
 *       ▼
 *   HBase (on Amazon EMR 7.2)
 * </pre>
 *
 * <h2>Parameters (via FlinkApplicationProperties or command-line args)</h2>
 * <ul>
 *   <li>{@code kafka.bootstrap.servers} — Kafka bootstrap servers</li>
 *   <li>{@code kafka.topic} — Kafka topic name</li>
 *   <li>{@code kafka.group.id} — Consumer group ID (default: flink-kafka-hbase-group)</li>
 *   <li>{@code kafka.format} — Message format: json or debezium-json (default: json)</li>
 *   <li>{@code kafka.startup.mode} — Startup mode: earliest-offset, latest-offset, group-offsets (default: earliest-offset)</li>
 *   <li>{@code hbase.table} — HBase table name (default: default:user_order)</li>
 *   <li>{@code hbase.zookeeper.quorum} — ZooKeeper quorum for HBase</li>
 *   <li>{@code hbase.zookeeper.znode} — ZooKeeper znode parent (default: /hbase)</li>
 * </ul>
 *
 * <h2>HBase Table Structure</h2>
 * <p>Row Key: {@code uuid_timestamp} (composite: uuid + '_' + epoch millis)</p>
 * <ul>
 *   <li>Column Family {@code info}: user_name, phone_number, ts</li>
 *   <li>Column Family {@code product}: product_id, product_name, product_type, manufacturing_date, price, unit</li>
 * </ul>
 *
 * <p>Pre-create the HBase table:</p>
 * <pre>
 *   hbase shell
 *   create 'default:user_order', 'info', 'product'
 * </pre>
 */
public class KafkaToHBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToHBase.class);

    /**
     * Load application parameters from KDA runtime properties (Managed Flink)
     * or fall back to command-line arguments (EMR Flink / local).
     */
    private static ParameterTool loadApplicationParameters(
            String[] args, StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else {
            // Try to load from KDA runtime properties first (for Managed Flink)
            try {
                Map<String, Properties> applicationProperties =
                        KinesisAnalyticsRuntime.getApplicationProperties();
                Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
                if (flinkProperties != null) {
                    Map<String, String> map = new HashMap<>(flinkProperties.size());
                    flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
                    return ParameterTool.fromMap(map);
                }
            } catch (Exception e) {
                LOG.info("Unable to load from KDA runtime properties, trying command line args: {}",
                        e.getMessage());
            }

            // Fallback to command line arguments (for EMR Flink)
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = loadApplicationParameters(args, env);

        LOG.info("Starting Kafka to HBase pipeline");

        HBaseSink.createAndDeployJob(env, params);
    }

    public static class HBaseSink {

        public static void createAndDeployJob(
                StreamExecutionEnvironment env, ParameterTool params) {
            try {
                StreamTableEnvironment tableEnv =
                        StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

                // Checkpoint configuration
                Configuration configuration = tableEnv.getConfig().getConfiguration();
                String checkpointInterval = params.get("checkpoint.interval", "1 min");
                configuration.setString("execution.checkpointing.interval", checkpointInterval);

                // Kafka parameters
                String kafkaBootstrapServers = params.get("kafka.bootstrap.servers", "localhost:9092");
                String kafkaTopic = params.get("kafka.topic", "kafka_topic");
                String kafkaGroupId = params.get("kafka.group.id", "flink-kafka-hbase-group");
                String kafkaFormat = params.get("kafka.format", "json");
                String kafkaStartupMode = params.get("kafka.startup.mode", "earliest-offset");

                // HBase parameters
                String hbaseTable = params.get("hbase.table", "default:user_order");
                String hbaseZkQuorum = params.get("hbase.zookeeper.quorum", "localhost:2181");
                String hbaseZkZnode = params.get("hbase.zookeeper.znode", "/hbase");

                LOG.info("Kafka Source — Servers: {}, Topic: {}, Format: {}, StartupMode: {}",
                        kafkaBootstrapServers, kafkaTopic, kafkaFormat, kafkaStartupMode);
                LOG.info("HBase Sink — Table: {}, ZK Quorum: {}, ZK Znode: {}",
                        hbaseTable, hbaseZkQuorum, hbaseZkZnode);

                // ============================================================
                // 1. Create Kafka Source Table
                // ============================================================
                // Schema: 30 fields from datafaker-generated Kafka messages
                String createKafkaSource = String.format(
                        "CREATE TABLE kafka_source (\n"
                        + "  uuid STRING,\n"
                        + "  customerId BIGINT,\n"
                        + "  transactionAmount BIGINT,\n"
                        + "  sourceIp STRING,\n"
                        + "  `status` STRING,\n"
                        + "  transactionTime STRING,\n"
                        + "  user_name STRING,\n"
                        + "  phone_number INT,\n"
                        + "  product_id INT,\n"
                        + "  product_name STRING,\n"
                        + "  product_type STRING,\n"
                        + "  manufacturing_date INT,\n"
                        + "  price DECIMAL(6,2),\n"
                        + "  unit INT,\n"
                        + "  email STRING,\n"
                        + "  address STRING,\n"
                        + "  city STRING,\n"
                        + "  country STRING,\n"
                        + "  ip_address STRING,\n"
                        + "  website STRING,\n"
                        + "  company_name STRING,\n"
                        + "  department STRING,\n"
                        + "  salary DECIMAL(8,2),\n"
                        + "  age INT,\n"
                        + "  gender STRING,\n"
                        + "  created_date STRING,\n"
                        + "  last_login STRING,\n"
                        + "  score INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = '%s',\n"
                        + "  'format' = '%s'\n"
                        + ")",
                        kafkaTopic, kafkaBootstrapServers, kafkaGroupId,
                        kafkaStartupMode, kafkaFormat);

                LOG.info("Creating Kafka source table");
                tableEnv.executeSql(createKafkaSource);

                // ============================================================
                // 2. Create HBase Sink Table
                // ============================================================
                // RowKey: uuid (globally unique per record)
                // Column families organized by business domain:
                //   info        — user profile
                //   transaction — payment/transaction details
                //   product     — product details
                //   work        — employment info
                String createHBaseSink = String.format(
                        "CREATE TABLE hbase_sink (\n"
                        + "  rowkey STRING,\n"
                        + "  info ROW<user_name STRING, phone_number INT, email STRING, "
                        + "address STRING, city STRING, country STRING, ip_address STRING, "
                        + "website STRING, age INT, gender STRING, "
                        + "created_date STRING, last_login STRING, score INT>,\n"
                        + "  transaction ROW<customerId BIGINT, transactionAmount BIGINT, "
                        + "sourceIp STRING, `status` STRING, transactionTime STRING>,\n"
                        + "  product ROW<product_id INT, product_name STRING, product_type STRING, "
                        + "manufacturing_date INT, price DECIMAL(6,2), unit INT>,\n"
                        + "  work ROW<company_name STRING, department STRING, salary DECIMAL(8,2)>\n"
                        + ") WITH (\n"
                        + "  'connector' = 'hbase-2.2',\n"
                        + "  'table-name' = '%s',\n"
                        + "  'zookeeper.quorum' = '%s',\n"
                        + "  'zookeeper.znode.parent' = '%s'\n"
                        + ")",
                        hbaseTable, hbaseZkQuorum, hbaseZkZnode);

                LOG.info("Creating HBase sink table");
                tableEnv.executeSql(createHBaseSink);

                // ============================================================
                // 3. Execute INSERT: Kafka → HBase
                // ============================================================
                // RowKey: uuid (unique per record)
                String insertSql =
                        "INSERT INTO hbase_sink\n"
                        + "SELECT\n"
                        + "  uuid AS rowkey,\n"
                        + "  ROW(user_name, phone_number, email, address, city, country, "
                        + "ip_address, website, age, gender, "
                        + "created_date, last_login, score) AS info,\n"
                        + "  ROW(customerId, transactionAmount, sourceIp, `status`, transactionTime) AS transaction,\n"
                        + "  ROW(product_id, product_name, product_type, "
                        + "manufacturing_date, price, unit) AS product,\n"
                        + "  ROW(company_name, department, salary) AS work\n"
                        + "FROM kafka_source";

                LOG.info("Starting INSERT pipeline: Kafka -> HBase");
                tableEnv.executeSql(insertSql);

                LOG.info("Pipeline job submitted successfully");

            } catch (Exception e) {
                LOG.error("Error creating and deploying Kafka to HBase pipeline", e);
                throw new RuntimeException("Failed to create and deploy pipeline", e);
            }
        }
    }
}
