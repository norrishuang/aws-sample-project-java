package com.amazonaws.java.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code kafka_bootstrap_servers} — Kafka bootstrap servers</li>
 *   <li>{@code topic} — Kafka topic name</li>
 *   <li>{@code kafka_group_id} — Consumer group ID (default: flink-kafka-hbase-group)</li>
 *   <li>{@code kafka_format} — Message format: json or debezium-json (default: json)</li>
 *   <li>{@code kafka_startup_mode} — Startup mode: earliest-offset, latest-offset, group-offsets (default: earliest-offset)</li>
 *   <li>{@code hbase_table} — HBase table name (default: default:user_order)</li>
 *   <li>{@code hbase_zookeeper_quorum} — ZooKeeper quorum for HBase (default: localhost:2181)</li>
 *   <li>{@code hbase_zookeeper_znode} — ZooKeeper znode parent (default: /hbase)</li>
 * </ul>
 *
 * <h2>HBase Table Structure</h2>
 * <p>The HBase sink table uses the following column family mapping:</p>
 * <ul>
 *   <li>Row Key: {@code uuid_timestamp} (STRING) — composite key of uuid + '_' + epoch millis</li>
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        LOG.info("Starting Kafka to HBase pipeline");

        HBaseSink.createAndDeployJob(env, params);
    }

    public static class HBaseSink {

        public static void createAndDeployJob(
                StreamExecutionEnvironment env, ParameterTool params) {

            StreamTableEnvironment tableEnv =
                    StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

            // Checkpoint configuration
            String checkpointInterval = params.get("checkpoint_interval", "1 min");
            tableEnv.getConfig().getConfiguration()
                    .setString("execution.checkpointing.interval", checkpointInterval);

            // Kafka parameters
            String kafkaBootstrapServers = params.getRequired("kafka_bootstrap_servers");
            String kafkaTopic = params.get("topic", "kafka_topic");
            String kafkaGroupId = params.get("kafka_group_id", "flink-kafka-hbase-group");
            String kafkaFormat = params.get("kafka_format", "json");
            String kafkaStartupMode = params.get("kafka_startup_mode", "earliest-offset");

            // HBase parameters
            String hbaseTable = params.get("hbase_table", "default:user_order");
            String hbaseZkQuorum = params.getRequired("hbase_zookeeper_quorum");
            String hbaseZkZnode = params.get("hbase_zookeeper_znode", "/hbase");

            LOG.info("Kafka — Servers: {}, Topic: {}, Format: {}, StartupMode: {}",
                    kafkaBootstrapServers, kafkaTopic, kafkaFormat, kafkaStartupMode);
            LOG.info("HBase — Table: {}, ZK Quorum: {}, ZK Znode: {}",
                    hbaseTable, hbaseZkQuorum, hbaseZkZnode);

            // ============================================================
            // 1. Create Kafka Source Table
            // ============================================================
            // Schema matches the same structure as flink-kafka-opensearch
            String createKafkaSource = String.format(
                    "CREATE TABLE kafka_source (\n"
                    + "  uuid STRING,\n"
                    + "  user_name STRING,\n"
                    + "  phone_number BIGINT,\n"
                    + "  product_id INT,\n"
                    + "  product_name STRING,\n"
                    + "  product_type STRING,\n"
                    + "  manufacturing_date INT,\n"
                    + "  price FLOAT,\n"
                    + "  unit INT,\n"
                    + "  ts TIMESTAMP(3)\n"
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
            // HBase table uses column families to organize data.
            // The rowkey is a composite key: uuid + '_' + timestamp_millis (BIGINT as epoch ms).
            // Flink HBase connector requires ROW type for each column family.
            String createHBaseSink = String.format(
                    "CREATE TABLE hbase_sink (\n"
                    + "  rowkey STRING,\n"
                    + "  info ROW<user_name STRING, phone_number BIGINT, ts TIMESTAMP(3)>,\n"
                    + "  product ROW<product_id INT, product_name STRING, product_type STRING, "
                    + "manufacturing_date INT, price FLOAT, unit INT>\n"
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
            // Build composite rowkey: uuid + '_' + epoch_millis from ts.
            // CAST(ts AS BIGINT) converts TIMESTAMP(3) to epoch milliseconds.
            // uuid becomes part of the rowkey for uniqueness, timestamp for time-based ordering.
            String insertSql =
                    "INSERT INTO hbase_sink\n"
                    + "SELECT\n"
                    + "  uuid || '_' || CAST(CAST(ts AS BIGINT) AS STRING) AS rowkey,\n"
                    + "  ROW(user_name, phone_number, ts) AS info,\n"
                    + "  ROW(product_id, product_name, product_type, "
                    + "manufacturing_date, price, unit) AS product\n"
                    + "FROM kafka_source";

            LOG.info("Starting INSERT pipeline: Kafka -> HBase");
            tableEnv.executeSql(insertSql);
        }
    }
}
