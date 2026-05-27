package com.amazonaws.java.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink application that consumes plain JSON from Kafka and writes to Iceberg/S3Tables
 * using the official Iceberg Dynamic Sink API.
 *
 * <h2>Architecture</h2>
 * <pre>
 *   Kafka Source (plain JSON)
 *       │
 *       ▼
 *   DataStream&lt;String&gt;
 *       │
 *       ▼
 *   DynamicIcebergSink
 *     ├─ KafkaJsonDynamicRecordGenerator (JSON → DynamicRecord)
 *     ├─ DynamicRecordProcessor (schema evolution, table creation, routing)
 *     ├─ DynamicWriter (writes data files)
 *     └─ DynamicCommitter (commits to Iceberg catalog)
 * </pre>
 */
public class KafkaJsonToIcebergDynamic {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonToIcebergDynamic.class);

    private static ParameterTool loadApplicationParameters(
            String[] args, StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else {
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
                LOG.info("Unable to load from KDA runtime, trying command line args: {}", e.getMessage());
            }
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = loadApplicationParameters(args, env);

        LOG.info("Starting Kafka JSON to Iceberg Dynamic Sink pipeline");

        // ============================================================
        // 1. Checkpointing
        // ============================================================
        long checkpointInterval = params.getLong("checkpoint.interval", 60_000L);
        env.enableCheckpointing(checkpointInterval);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setCheckpointTimeout(params.getLong("checkpoint.timeout", 600_000L));
        cpConfig.setTolerableCheckpointFailureNumber(
                params.getInt("checkpoint.tolerable.failures", 3));
        cpConfig.setMinPauseBetweenCheckpoints(
                params.getLong("checkpoint.min.pause", 30_000L));
        cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ============================================================
        // 2. Kafka Source configuration
        // ============================================================
        String kafkaBootstrapServers = params.get("kafka.bootstrap.servers", "localhost:9092");
        String kafkaTopic = params.get("kafka.topic", "game-action-logs");
        String kafkaGroupId = params.get("kafka.group.id", "flink-kafka-iceberg-dynamic");
        String startupMode = params.get("kafka.startup.mode", "latest-offset");

        OffsetsInitializer offsetsInitializer;
        switch (startupMode) {
            case "earliest-offset":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "group-offsets":
                offsetsInitializer = OffsetsInitializer.committedOffsets();
                break;
            case "latest-offset":
            default:
                offsetsInitializer = OffsetsInitializer.latest();
                break;
        }

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        LOG.info("Kafka Source — Brokers: {}, Topic: {}, Group: {}, StartupMode: {}",
                kafkaBootstrapServers, kafkaTopic, kafkaGroupId, startupMode);

        // ============================================================
        // 3. Sink target & Iceberg configuration
        // ============================================================
        String sinkTarget = params.get("sink.target", "iceberg").toLowerCase();
        String catalogType;
        String catalogName;
        String warehouse;

        if ("s3tables".equals(sinkTarget)) {
            catalogType = "rest";
            catalogName = params.get("iceberg.catalog.name", "s3tables_catalog");
            warehouse = params.get("s3tables.warehouse",
                    params.get("iceberg.warehouse", ""));
            if (warehouse.isEmpty()) {
                throw new IllegalArgumentException(
                        "S3 Tables requires --s3tables.warehouse (table bucket ARN)");
            }
            LOG.info("Sink target: S3 Tables — warehouse: {}", warehouse);
        } else {
            catalogType = params.get("iceberg.catalog.type", "hadoop");
            catalogName = params.get("iceberg.catalog.name", "iceberg_catalog");
            warehouse = params.get("iceberg.warehouse", "s3://my-bucket/warehouse");
            LOG.info("Sink target: Iceberg — catalog type: {}", catalogType);
        }

        String namespace = params.get("iceberg.namespace", "default");
        String tableName = params.get("iceberg.table", "kafka_json_table");
        String routingField = params.get("kafka.routing.field", null);
        String schemaDefinition = params.get("iceberg.schema", null);
        String branch = params.get("iceberg.branch", null);
        int writeParallel = params.getInt("sink.parallelism", 2);

        LOG.info("Sink — Namespace: {}, Table: {}, RoutingField: {}, Parallelism: {}",
                namespace, tableName, routingField, writeParallel);

        // ============================================================
        // 4. Build CatalogLoader
        // ============================================================
        Map<String, String> catalogProperties = buildCatalogProperties(params, catalogType, warehouse);
        Configuration hadoopConf = new Configuration();
        CatalogLoader catalogLoader = buildCatalogLoader(
                catalogType, catalogName, catalogProperties, hadoopConf);

        // ============================================================
        // 5. Build Iceberg Dynamic Sink
        // ============================================================
        DynamicIcebergSink.<String>forInput(kafkaStream)
                .generator(new KafkaJsonDynamicRecordGenerator(
                        namespace, tableName, routingField, schemaDefinition, branch, writeParallel))
                .catalogLoader(catalogLoader)
                .writeParallelism(writeParallel)
                .immediateTableUpdate(true)
                .cacheMaxSize(100)
                .cacheRefreshMs(60_000L)
                .uidPrefix("kafka-iceberg-dynamic")
                .append();

        LOG.info("Pipeline configured — executing Flink job");
        env.execute("Kafka JSON to Iceberg Dynamic Sink");
    }

    // ==================== Catalog Loader ====================

    private static CatalogLoader buildCatalogLoader(
            String catalogType, String catalogName,
            Map<String, String> catalogProperties, Configuration hadoopConf) {
        switch (catalogType) {
            case "hive":
                return CatalogLoader.hive(catalogName, hadoopConf, catalogProperties);
            case "rest":
                return CatalogLoader.rest(catalogName, hadoopConf, catalogProperties);
            case "glue":
                return CatalogLoader.custom(
                        catalogName, catalogProperties, hadoopConf,
                        "org.apache.iceberg.aws.glue.GlueCatalog");
            case "hadoop":
            default:
                return CatalogLoader.hadoop(catalogName, hadoopConf, catalogProperties);
        }
    }

    private static Map<String, String> buildCatalogProperties(
            ParameterTool params, String catalogType, String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("catalog-type", catalogType);
        props.put("warehouse", warehouse);

        String sinkTarget = params.get("sink.target", "iceberg").toLowerCase();

        switch (catalogType) {
            case "rest":
                if ("s3tables".equals(sinkTarget)) {
                    String region = params.get("aws.region", "us-east-1");
                    String s3tablesUri = params.get("s3tables.rest.uri",
                            "https://s3tables." + region + ".amazonaws.com/iceberg");
                    props.put("uri", s3tablesUri);
                    props.put("rest.sigv4-enabled", "true");
                    props.put("rest.signing-name", "s3tables");
                    props.put("rest.signing-region", region);
                    props.put("client.region", region);
                } else {
                    props.put("uri", params.get("iceberg.rest.uri", "http://localhost:8181"));
                    if (params.getBoolean("iceberg.rest.sigv4.enabled", false)) {
                        String region = params.get("aws.region", "us-east-1");
                        props.put("rest.sigv4-enabled", "true");
                        props.put("rest.signing-name", "s3tables");
                        props.put("rest.signing-region", region);
                        props.put("client.region", region);
                    }
                }
                break;
            case "hive":
                props.put("uri", params.get("iceberg.hive.uri", "thrift://localhost:9083"));
                break;
            case "glue":
                String region = params.get("aws.region", "us-east-1");
                props.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
                props.put("client.region", region);
                String glueCatalogId = params.get("iceberg.glue.catalog.id", null);
                if (glueCatalogId != null && !glueCatalogId.isEmpty()) {
                    props.put("glue.id", glueCatalogId);
                }
                break;
        }

        // S3 FileIO
        if ("s3tables".equals(sinkTarget)) {
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            props.put("client.region", params.get("aws.region", "us-east-1"));
        } else if (warehouse.startsWith("s3://") || warehouse.startsWith("s3a://")) {
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        }

        return props;
    }
}
