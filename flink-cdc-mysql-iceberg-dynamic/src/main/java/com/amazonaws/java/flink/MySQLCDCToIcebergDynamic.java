package com.amazonaws.java.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink CDC application using DataStream API to sync MySQL changes to Iceberg with dynamic schema support.
 *
 * This implementation uses:
 * - Flink CDC MySQL Source with DataStream API
 * - Iceberg Dynamic Sink for automatic schema evolution
 * - immediateTableUpdate for real-time schema updates
 *
 * Key Features:
 * - Automatic schema detection from MySQL CDC events
 * - Dynamic schema evolution when source table schema changes (via immediateTableUpdate)
 * - Support for INSERT, UPDATE, DELETE operations
 * - Dynamic table routing: CDC events from different MySQL tables are routed to separate Iceberg tables
 * - Configurable catalog types (Hadoop, Hive, REST, Glue)
 * - AWS S3 Tables support with SigV4 authentication
 *
 * Reviewed and optimized based on Iceberg official documentation:
 * https://iceberg.apache.org/docs/latest/flink-writes/
 */
public class MySQLCDCToIcebergDynamic {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCToIcebergDynamic.class);

    private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else {
            try {
                Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
                Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
                if (flinkProperties != null) {
                    Map<String, String> map = new HashMap<>(flinkProperties.size());
                    flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
                    return ParameterTool.fromMap(map);
                }
            } catch (Exception e) {
                LOG.info("Unable to load from KDA runtime properties, trying command line args: {}", e.getMessage());
            }
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = loadApplicationParameters(args, env);

        LOG.info("Starting MySQL CDC to Iceberg Dynamic Schema pipeline");

        // Enable checkpointing — required for Iceberg exactly-once semantics.
        // Per Iceberg docs: "Flink streaming write jobs rely on snapshot summary to keep the
        // last committed checkpoint ID". Commit happens in notifyCheckpointComplete callback.
        long checkpointInterval = params.getLong("checkpoint.interval", 300000L); // Default 5 minutes
        env.enableCheckpointing(checkpointInterval);

        // Get MySQL configuration
        String mysqlHostname = params.get("mysql.hostname", "localhost");
        int mysqlPort = params.getInt("mysql.port", 3306);
        String mysqlUsername = params.get("mysql.username", "root");
        String mysqlPassword = params.get("mysql.password", "password");
        String mysqlDatabase = params.get("mysql.database", "testdb");
        String mysqlTables = params.get("mysql.tables", ".*"); // Support regex pattern
        String serverTimezone = params.get("mysql.server.timezone", "UTC");

        // Get Iceberg configuration
        String catalogType = params.get("iceberg.catalog.type", "hadoop"); // hadoop, hive, rest, glue
        String catalogName = params.get("iceberg.catalog.name", "iceberg_catalog");
        String warehouse = params.get("iceberg.warehouse", "s3://my-bucket/warehouse");
        String namespace = params.get("iceberg.namespace", "default");
        String branch = params.get("iceberg.branch", null); // Optional branch name

        // [FIX] Whether to enable upsert mode (requires v2 table format + primary key / equality fields)
        // Per Iceberg docs: "OVERWRITE and UPSERT modes are mutually exclusive"
        boolean upsertEnabled = params.getBoolean("sink.upsert", false);

        LOG.info("MySQL Source - Host: {}, Port: {}, Database: {}, Tables: {}",
                mysqlHostname, mysqlPort, mysqlDatabase, mysqlTables);
        LOG.info("Iceberg Sink - Catalog: {}, Warehouse: {}, Namespace: {}",
                catalogType, warehouse, namespace);

        // Create MySQL CDC Source with schema change support
        // [NOTE] JsonDebeziumDeserializationSchema includes full schema info in the JSON output,
        // which CDCToDynamicRecordConverter will parse for accurate type inference.
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDatabase)
                .tableList(mysqlDatabase + "." + mysqlTables)
                .username(mysqlUsername)
                .password(mysqlPassword)
                .serverTimeZone(serverTimezone)
                .deserializer(new JsonDebeziumDeserializationSchema(true)) // [FIX] includeSchema=true to get Debezium schema info
                .includeSchemaChanges(true) // Enable schema change events for awareness
                .scanNewlyAddedTableEnabled(true) // Detect newly added tables
                .build();

        DataStream<String> cdcStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(1); // CDC source must be single parallelism to maintain order

        // [FIX] Convert CDC JSON events to DynamicRecord.
        // Now routes each MySQL table to a separate Iceberg table (dynamic routing),
        // and uses Debezium schema info for accurate type inference.
        DataStream<DynamicRecord> dynamicRecordStream = cdcStream
                .process(new CDCToDynamicRecordConverter(namespace, branch, upsertEnabled))
                .name("CDC to DynamicRecord Converter");

        // [FIX] Create proper CatalogLoader based on catalog type.
        // Original code always used CatalogLoader.hadoop() regardless of catalog type.
        Map<String, String> catalogProperties = buildCatalogProperties(params, catalogType, warehouse);
        Configuration hadoopConf = new Configuration();
        CatalogLoader catalogLoader = buildCatalogLoader(catalogType, catalogName, catalogProperties, hadoopConf);

        // Build Iceberg Dynamic Sink with automatic schema evolution
        // Per Iceberg docs: Dynamic Sink supports NONE and HASH distribution modes.
        // RANGE distribution is NOT supported and will fall back to HASH.
        DynamicIcebergSink.Builder<DynamicRecord> sinkBuilder = DynamicIcebergSink.forInput(dynamicRecordStream)
                .catalogLoader(catalogLoader)
                .writeParallelism(params.getInt("sink.parallelism", 2))
                .immediateTableUpdate(params.getBoolean("sink.immediate.table.update", true))
                .cacheMaxSize(params.getInt("sink.cache.max.size", 100))
                .cacheRefreshMs(params.getLong("sink.cache.refresh.ms", 60000L));

        // [FIX] Only set upsert when explicitly enabled.
        // Default changed to false because upsert requires:
        //   1. v2 table format
        //   2. Primary key / equality fields defined
        // Setting it blindly to true would cause failures on tables without primary keys.
        if (upsertEnabled) {
            sinkBuilder.set("write.upsert.enabled", "true");
            // Ensure v2 format for upsert support
            sinkBuilder.set("format-version", "2");
        }

        // Optional: Set write format (parquet is the default and recommended)
        String writeFormat = params.get("sink.write.format", null);
        if (writeFormat != null) {
            sinkBuilder.set("write-format", writeFormat);
        }

        // Optional: Set target file size
        String targetFileSize = params.get("sink.target.file.size.bytes", null);
        if (targetFileSize != null) {
            sinkBuilder.set("target-file-size-bytes", targetFileSize);
        }

        sinkBuilder.append();

        LOG.info("Pipeline configured with Dynamic Iceberg Sink (upsert={}, immediateTableUpdate={}), starting execution",
                upsertEnabled, params.getBoolean("sink.immediate.table.update", true));
        env.execute("MySQL CDC to Iceberg Dynamic Schema");
    }

    /**
     * Build proper CatalogLoader based on catalog type.
     *
     * [FIX] Original code always used CatalogLoader.hadoop() regardless of catalog type,
     * which would fail for Hive, REST, and Glue catalogs. Now correctly creates the
     * appropriate CatalogLoader for each catalog type.
     */
    private static CatalogLoader buildCatalogLoader(
            String catalogType, String catalogName,
            Map<String, String> catalogProperties, Configuration hadoopConf) {
        switch (catalogType) {
            case "hive":
                String hiveUri = catalogProperties.get("uri");
                LOG.info("Creating Hive CatalogLoader with URI: {}", hiveUri);
                return CatalogLoader.hive(catalogName, hadoopConf, catalogProperties);

            case "rest":
                String restUri = catalogProperties.get("uri");
                LOG.info("Creating REST CatalogLoader with URI: {}", restUri);
                return CatalogLoader.rest(catalogName, hadoopConf, catalogProperties);

            case "glue":
                // Glue catalog uses custom catalog implementation
                LOG.info("Creating Glue CatalogLoader");
                return CatalogLoader.custom(
                        catalogName,
                        catalogProperties,
                        hadoopConf,
                        "org.apache.iceberg.aws.glue.GlueCatalog");

            case "hadoop":
            default:
                String warehouse = catalogProperties.get("warehouse");
                LOG.info("Creating Hadoop CatalogLoader with warehouse: {}", warehouse);
                return CatalogLoader.hadoop(catalogName, hadoopConf, catalogProperties);
        }
    }

    private static Map<String, String> buildCatalogProperties(
            ParameterTool params, String catalogType, String warehouse) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put("catalog-type", catalogType);
        catalogProperties.put("warehouse", warehouse);

        // Add catalog-specific properties
        if ("rest".equals(catalogType)) {
            String restUri = params.get("iceberg.rest.uri", "http://localhost:8181");
            catalogProperties.put("uri", restUri);

            // For AWS S3 Tables REST Catalog
            if (params.getBoolean("iceberg.rest.sigv4.enabled", false)) {
                String awsRegion = params.get("aws.region", "us-east-1");
                catalogProperties.put("rest.sigv4-enabled", "true");
                catalogProperties.put("rest.signing-name", "s3tables");
                catalogProperties.put("rest.signing-region", awsRegion);
                catalogProperties.put("client.region", awsRegion);
            }
        } else if ("hive".equals(catalogType)) {
            String hiveUri = params.get("iceberg.hive.uri", "thrift://localhost:9083");
            catalogProperties.put("uri", hiveUri);
        } else if ("glue".equals(catalogType)) {
            // AWS Glue Catalog configuration
            String awsRegion = params.get("aws.region", "us-east-1");
            catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProperties.put("client.region", awsRegion);

            // Optional: Glue catalog ID (defaults to AWS account ID)
            String glueCatalogId = params.get("iceberg.glue.catalog.id", null);
            if (glueCatalogId != null && !glueCatalogId.isEmpty()) {
                catalogProperties.put("glue.id", glueCatalogId);
            }

            // Optional: Skip name validation (useful for special characters)
            if (params.getBoolean("iceberg.glue.skip.name.validation", false)) {
                catalogProperties.put("glue.skip-name-validation", "true");
            }

            // Optional: Glue endpoint override (for VPC endpoints or testing)
            String glueEndpoint = params.get("iceberg.glue.endpoint", null);
            if (glueEndpoint != null && !glueEndpoint.isEmpty()) {
                catalogProperties.put("glue.endpoint", glueEndpoint);
            }

            LOG.info("Configured AWS Glue Catalog - Region: {}, Catalog ID: {}",
                    awsRegion, glueCatalogId != null ? glueCatalogId : "default");
        }

        // Add S3 FileIO properties if using S3
        if (warehouse.startsWith("s3://") || warehouse.startsWith("s3a://")) {
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

            // Optional: S3 endpoint override (for LocalStack or MinIO)
            String s3Endpoint = params.get("iceberg.s3.endpoint", null);
            if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
                catalogProperties.put("s3.endpoint", s3Endpoint);
            }

            // Optional: Path style access (for MinIO compatibility)
            if (params.getBoolean("iceberg.s3.path.style.access", false)) {
                catalogProperties.put("s3.path-style-access", "true");
            }
        }

        return catalogProperties;
    }
}
