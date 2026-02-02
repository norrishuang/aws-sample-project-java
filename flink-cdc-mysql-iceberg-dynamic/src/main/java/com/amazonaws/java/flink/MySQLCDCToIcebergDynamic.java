package com.amazonaws.java.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.DistributionMode;
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
 * - Configurable catalog types (Hadoop, Hive, REST)
 * - AWS S3 Tables support with SigV4 authentication
 * - No manual schema handling required - Iceberg handles it automatically!
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
                LOG.info("Unable to load from KDA runtime properties, trying command line args: " + e.getMessage());
            }
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = loadApplicationParameters(args, env);

        LOG.info("Starting MySQL CDC to Iceberg Dynamic Schema pipeline");

        // Enable checkpointing
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
        String tableName = params.get("iceberg.table.name", "cdc_table");
        String branch = params.get("iceberg.branch", null); // Optional branch name
        
        LOG.info("MySQL Source - Host: {}, Port: {}, Database: {}, Tables: {}", 
                 mysqlHostname, mysqlPort, mysqlDatabase, mysqlTables);
        LOG.info("Iceberg Sink - Catalog: {}, Warehouse: {}, Table: {}.{}", 
                 catalogType, warehouse, namespace, tableName);

        // Create MySQL CDC Source with schema change support
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDatabase)
                .tableList(mysqlDatabase + "." + mysqlTables)
                .username(mysqlUsername)
                .password(mysqlPassword)
                .serverTimeZone(serverTimezone)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .includeSchemaChanges(true) // Critical: Enable schema change events
                .scanNewlyAddedTableEnabled(true) // Detect newly added tables
                .build();

        DataStream<String> cdcStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(1); // CDC source should be single parallelism

        // Convert CDC JSON events to DynamicRecord
        // DynamicRecord contains: TableIdentifier, Schema, RowData, PartitionSpec, etc.
        DataStream<DynamicRecord> dynamicRecordStream = cdcStream
                .process(new CDCToDynamicRecordConverter(namespace, tableName, branch))
                .name("CDC to DynamicRecord Converter");

        // Create Iceberg Catalog Loader
        Map<String, String> catalogProperties = buildCatalogProperties(params, catalogType, warehouse);
        Configuration hadoopConf = new Configuration();
        CatalogLoader catalogLoader = CatalogLoader.hadoop(catalogName, hadoopConf, catalogProperties);

        // Build Iceberg Dynamic Sink with automatic schema evolution
        DynamicIcebergSink.forInput(dynamicRecordStream)
                .catalogLoader(catalogLoader)
                .writeParallelism(params.getInt("sink.parallelism", 2))
                .immediateTableUpdate(params.getBoolean("sink.immediate.table.update", true)) // Key: Enable immediate schema updates
                .cacheMaxSize(params.getInt("sink.cache.max.size", 100)) // Cache for table metadata
                .cacheRefreshMs(params.getLong("sink.cache.refresh.ms", 60000L)) // Cache refresh interval
                .set("write.upsert.enabled", String.valueOf(params.getBoolean("sink.upsert", true))) // Enable upsert
                .append(); // Use append mode (upsert is controlled by write.upsert.enabled)

        LOG.info("Pipeline configured with Dynamic Iceberg Sink, starting execution");
        env.execute("MySQL CDC to Iceberg Dynamic Schema");
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
