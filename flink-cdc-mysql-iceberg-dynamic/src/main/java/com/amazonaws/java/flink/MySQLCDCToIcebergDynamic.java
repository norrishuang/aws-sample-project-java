package com.amazonaws.java.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink CDC application that syncs MySQL changes to Apache Iceberg using the official
 * <strong>Iceberg Dynamic Sink API</strong>.
 *
 * <h2>Architecture</h2>
 * <pre>
 *   MySQL CDC Source (Debezium JSON)
 *       │
 *       ▼
 *   DataStream&lt;String&gt;  (raw CDC JSON events)
 *       │
 *       ▼
 *   DynamicIcebergSink
 *     ├─ DynamicRecordGenerator  (CDCDynamicRecordGenerator: converts JSON → DynamicRecord)
 *     ├─ DynamicRecordProcessor  (built-in: schema evolution, table creation, routing)
 *     ├─ DynamicWriter           (built-in: writes data files)
 *     └─ DynamicCommitter        (built-in: commits to Iceberg catalog)
 * </pre>
 *
 * <h2>Sink Targets</h2>
 * <ul>
 *   <li>{@code --sink.target iceberg} (default) — writes to standard Iceberg catalog
 *       (Glue, Hive, Hadoop, or REST)</li>
 *   <li>{@code --sink.target s3tables} — writes to Amazon S3 Tables via REST catalog
 *       with SigV4 authentication. Requires {@code --s3tables.warehouse} (table bucket ARN)
 *       and {@code --aws.region}.</li>
 * </ul>
 *
 * <h2>Key Design Decisions</h2>
 * <ul>
 *   <li>The raw {@code DataStream<String>} is passed directly to {@code DynamicIcebergSink.forInput()},
 *       with a {@link CDCDynamicRecordGenerator} set via {@code .generator()}.</li>
 *   <li>Schema evolution is handled automatically by the Dynamic Sink — no manual schema management.</li>
 *   <li>Table creation is handled automatically by the Dynamic Sink — no manual catalog operations.</li>
 *   <li>Table routing is done via {@code DynamicRecord.tableIdentifier()} — no manual routing logic.</li>
 * </ul>
 *
 * @see CDCDynamicRecordGenerator
 * @see <a href="https://iceberg.apache.org/docs/latest/flink-writes/">Iceberg Flink Writes</a>
 */
public class MySQLCDCToIcebergDynamic {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCToIcebergDynamic.class);

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
                LOG.info("Unable to load from KDA runtime properties, trying command line args: {}",
                        e.getMessage());
            }
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = loadApplicationParameters(args, env);

        LOG.info("Starting MySQL CDC to Iceberg Dynamic Sink pipeline");

        // ============================================================
        // 1. Checkpointing — required for Iceberg exactly-once semantics
        // ============================================================
        // IMPORTANT: When using Glue Catalog, the Dynamic Sink's DynamicRecordProcessor
        // calls Glue API (createTable/getTable) synchronously in the data processing thread.
        // If a checkpoint barrier arrives while a Glue API call is in flight, Flink may
        // interrupt the thread, causing AWS SDK's AbortedException (WARN level).
        //
        // Mitigation strategy:
        //   1. Use a longer checkpoint interval (default 5min) to reduce interrupt frequency
        //   2. Set checkpoint timeout to allow Glue operations to complete
        //   3. Use UNALIGNED checkpoints to avoid blocking on barriers
        //   4. Configure Glue client timeouts via catalog properties (see below)
        long checkpointInterval = params.getLong("checkpoint.interval", 300_000L); // 5 min default
        env.enableCheckpointing(checkpointInterval);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        // Allow checkpoints to take up to 10 minutes before timing out
        cpConfig.setCheckpointTimeout(params.getLong("checkpoint.timeout", 600_000L));
        // Tolerate up to 3 consecutive checkpoint failures before failing the job
        cpConfig.setTolerableCheckpointFailureNumber(
                params.getInt("checkpoint.tolerable.failures", 3));
        // Minimum pause between checkpoints — prevents checkpoint storms
        cpConfig.setMinPauseBetweenCheckpoints(
                params.getLong("checkpoint.min.pause", 60_000L));
        // Use EXACTLY_ONCE for Iceberg consistency
        cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Enable unaligned checkpoints to reduce barrier wait time,
        // which decreases the chance of interrupting Glue API calls
        if (params.getBoolean("checkpoint.unaligned", false)) {
            cpConfig.enableUnalignedCheckpoints();
            LOG.info("Unaligned checkpoints enabled");
        }

        LOG.info("Checkpoint config — interval: {}ms, timeout: {}ms, tolerableFailures: {}, minPause: {}ms",
                checkpointInterval,
                cpConfig.getCheckpointTimeout(),
                cpConfig.getTolerableCheckpointFailureNumber(),
                cpConfig.getMinPauseBetweenCheckpoints());

        // ============================================================
        // 2. MySQL CDC Source configuration
        // ============================================================
        String mysqlHostname   = params.get("mysql.hostname", "localhost");
        int    mysqlPort       = params.getInt("mysql.port", 3306);
        String mysqlUsername   = params.get("mysql.username", "root");
        String mysqlPassword   = params.get("mysql.password", "password");
        String mysqlDatabase   = params.get("mysql.database", "testdb");
        String mysqlTables     = params.get("mysql.tables", ".*"); // regex pattern
        String serverTimezone  = params.get("mysql.server.timezone", "UTC");

        // ============================================================
        // 3. Sink target & Iceberg configuration
        //    --sink.target  iceberg  → use Glue/Hive/Hadoop/REST catalog (original behavior)
        //    --sink.target  s3tables → use S3 Tables REST catalog with SigV4
        // ============================================================
        String  sinkTarget     = params.get("sink.target", "iceberg").toLowerCase();
        String  catalogType;
        String  catalogName;
        String  warehouse;

        if ("s3tables".equals(sinkTarget)) {
            // S3 Tables: force REST catalog with SigV4
            catalogType = "rest";
            catalogName = params.get("iceberg.catalog.name", "s3tables_catalog");
            warehouse   = params.get("s3tables.warehouse",
                    params.get("iceberg.warehouse", ""));
            // S3 Tables requires a table bucket ARN as warehouse
            if (warehouse.isEmpty()) {
                throw new IllegalArgumentException(
                        "S3 Tables requires --s3tables.warehouse (table bucket ARN), e.g. " +
                        "arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket");
            }
            LOG.info("Sink target: S3 Tables — warehouse (table bucket ARN): {}", warehouse);
        } else {
            // Standard Iceberg catalog (glue/hive/hadoop/rest)
            catalogType = params.get("iceberg.catalog.type", "hadoop");
            catalogName = params.get("iceberg.catalog.name", "iceberg_catalog");
            warehouse   = params.get("iceberg.warehouse", "s3://my-bucket/warehouse");
            LOG.info("Sink target: Iceberg — catalog type: {}", catalogType);
        }

        String  namespace      = params.get("iceberg.namespace", "default");
        String  branch         = params.get("iceberg.branch", null);
        boolean upsertEnabled  = params.getBoolean("sink.upsert", false);
        int     writeParallel  = params.getInt("sink.parallelism", 2);

        LOG.info("MySQL Source — Host: {}, Port: {}, Database: {}, Tables: {}",
                mysqlHostname, mysqlPort, mysqlDatabase, mysqlTables);
        LOG.info("Sink — Target: {}, Catalog: {} ({}), Warehouse: {}, Namespace: {}, Upsert: {}",
                sinkTarget, catalogName, catalogType, warehouse, namespace, upsertEnabled);

        // ============================================================
        // 4. Build MySQL CDC Source
        // ============================================================
        // includeSchema=true: Debezium JSON will contain full column type info,
        // which CDCDynamicRecordGenerator uses for accurate Iceberg type mapping.
        //
        // tableList format: Flink CDC requires each table to be fully qualified as "db.table".
        //   --mysql.tables supports:
        //     1. Regex:       ".*"  or "user_.*"    → prefixed as "norrisdb\\..*"
        //     2. Multi-table: "t1|t2|t3"            → expanded to "norrisdb.t1,norrisdb.t2,norrisdb.t3"
        //     3. Single:      "orders"              → prefixed as "norrisdb.orders"
        String qualifiedTables = buildQualifiedTableList(mysqlDatabase, mysqlTables);
        LOG.info("MySQL tableList: {}", qualifiedTables);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDatabase)
                .tableList(qualifiedTables)
                .username(mysqlUsername)
                .password(mysqlPassword)
                .serverTimeZone(serverTimezone)
                .deserializer(new JsonDebeziumDeserializationSchema(true))
                .includeSchemaChanges(false)
                .scanNewlyAddedTableEnabled(true)
                .build();

        DataStream<String> cdcStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(1); // CDC source must be single-parallelism for ordering

        // ============================================================
        // 5. Build CatalogLoader
        // ============================================================
        Map<String, String> catalogProperties = buildCatalogProperties(params, catalogType, warehouse);
        Configuration hadoopConf = new Configuration();
        CatalogLoader catalogLoader = buildCatalogLoader(
                catalogType, catalogName, catalogProperties, hadoopConf);

        // ============================================================
        // 6. Build Iceberg Dynamic Sink (the correct way)
        // ============================================================
        // Key: pass the raw DataStream<String> and a DynamicRecordGenerator<String>.
        // The sink internally uses DynamicRecordProcessor to:
        //   - call generator.generate() for each record
        //   - handle table creation/updates
        //   - handle schema evolution
        //   - route records to the right table
        DynamicIcebergSink.Builder<String> sinkBuilder = DynamicIcebergSink
                .<String>forInput(cdcStream)
                .generator(new CDCDynamicRecordGenerator(
                        namespace, branch, upsertEnabled, writeParallel))
                .catalogLoader(catalogLoader)
                .writeParallelism(writeParallel)
                .immediateTableUpdate(params.getBoolean("sink.immediate.table.update", true))
                .cacheMaxSize(params.getInt("sink.cache.max.size", 100))
                .cacheRefreshMs(params.getLong("sink.cache.refresh.ms", 60_000L))
                .uidPrefix("cdc-iceberg-dynamic");

        // Upsert configuration
        if (upsertEnabled) {
            sinkBuilder.set("write.upsert.enabled", "true");
            sinkBuilder.set("format-version", "2");
        }

        // Optional write format
        String writeFormat = params.get("sink.write.format", null);
        if (writeFormat != null) {
            sinkBuilder.set("write-format", writeFormat);
        }

        // Optional target file size
        String targetFileSize = params.get("sink.target.file.size.bytes", null);
        if (targetFileSize != null) {
            sinkBuilder.set("target-file-size-bytes", targetFileSize);
        }

        // Append the sink to the pipeline — this builds the full topology
        sinkBuilder.append();

        LOG.info("Pipeline configured — executing Flink job");
        env.execute("MySQL CDC to Iceberg Dynamic Sink");
    }

    // ==================== Catalog Loader ====================

    private static CatalogLoader buildCatalogLoader(
            String catalogType, String catalogName,
            Map<String, String> catalogProperties, Configuration hadoopConf) {
        switch (catalogType) {
            case "hive":
                LOG.info("Creating Hive CatalogLoader with URI: {}",
                        catalogProperties.get("uri"));
                return CatalogLoader.hive(catalogName, hadoopConf, catalogProperties);

            case "rest":
                LOG.info("Creating REST CatalogLoader with URI: {}",
                        catalogProperties.get("uri"));
                return CatalogLoader.rest(catalogName, hadoopConf, catalogProperties);

            case "glue":
                LOG.info("Creating Glue CatalogLoader");
                return CatalogLoader.custom(
                        catalogName, catalogProperties, hadoopConf,
                        "org.apache.iceberg.aws.glue.GlueCatalog");

            case "hadoop":
            default:
                LOG.info("Creating Hadoop CatalogLoader with warehouse: {}",
                        catalogProperties.get("warehouse"));
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

        // Catalog-specific properties
        switch (catalogType) {
            case "rest":
                if ("s3tables".equals(sinkTarget)) {
                    // S3 Tables REST catalog configuration
                    // URI: S3 Tables endpoint (regional)
                    String region = params.get("aws.region", "us-east-1");
                    String s3tablesUri = params.get("s3tables.rest.uri",
                            "https://s3tables." + region + ".amazonaws.com/iceberg");
                    props.put("uri", s3tablesUri);

                    // SigV4 authentication is mandatory for S3 Tables
                    props.put("rest.sigv4-enabled", "true");
                    props.put("rest.signing-name", "s3tables");
                    props.put("rest.signing-region", region);
                    props.put("client.region", region);

                    LOG.info("S3 Tables REST catalog — URI: {}, Region: {}", s3tablesUri, region);
                } else {
                    // Generic REST catalog
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
                if (params.getBoolean("iceberg.glue.skip.name.validation", false)) {
                    props.put("glue.skip-name-validation", "true");
                }
                String glueEndpoint = params.get("iceberg.glue.endpoint", null);
                if (glueEndpoint != null && !glueEndpoint.isEmpty()) {
                    props.put("glue.endpoint", glueEndpoint);
                }
                // Glue HTTP client timeout configuration.
                // Shorter timeouts ensure Glue API calls complete (or fail fast) before
                // a checkpoint barrier interrupts the thread.
                // API call timeout: total time for a single API call including retries (default 60s → 30s)
                props.put("client.api-call-timeout-ms",
                        params.get("glue.api.call.timeout.ms", "30000"));
                // API call attempt timeout: timeout for a single HTTP attempt (default ~30s → 10s)
                props.put("client.api-call-attempt-timeout-ms",
                        params.get("glue.api.call.attempt.timeout.ms", "10000"));
                // Connection timeout (default 2s, good as is)
                props.put("client.connect-timeout-ms",
                        params.get("glue.connect.timeout.ms", "2000"));
                // Socket timeout (default 30s → 10s for faster failure)
                props.put("client.socket-timeout-ms",
                        params.get("glue.socket.timeout.ms", "10000"));
                break;
        }

        // S3 FileIO properties
        if ("s3tables".equals(sinkTarget)) {
            // S3 Tables: IO is handled via REST catalog's vended credentials.
            // Set S3FileIO explicitly for compatibility; the REST catalog will
            // provide the actual credentials via the credential-vending flow.
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            String region = params.get("aws.region", "us-east-1");
            props.put("client.region", region);
        } else if (warehouse.startsWith("s3://") || warehouse.startsWith("s3a://")) {
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            String s3Endpoint = params.get("iceberg.s3.endpoint", null);
            if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
                props.put("s3.endpoint", s3Endpoint);
            }
            if (params.getBoolean("iceberg.s3.path.style.access", false)) {
                props.put("s3.path-style-access", "true");
            }
        }

        return props;
    }

    // ==================== Table List Builder ====================

    /**
     * Build fully qualified table list for Flink CDC MySqlSource.
     * Flink CDC requires each table in the format "database.table".
     *
     * <p>Handles three input patterns:
     * <ul>
     *   <li>Regex pattern (contains *, ?, [, +, ^, $, \): prefix with "database\\." for regex matching</li>
     *   <li>Pipe-separated list ("t1|t2|t3"): expand each to "database.t1,database.t2,database.t3"</li>
     *   <li>Single table ("orders"): prefix as "database.orders"</li>
     *   <li>Already qualified ("db.table"): pass through as-is</li>
     * </ul>
     */
    private static String buildQualifiedTableList(String database, String tables) {
        // If already contains dots (fully qualified), pass through
        // e.g. "norrisdb.orders,norrisdb.customers" or "norrisdb\\..*"
        if (tables.contains(".")) {
            return tables;
        }

        // Check if it's a regex pattern (contains regex metacharacters other than |)
        boolean isRegex = tables.matches(".*[*?\\[\\]+^$\\\\].*");

        if (isRegex) {
            // Regex mode: prefix with "database\\." for Debezium regex matching
            // e.g. ".*" → "norrisdb\\..*"
            // e.g. "user_.*" → "norrisdb\\.user_.*"
            return database + "\\\\." + tables;
        }

        // Pipe-separated or single table: split by | and qualify each
        // e.g. "t1|t2|t3" → "norrisdb.t1,norrisdb.t2,norrisdb.t3"
        // e.g. "orders" → "norrisdb.orders"
        String[] tableNames = tables.split("\\|");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tableNames.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            String t = tableNames[i].trim();
            sb.append(database).append(".").append(t);
        }
        return sb.toString();
    }
}
