package com.amazonaws.java.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements Iceberg's official {@link DynamicRecordGenerator} interface to convert
 * Debezium CDC JSON events into {@link DynamicRecord} objects.
 *
 * <p>This is the correct way to use Iceberg's Dynamic Sink API:
 * <ul>
 *   <li>The generator is plugged into {@code DynamicIcebergSink.Builder.generator()}</li>
 *   <li>The sink's internal {@code DynamicRecordProcessor} calls {@code generate()} for each input record</li>
 *   <li>Schema evolution is handled automatically by the Dynamic Sink — no manual schema management needed</li>
 *   <li>Table creation is handled automatically by the Dynamic Sink</li>
 *   <li>Table routing is done via {@code DynamicRecord.tableIdentifier()}</li>
 * </ul>
 *
 * <p>Key responsibilities of this generator:
 * <ol>
 *   <li>Parse Debezium CDC JSON events</li>
 *   <li>Extract source table name for dynamic routing (TableIdentifier)</li>
 *   <li>Build Iceberg Schema from Debezium schema metadata</li>
 *   <li>Convert JSON data to Flink RowData with correct RowKind</li>
 *   <li>Extract primary key info for equality fields (upsert mode)</li>
 * </ol>
 */
public class CDCDynamicRecordGenerator implements DynamicRecordGenerator<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CDCDynamicRecordGenerator.class);

    private final String namespace;
    private final String branch;
    private final boolean upsertEnabled;
    private final int writeParallelism;

    private transient ObjectMapper objectMapper;
    // Cache schemas per table for better Dynamic Sink cache hit rates.
    // Per Iceberg docs: reuse the same DynamicRecord.schema instance if unchanged.
    private transient Map<String, Schema> schemaCache;
    private transient Map<String, Set<String>> primaryKeyCache;

    /**
     * @param namespace        Iceberg namespace (e.g., "default" or "my_database")
     * @param branch           Optional Iceberg branch name (null for main branch)
     * @param upsertEnabled    Whether to enable upsert mode (requires primary key / equality fields)
     * @param writeParallelism Write parallelism per table; use Integer.MAX_VALUE for max available
     */
    public CDCDynamicRecordGenerator(String namespace, String branch,
                                     boolean upsertEnabled, int writeParallelism) {
        this.namespace = namespace;
        this.branch = branch;
        this.upsertEnabled = upsertEnabled;
        this.writeParallelism = writeParallelism;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper = new ObjectMapper();
        schemaCache = new ConcurrentHashMap<>();
        primaryKeyCache = new ConcurrentHashMap<>();
    }

    /**
     * Convert a Debezium CDC JSON string into zero or one DynamicRecord.
     * Non-data events (DDL, heartbeat, etc.) are silently skipped.
     */
    @Override
    public void generate(String jsonString, Collector<DynamicRecord> out) throws Exception {
        JsonNode rootNode = objectMapper.readTree(jsonString);

        // DEBUG: Log the first few characters of every incoming event and its top-level keys
        if (LOG.isInfoEnabled()) {
            String preview = jsonString.length() > 500 ? jsonString.substring(0, 500) + "..." : jsonString;
            LOG.info("[DEBUG-CDC] Received event (len={}), top-level keys: {}, preview: {}",
                    jsonString.length(), rootNode.fieldNames() != null ? iteratorToString(rootNode.fieldNames()) : "null", preview);
        }

        // Skip non-data events (DDL, schema changes, heartbeats).
        // Data change events from Debezium always contain an "op" field.
        if (!rootNode.has("op")) {
            LOG.info("[DEBUG-CDC] SKIPPED: no 'op' field found. Top-level keys: {}. This event is not a data change.",
                    iteratorToString(rootNode.fieldNames()));
            return;
        }

        String op = rootNode.get("op").asText();
        LOG.info("[DEBUG-CDC] op={}", op);

        // Get the data payload: "after" for insert/update/snapshot, "before" for delete
        JsonNode dataNode;
        if ("d".equals(op)) {
            dataNode = rootNode.get("before");
        } else {
            dataNode = rootNode.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            LOG.info("[DEBUG-CDC] SKIPPED: dataNode is null for op={}. 'before' keys: {}, 'after' keys: {}",
                    op,
                    rootNode.has("before") ? (rootNode.get("before").isNull() ? "null-value" : "present") : "missing",
                    rootNode.has("after") ? (rootNode.get("after").isNull() ? "null-value" : "present") : "missing");
            return;
        }

        // Extract source table name for dynamic routing
        String targetTableName = extractTargetTableName(rootNode);
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, targetTableName);
        LOG.info("[DEBUG-CDC] Target table: {}, fields in data: {}", tableIdentifier, iteratorToString(dataNode.fieldNames()));

        // Build Iceberg schema from Debezium schema metadata (or infer from values as fallback)
        JsonNode schemaNode = rootNode.get("schema");
        Schema schema = buildSchema(targetTableName, dataNode, schemaNode, op);
        LOG.info("[DEBUG-CDC] Schema built with {} columns for table {}", schema.columns().size(), targetTableName);

        // Convert JSON to Flink RowData with correct RowKind
        RowData rowData = convertToRowData(dataNode, schema, op);
        if (rowData == null) {
            LOG.info("[DEBUG-CDC] SKIPPED: convertToRowData returned null for op={}, table={}", op, targetTableName);
            return;
        }

        LOG.info("[DEBUG-CDC] Emitting DynamicRecord: table={}, op={}, schema_cols={}, rowKind={}",
                tableIdentifier, op, schema.columns().size(), rowData.getRowKind());

        // Create DynamicRecord — the core output for Dynamic Sink
        DynamicRecord record = new DynamicRecord(
                tableIdentifier,
                branch,                       // Optional branch (null = main)
                schema,                       // Schema (cached per table for performance)
                rowData,                      // The actual data row
                PartitionSpec.unpartitioned(), // Can be customized per table if needed
                DistributionMode.HASH,         // HASH recommended; RANGE not supported in Dynamic Sink
                writeParallelism              // Per-table write parallelism
        );

        // Set upsert mode and equality fields when enabled
        if (upsertEnabled) {
            record.setUpsertMode(true);
            Set<String> primaryKeys = primaryKeyCache.get(targetTableName);
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                record.setEqualityFields(primaryKeys);
            }
        }

        out.collect(record);
    }

    // ==================== Table Name Extraction ====================

    /**
     * Extract target Iceberg table name from the Debezium "source" field.
     * Each MySQL source table maps to its own Iceberg table via Dynamic Sink routing.
     */
    private String extractTargetTableName(JsonNode rootNode) {
        JsonNode sourceNode = rootNode.get("source");
        if (sourceNode != null && sourceNode.has("table")) {
            return sourceNode.get("table").asText();
        }
        LOG.warn("No source.table found in CDC event, using 'unknown_table' as fallback");
        return "unknown_table";
    }

    // ==================== Schema Building ====================

    /**
     * Build Iceberg Schema from CDC event.
     * Prefers Debezium schema metadata for accurate types; falls back to JSON value inference.
     * Schemas are cached per table name to improve Dynamic Sink cache hit rates.
     */
    private Schema buildSchema(String tableName, JsonNode dataNode,
                               JsonNode schemaNode, String op) {
        if (schemaNode != null) {
            Schema debeziumSchema = buildSchemaFromDebezium(tableName, schemaNode, op);
            if (debeziumSchema != null) {
                return debeziumSchema;
            }
        }
        return buildSchemaFromValues(tableName, dataNode);
    }

    /**
     * Build schema from Debezium's schema metadata (accurate types from MySQL information_schema).
     * Also extracts primary key information for upsert equality fields.
     */
    private Schema buildSchemaFromDebezium(String tableName, JsonNode schemaNode, String op) {
        try {
            JsonNode fieldsArray = schemaNode.get("fields");
            if (fieldsArray == null || !fieldsArray.isArray()) {
                return null;
            }

            // Find the "after" (or "before" for deletes) field schema in the Debezium envelope
            String targetField = "d".equals(op) ? "before" : "after";
            JsonNode valueSchemaNode = null;
            for (JsonNode field : fieldsArray) {
                if (targetField.equals(field.path("field").asText())) {
                    valueSchemaNode = field;
                    break;
                }
            }

            if (valueSchemaNode == null || !valueSchemaNode.has("fields")) {
                return null;
            }

            JsonNode columnFields = valueSchemaNode.get("fields");
            if (columnFields == null || !columnFields.isArray()) {
                return null;
            }

            // Also extract primary keys from the key schema if present
            extractPrimaryKeys(tableName, schemaNode);

            List<Types.NestedField> icebergFields = new ArrayList<>();
            int fieldId = 1;

            for (JsonNode columnField : columnFields) {
                String fieldName = columnField.get("field").asText();
                String fieldType = columnField.has("type") ? columnField.get("type").asText() : "string";
                boolean optional = !columnField.has("optional") || columnField.get("optional").asBoolean(true);
                String logicalName = columnField.has("name") ? columnField.get("name").asText() : null;

                org.apache.iceberg.types.Type icebergType = mapDebeziumType(
                        fieldType, logicalName, columnField.get("parameters"));

                if (optional) {
                    icebergFields.add(Types.NestedField.optional(fieldId++, fieldName, icebergType));
                } else {
                    icebergFields.add(Types.NestedField.required(fieldId++, fieldName, icebergType));
                }
            }

            Schema schema = new Schema(icebergFields);

            // Reuse cached schema instance if unchanged (better Dynamic Sink cache performance)
            Schema cached = schemaCache.get(tableName);
            if (cached != null && cached.sameSchema(schema)) {
                return cached;
            }
            schemaCache.put(tableName, schema);
            return schema;

        } catch (Exception e) {
            LOG.warn("Failed to parse Debezium schema for table {}, falling back to value inference: {}",
                    tableName, e.getMessage());
            return null;
        }
    }

    /**
     * Extract primary key fields from Debezium's key schema.
     * The key schema is typically the first "fields" entry with field name matching the key.
     * For Debezium MySQL connector, the primary key columns appear in the key schema.
     */
    private void extractPrimaryKeys(String tableName, JsonNode schemaNode) {
        if (primaryKeyCache.containsKey(tableName)) {
            return; // Already extracted
        }

        try {
            // Debezium's envelope schema: look for "before"/"after" fields and check which
            // columns are marked as non-optional (required) — these are typically PKs.
            // A more reliable approach: if the Debezium message has a "key" field at root level,
            // the key field names are the primary keys.
            JsonNode fieldsArray = schemaNode.get("fields");
            if (fieldsArray == null) {
                return;
            }

            // Strategy: In Debezium CDC events with includeSchema=true, the key schema
            // has fields that correspond to the primary key columns.
            // We look for the "before" or "after" struct and identify required fields.
            for (JsonNode field : fieldsArray) {
                String fieldName = field.path("field").asText();
                if ("after".equals(fieldName) || "before".equals(fieldName)) {
                    JsonNode columns = field.get("fields");
                    if (columns != null && columns.isArray()) {
                        Set<String> pks = new LinkedHashSet<>();
                        for (JsonNode col : columns) {
                            // Required (non-optional) fields in Debezium schema are typically PKs
                            // This is a heuristic; in practice all PK columns are required
                            boolean optional = !col.has("optional") || col.get("optional").asBoolean(true);
                            if (!optional) {
                                pks.add(col.get("field").asText());
                            }
                        }
                        if (!pks.isEmpty()) {
                            primaryKeyCache.put(tableName, pks);
                            LOG.info("Detected primary keys for table {}: {}", tableName, pks);
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not extract primary keys for table {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * Map Debezium schema types to Iceberg types.
     * Reference: https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
     */
    private org.apache.iceberg.types.Type mapDebeziumType(String debeziumType,
                                                          String logicalName,
                                                          JsonNode parameters) {
        // Check logical type first (Debezium semantic types)
        if (logicalName != null) {
            switch (logicalName) {
                case "io.debezium.time.Timestamp":
                case "io.debezium.time.MicroTimestamp":
                case "io.debezium.time.NanoTimestamp":
                case "org.apache.kafka.connect.data.Timestamp":
                    return Types.TimestampType.withoutZone();
                case "io.debezium.time.ZonedTimestamp":
                    return Types.TimestampType.withZone();
                case "io.debezium.time.Date":
                case "org.apache.kafka.connect.data.Date":
                    return Types.DateType.get();
                case "io.debezium.time.Time":
                case "io.debezium.time.MicroTime":
                case "io.debezium.time.NanoTime":
                case "org.apache.kafka.connect.data.Time":
                    return Types.TimeType.get();
                case "org.apache.kafka.connect.data.Decimal":
                    int precision = 38;
                    int scale = 18;
                    if (parameters != null) {
                        if (parameters.has("scale")) {
                            scale = parameters.get("scale").asInt(18);
                        }
                        if (parameters.has("connect.decimal.precision")) {
                            precision = parameters.get("connect.decimal.precision").asInt(38);
                        }
                    }
                    return Types.DecimalType.of(precision, scale);
                default:
                    break;
            }
        }

        // Map base Debezium/Kafka Connect types
        switch (debeziumType) {
            case "boolean":
                return Types.BooleanType.get();
            case "int8":
            case "int16":
            case "int32":
                return Types.IntegerType.get();
            case "int64":
                return Types.LongType.get();
            case "float32":
                return Types.FloatType.get();
            case "float64":
                return Types.DoubleType.get();
            case "bytes":
                return Types.BinaryType.get();
            case "string":
            default:
                return Types.StringType.get();
        }
    }

    /**
     * Fallback: Build schema by inferring types from JSON values.
     * Less accurate than Debezium schema but works when schema metadata is unavailable.
     */
    private Schema buildSchemaFromValues(String tableName, JsonNode dataNode) {
        Schema cached = schemaCache.get(tableName);

        Set<String> currentFieldNames = new LinkedHashSet<>();
        Iterator<Map.Entry<String, JsonNode>> it = dataNode.fields();
        while (it.hasNext()) {
            currentFieldNames.add(it.next().getKey());
        }

        // Reuse cached schema if field names match
        if (cached != null) {
            Set<String> cachedFieldNames = new LinkedHashSet<>();
            for (Types.NestedField f : cached.columns()) {
                cachedFieldNames.add(f.name());
            }
            if (cachedFieldNames.equals(currentFieldNames)) {
                return cached;
            }
        }

        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 1;
        Iterator<Map.Entry<String, JsonNode>> fieldIterator = dataNode.fields();
        while (fieldIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldIterator.next();
            String fieldName = field.getKey();
            JsonNode valueNode = field.getValue();

            org.apache.iceberg.types.Type fieldType;
            if (cached != null && cached.findField(fieldName) != null) {
                fieldType = cached.findField(fieldName).type();
            } else {
                fieldType = inferIcebergType(valueNode);
            }

            fields.add(Types.NestedField.optional(fieldId++, fieldName, fieldType));
        }

        Schema newSchema = new Schema(fields);
        schemaCache.put(tableName, newSchema);
        return newSchema;
    }

    private org.apache.iceberg.types.Type inferIcebergType(JsonNode node) {
        if (node == null || node.isNull()) {
            return Types.StringType.get();
        }
        if (node.isBoolean()) {
            return Types.BooleanType.get();
        } else if (node.isInt()) {
            return Types.IntegerType.get();
        } else if (node.isLong()) {
            return Types.LongType.get();
        } else if (node.isDouble() || node.isFloat()) {
            return Types.DoubleType.get();
        } else if (node.isBigDecimal()) {
            return Types.DecimalType.of(38, 18);
        } else {
            return Types.StringType.get();
        }
    }

    // ==================== Data Conversion ====================

    /**
     * Convert JSON data to Flink RowData with correct RowKind based on CDC operation.
     */
    private RowData convertToRowData(JsonNode dataNode, Schema schema, String operation) {
        List<Types.NestedField> fields = schema.columns();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            JsonNode valueNode = dataNode.get(field.name());
            rowData.setField(i, convertValue(valueNode, field.type()));
        }

        // Set RowKind based on CDC operation
        switch (operation) {
            case "c": // create (insert)
            case "r": // read (snapshot)
                rowData.setRowKind(RowKind.INSERT);
                break;
            case "u": // update
                rowData.setRowKind(RowKind.UPDATE_AFTER);
                break;
            case "d": // delete
                rowData.setRowKind(RowKind.DELETE);
                break;
            default:
                rowData.setRowKind(RowKind.INSERT);
        }

        return rowData;
    }

    /**
     * Convert a JSON value to the Flink internal format corresponding to the Iceberg type.
     */
    private Object convertValue(JsonNode node, org.apache.iceberg.types.Type type) {
        if (node == null || node.isNull()) {
            return null;
        }

        try {
            if (type instanceof Types.BooleanType) {
                return node.asBoolean();
            } else if (type instanceof Types.IntegerType) {
                return node.asInt();
            } else if (type instanceof Types.LongType) {
                return node.asLong();
            } else if (type instanceof Types.FloatType) {
                return (float) node.asDouble();
            } else if (type instanceof Types.DoubleType) {
                return node.asDouble();
            } else if (type instanceof Types.StringType) {
                return StringData.fromString(node.asText());
            } else if (type instanceof Types.TimestampType) {
                if (node.isLong() || node.isInt()) {
                    long timestamp = node.asLong();
                    // Debezium MicroTimestamp: microseconds since epoch
                    if (timestamp > 1_000_000_000_000_000L) {
                        long millis = timestamp / 1000;
                        int nanoOfMilli = (int) ((timestamp % 1000) * 1000);
                        return TimestampData.fromEpochMillis(millis, nanoOfMilli);
                    }
                    // Debezium Timestamp: milliseconds since epoch
                    return TimestampData.fromEpochMillis(timestamp);
                } else {
                    return TimestampData.fromEpochMillis(
                            java.time.Instant.parse(node.asText()).toEpochMilli());
                }
            } else if (type instanceof Types.DateType) {
                // Debezium Date: days since epoch
                return node.asInt();
            } else if (type instanceof Types.TimeType) {
                // Debezium Time: milliseconds past midnight (or microseconds for MicroTime)
                return node.asLong();
            } else if (type instanceof Types.DecimalType) {
                BigDecimal decimal = new BigDecimal(node.asText());
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return DecimalData.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
            } else if (type instanceof Types.BinaryType) {
                return node.asText().getBytes();
            } else {
                return StringData.fromString(node.asText());
            }
        } catch (Exception e) {
            LOG.warn("Error converting value for field type {}: {}, falling back to string",
                    type, e.getMessage());
            return StringData.fromString(node.asText());
        }
    }

    // ==================== Utilities ====================

    private static String iteratorToString(java.util.Iterator<String> it) {
        if (it == null) return "null";
        StringBuilder sb = new StringBuilder("[");
        while (it.hasNext()) {
            if (sb.length() > 1) sb.append(", ");
            sb.append(it.next());
        }
        sb.append("]");
        return sb.toString();
    }
}
