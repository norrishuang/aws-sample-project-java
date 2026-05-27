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
import org.apache.iceberg.SnapshotRef;
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
 * Implements {@link DynamicRecordGenerator} for plain business JSON messages from Kafka.
 * Unlike CDCDynamicRecordGenerator, this handles non-Debezium JSON (no op/before/after wrapper).
 * All messages are treated as INSERT operations.
 */
public class KafkaJsonDynamicRecordGenerator implements DynamicRecordGenerator<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDynamicRecordGenerator.class);

    private final String namespace;
    private final String fixedTableName;
    private final String routingField;
    private final String schemaDefinition;
    private final String branch;
    private final int writeParallelism;

    private transient ObjectMapper objectMapper;
    private transient Map<String, Schema> schemaCache;
    private transient Schema predefinedSchema;

    /**
     * @param namespace        Iceberg namespace
     * @param fixedTableName   Fixed table name (FIXED mode) or fallback table name (FIELD mode)
     * @param routingField     FIELD mode: JSON field whose value becomes the table name; null = FIXED mode
     * @param schemaDefinition Optional predefined schema string "field1:type1,field2:type2,..."; null = infer
     * @param branch           Optional Iceberg branch name; null for main branch
     * @param writeParallelism Write parallelism per table
     */
    public KafkaJsonDynamicRecordGenerator(
            String namespace,
            String fixedTableName,
            String routingField,
            String schemaDefinition,
            String branch,
            int writeParallelism) {
        this.namespace = namespace;
        this.fixedTableName = fixedTableName;
        this.routingField = routingField;
        this.schemaDefinition = schemaDefinition;
        this.branch = (branch != null) ? branch : SnapshotRef.MAIN_BRANCH;
        this.writeParallelism = writeParallelism;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper = new ObjectMapper();
        schemaCache = new ConcurrentHashMap<>();
        if (schemaDefinition != null && !schemaDefinition.isEmpty()) {
            predefinedSchema = parseSchemaDefinition(schemaDefinition);
        }
    }

    @Override
    public void generate(String jsonString, Collector<DynamicRecord> out) throws Exception {
        JsonNode rootNode = objectMapper.readTree(jsonString);
        if (rootNode == null || !rootNode.isObject()) {
            LOG.warn("Skipping non-object JSON: {}", jsonString);
            return;
        }

        // Table routing
        String tableName = resolveTableName(rootNode);
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        // Schema resolution
        Schema schema = resolveSchema(tableName, rootNode);

        // Convert to RowData (all INSERT)
        RowData rowData = convertToRowData(rootNode, schema);

        DynamicRecord record = new DynamicRecord(
                tableIdentifier, branch, schema, rowData,
                PartitionSpec.unpartitioned(), DistributionMode.HASH, writeParallelism);
        out.collect(record);
    }

    // ==================== Table Routing ====================

    private String resolveTableName(JsonNode rootNode) {
        if (routingField != null && !routingField.isEmpty()) {
            JsonNode fieldNode = rootNode.get(routingField);
            if (fieldNode != null && !fieldNode.isNull() && fieldNode.isTextual()) {
                String value = fieldNode.asText().trim();
                if (!value.isEmpty()) {
                    return value;
                }
            }
            // Fallback to fixed table name
        }
        return fixedTableName;
    }

    // ==================== Schema Resolution ====================

    private Schema resolveSchema(String tableName, JsonNode dataNode) {
        if (predefinedSchema != null) {
            return predefinedSchema;
        }
        return buildSchemaFromValues(tableName, dataNode);
    }

    /**
     * Build schema by inferring types from JSON values. Cached per table name.
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
        } else {
            return Types.StringType.get();
        }
    }

    // ==================== Predefined Schema Parsing ====================

    /**
     * Parse schema definition string: "field1:type1,field2:type2,..."
     * Supported types: string, int, long, double, boolean, timestamp, date, decimal(p,s)
     */
    private Schema parseSchemaDefinition(String definition) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 1;
        String[] parts = definition.split(",");
        for (String part : parts) {
            String[] kv = part.trim().split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid schema definition part: " + part);
            }
            String fieldName = kv[0].trim();
            String typeName = kv[1].trim().toLowerCase();
            org.apache.iceberg.types.Type icebergType = parseType(typeName);
            fields.add(Types.NestedField.optional(fieldId++, fieldName, icebergType));
        }
        return new Schema(fields);
    }

    private org.apache.iceberg.types.Type parseType(String typeName) {
        if (typeName.startsWith("decimal")) {
            // decimal(p,s)
            String params = typeName.substring(typeName.indexOf('(') + 1, typeName.indexOf(')'));
            String[] ps = params.split(",");
            int precision = Integer.parseInt(ps[0].trim());
            int scale = Integer.parseInt(ps[1].trim());
            return Types.DecimalType.of(precision, scale);
        }
        switch (typeName) {
            case "string":
                return Types.StringType.get();
            case "int":
            case "integer":
                return Types.IntegerType.get();
            case "long":
                return Types.LongType.get();
            case "double":
            case "float":
                return Types.DoubleType.get();
            case "boolean":
                return Types.BooleanType.get();
            case "timestamp":
                return Types.TimestampType.withoutZone();
            case "date":
                return Types.DateType.get();
            default:
                return Types.StringType.get();
        }
    }

    // ==================== Data Conversion ====================

    private RowData convertToRowData(JsonNode dataNode, Schema schema) {
        List<Types.NestedField> fields = schema.columns();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            JsonNode valueNode = dataNode.get(field.name());
            rowData.setField(i, convertValue(valueNode, field.type()));
        }

        rowData.setRowKind(RowKind.INSERT);
        return rowData;
    }

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
                    return TimestampData.fromEpochMillis(node.asLong());
                }
                return TimestampData.fromEpochMillis(
                        java.time.Instant.parse(node.asText()).toEpochMilli());
            } else if (type instanceof Types.DateType) {
                return node.asInt();
            } else if (type instanceof Types.DecimalType) {
                BigDecimal decimal = new BigDecimal(node.asText());
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return DecimalData.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
            } else {
                return StringData.fromString(node.asText());
            }
        } catch (Exception e) {
            LOG.warn("Error converting value for type {}: {}", type, e.getMessage());
            return StringData.fromString(node.asText());
        }
    }
}
