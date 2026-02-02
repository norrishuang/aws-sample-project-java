package com.amazonaws.java.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.DistributionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Converts CDC JSON events to DynamicRecord for Iceberg Dynamic Sink.
 * 
 * DynamicRecord contains:
 * - TableIdentifier: Target table
 * - Schema: Record schema (extracted from CDC event)
 * - RowData: Actual data
 * - PartitionSpec: Partitioning specification
 * - DistributionMode: How to distribute data
 * 
 * The Dynamic Sink will automatically:
 * - Create tables if they don't exist
 * - Evolve schema when new fields are detected (with immediateTableUpdate=true)
 * - Handle INSERT/UPDATE/DELETE operations
 */
public class CDCToDynamicRecordConverter extends ProcessFunction<String, DynamicRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCToDynamicRecordConverter.class);
    
    private final String namespace;
    private final String tableName;
    private final String branch;
    private transient ObjectMapper objectMapper;
    private transient TableIdentifier tableIdentifier;
    
    public CDCToDynamicRecordConverter(String namespace, String tableName, String branch) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.branch = branch;
    }

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
        tableIdentifier = TableIdentifier.of(namespace, tableName);
    }

    @Override
    public void processElement(String jsonString, Context ctx, Collector<DynamicRecord> out) throws Exception {
        JsonNode rootNode = objectMapper.readTree(jsonString);
        
        // Skip schema change events - Dynamic Sink handles schema evolution automatically
        if (rootNode.has("schema") && !rootNode.has("payload")) {
            LOG.debug("Skipping schema change event - Dynamic Sink will handle it");
            return;
        }
        
        // Get operation type (c=create/insert, u=update, d=delete, r=read/snapshot)
        String op = rootNode.has("op") ? rootNode.get("op").asText() : "r";
        
        // Get the data payload
        JsonNode dataNode;
        if ("d".equals(op)) {
            dataNode = rootNode.get("before");
        } else {
            dataNode = rootNode.get("after");
        }
        
        if (dataNode == null || dataNode.isNull()) {
            LOG.debug("No data found in CDC event, skipping");
            return;
        }

        // Extract schema from data
        Schema schema = extractSchema(dataNode);
        
        // Convert to RowData
        RowData rowData = convertToRowData(dataNode, schema, op);
        
        if (rowData != null) {
            // Create DynamicRecord
            // The Dynamic Sink will automatically handle schema evolution
            DynamicRecord dynamicRecord = new DynamicRecord(
                    tableIdentifier,
                    branch,  // Optional branch name
                    schema,  // Schema extracted from CDC event
                    rowData,  // Actual data
                    PartitionSpec.unpartitioned(),  // Can be customized based on requirements
                    DistributionMode.HASH,  // Use HASH distribution
                    2  // Write parallelism per table
            );
            
            out.collect(dynamicRecord);
        }
    }

    /**
     * Extract Iceberg Schema from CDC JSON data.
     * This schema will be used by Dynamic Sink to create/evolve the table.
     */
    private Schema extractSchema(JsonNode dataNode) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 1;
        
        Iterator<Map.Entry<String, JsonNode>> fieldIterator = dataNode.fields();
        while (fieldIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldIterator.next();
            String fieldName = field.getKey();
            JsonNode valueNode = field.getValue();
            
            Types.NestedField nestedField = Types.NestedField.optional(
                    fieldId++,
                    fieldName,
                    inferIcebergType(valueNode)
            );
            fields.add(nestedField);
        }
        
        return new Schema(fields);
    }

    /**
     * Infer Iceberg type from JSON value.
     */
    private org.apache.iceberg.types.Type inferIcebergType(JsonNode node) {
        if (node == null || node.isNull()) {
            return Types.StringType.get();  // Default to string for null values
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

    /**
     * Convert JSON data to Flink RowData.
     */
    private RowData convertToRowData(JsonNode dataNode, Schema schema, String operation) {
        List<Types.NestedField> fields = schema.columns();
        GenericRowData rowData = new GenericRowData(fields.size());
        
        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            JsonNode valueNode = dataNode.get(field.name());
            
            Object value = convertValue(valueNode, field.type());
            rowData.setField(i, value);
        }
        
        // Set row kind based on operation
        switch (operation) {
            case "c":
            case "r":
                rowData.setRowKind(RowKind.INSERT);
                break;
            case "u":
                rowData.setRowKind(RowKind.UPDATE_AFTER);
                break;
            case "d":
                rowData.setRowKind(RowKind.DELETE);
                break;
            default:
                rowData.setRowKind(RowKind.INSERT);
        }
        
        return rowData;
    }

    /**
     * Convert JSON value to Flink internal format based on Iceberg type.
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
                long timestamp = node.asLong();
                return TimestampData.fromEpochMillis(timestamp);
            } else if (type instanceof Types.DecimalType) {
                BigDecimal decimal = new BigDecimal(node.asText());
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return DecimalData.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
            } else {
                // Default to string
                return StringData.fromString(node.asText());
            }
        } catch (Exception e) {
            LOG.warn("Error converting value for type {}: {}", type, e.getMessage());
            return StringData.fromString(node.asText());
        }
    }
}
