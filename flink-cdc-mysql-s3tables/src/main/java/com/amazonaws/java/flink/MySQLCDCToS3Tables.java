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
 * Flink CDC application for streaming data from MySQL to AWS S3 Tables (Iceberg with REST Catalog).
 * This application captures change data from MySQL using Flink CDC and sinks to S3 Tables.
 */
public class MySQLCDCToS3Tables {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCToS3Tables.class);

    private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args);
        } else {
            // Try to load from KDA runtime properties first (for Managed Flink)
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
            
            // Fallback to command line arguments
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool applicationProperties = loadApplicationParameters(args, env);

        LOG.info("Starting MySQL CDC to S3 Tables pipeline");

        // Process stream using SQL API
        CDCPipeline.createAndDeployJob(env, applicationProperties);
    }

    public static class CDCPipeline {

        public static void createAndDeployJob(
                StreamExecutionEnvironment env, ParameterTool applicationProperties) {
            try {
                // Create Table Environment
                StreamTableEnvironment streamTableEnvironment =
                        StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

                // Configure checkpointing
                Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
                configuration.setString("execution.checkpointing.interval", "5 min");

                // Get MySQL configuration parameters
                String mysqlHostname = applicationProperties.get("mysql.hostname", "<mysql-host>");
                String mysqlPort = applicationProperties.get("mysql.port", "3306");
                String mysqlUsername = applicationProperties.get("mysql.username", "admin");
                String mysqlPassword = applicationProperties.get("mysql.password", "<password>");
                String mysqlDatabase = applicationProperties.get("mysql.database", "testdb");
                String mysqlTable = applicationProperties.get("mysql.table", "user_order_list");
                String mysqlServerTimezone = applicationProperties.get("mysql.server.timezone", "UTC");

                // Get S3 Tables / Iceberg REST Catalog configuration
                String restCatalogUri = applicationProperties.get("iceberg.rest.uri", 
                        "https://s3tables.us-east-1.amazonaws.com/iceberg");
                String warehouseArn = applicationProperties.get("iceberg.warehouse.arn", 
                        "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket");
                String namespace = applicationProperties.get("iceberg.namespace", "my_namespace");
                String tableName = applicationProperties.get("iceberg.table.name", "user_order_list_s3tables");
                String awsRegion = applicationProperties.get("aws.region", "us-east-1");

                LOG.info("MySQL Source Configuration - Host: {}, Port: {}, Database: {}, Table: {}", 
                         mysqlHostname, mysqlPort, mysqlDatabase, mysqlTable);
                LOG.info("S3 Tables Configuration - REST URI: {}, Warehouse ARN: {}, Namespace: {}, Table: {}", 
                         restCatalogUri, warehouseArn, namespace, tableName);

                // Create Iceberg Catalog with REST Catalog for S3 Tables
                // S3 Tables requires SigV4 authentication
                // IMPORTANT: Must specify 'catalog-type' = 'rest' to use REST catalog
                final String icebergCatalog = String.format(
                        "CREATE CATALOG s3tables_catalog WITH (\n"
                                + "  'type' = 'iceberg',\n"
                                + "  'catalog-type' = 'rest',\n"
                                + "  'uri' = '%s',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n"
                                + "  'rest.sigv4-enabled' = 'true',\n"
                                + "  'rest.signing-name' = 's3tables',\n"
                                + "  'rest.signing-region' = '%s',\n"
                                + "  'client.region' = '%s'\n"
                                + ");",
                        restCatalogUri, warehouseArn, awsRegion, awsRegion);

                LOG.info("Creating Iceberg catalog with REST endpoint");
                streamTableEnvironment.executeSql(icebergCatalog);

                // Create MySQL CDC Source Table
                final String mysqlSourceDDL = String.format(
                        "CREATE TABLE mysql_source (\n"
                                + "  id INT,\n"
                                + "  uuid STRING,\n"
                                + "  user_name STRING,\n"
                                + "  phone_number STRING,\n"
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
                                + "  status STRING,\n"
                                + "  created_date TIMESTAMP,\n"
                                + "  last_login TIMESTAMP,\n"
                                + "  score INT,\n"
                                + "  description STRING,\n"
                                + "  created_at TIMESTAMP,\n"
                                + "  updated_at TIMESTAMP,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'mysql-cdc',\n"
                                + "  'hostname' = '%s',\n"
                                + "  'port' = '%s',\n"
                                + "  'username' = '%s',\n"
                                + "  'password' = '%s',\n"
                                + "  'database-name' = '%s',\n"
                                + "  'table-name' = '%s',\n"
                                + "  'scan.incremental.snapshot.enabled' = 'true',\n"
                                + "  'scan.incremental.snapshot.chunk.size' = '8192',\n"
                                + "  'scan.snapshot.fetch.size' = '2000',\n"
                                + "  'debezium.max.batch.size' = '1000',\n"
                                + "  'debezium.max.queue.size' = '1000',\n"
                                + "  'server-time-zone' = '%s'\n"
                                + ");",
                        mysqlHostname, mysqlPort, mysqlUsername, mysqlPassword,
                        mysqlDatabase, mysqlTable, mysqlServerTimezone);

                LOG.info("Creating MySQL CDC source table");
                streamTableEnvironment.executeSql(mysqlSourceDDL);

                // Create Iceberg Sink Table in S3 Tables
                final String icebergSinkDDL = String.format(
                        "CREATE TABLE IF NOT EXISTS s3tables_catalog.%s.%s (\n"
                                + "  id INT,\n"
                                + "  uuid STRING,\n"
                                + "  user_name STRING,\n"
                                + "  phone_number STRING,\n"
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
                                + "  status STRING,\n"
                                + "  created_date TIMESTAMP,\n"
                                + "  last_login TIMESTAMP,\n"
                                + "  score INT,\n"
                                + "  description STRING,\n"
                                + "  created_at TIMESTAMP,\n"
                                + "  updated_at TIMESTAMP,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'type' = 'iceberg',\n"
                                + "  'catalog-name' = 's3tables_catalog',\n"
                                + "  'write.metadata.delete-after-commit.enabled' = 'true',\n"
                                + "  'write.metadata.previous-versions-max' = '5',\n"
                                + "  'format-version' = '2'\n"
                                + ");",
                        namespace, tableName);

                LOG.info("Creating Iceberg sink table in S3 Tables");
                streamTableEnvironment.executeSql(icebergSinkDDL);

                // Execute data pipeline from MySQL CDC to S3 Tables
                LOG.info("Starting data pipeline: MySQL CDC -> S3 Tables (Iceberg)");
                streamTableEnvironment.executeSql(
                        String.format("INSERT INTO s3tables_catalog.%s.%s SELECT * FROM mysql_source",
                                namespace, tableName));

                LOG.info("Pipeline job submitted successfully");

            } catch (Exception e) {
                LOG.error("Error creating and deploying CDC pipeline job", e);
                throw new RuntimeException("Failed to create and deploy CDC pipeline", e);
            }
        }
    }
}
