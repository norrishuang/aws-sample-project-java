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
 * Flink CDC application for streaming data from MySQL to OpenSearch.
 * This application captures change data from MySQL using Flink CDC and sinks to OpenSearch.
 */
public class MySQLCDCToOpenSearch {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCToOpenSearch.class);

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
                LOG.info("Unable to load from KDA runtime properties, trying command line args for EMR: " + e.getMessage());
            }
            
            // Fallback to command line arguments (for EMR Flink)
            return ParameterTool.fromArgs(args);
        }
    }

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool applicationProperties = loadApplicationParameters(args, env);

        LOG.info("Starting MySQL CDC to OpenSearch pipeline");

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

                // Get MySQL configuration parameters (using underscore format for AWS Managed Flink)
                String mysqlHostname = applicationProperties.get("mysql.hostname", "<mysql-host>");
                String mysqlPort = applicationProperties.get("mysql.port", "3306");
                String mysqlUsername = applicationProperties.get("mysql.username", "admin");
                String mysqlPassword = applicationProperties.get("mysql.password", "<password>");
                String mysqlDatabase = applicationProperties.get("mysql.database", "norrisdb");
                String mysqlTable = applicationProperties.get("mysql.table", "user_order_list_20cols_new");
                String mysqlServerTimezone = applicationProperties.get("mysql_server_timezone", "UTC");

                // Get OpenSearch configuration parameters (using underscore format for AWS Managed Flink)
                String opensearchHost = applicationProperties.get("opensearch.host", 
                        "https://<host>:443");
                String opensearchIndex = applicationProperties.get("opensearch.index", "user_order_list_20cols_new");
                String opensearchUsername = applicationProperties.get("opensearch.username", "admin");
                String opensearchPassword = applicationProperties.get("opensearch.password", "ABCDEF");

                LOG.info("MySQL Source Configuration - Host: {}, Port: {}, Database: {}, Table: {}", 
                         mysqlHostname, mysqlPort, mysqlDatabase, mysqlTable);
                LOG.info("OpenSearch Sink Configuration - Host: {}, Index: {}", 
                         opensearchHost, opensearchIndex);

                // Create MySQL CDC Source Table with the specified schema
                final String mysqlSourceDDL = String.format(
                        "CREATE TABLE user_order_list_20cols_new (\n"
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
                                + "  'scan.incremental.snapshot.chunk.size' = '500',\n"
                                // + "  'scan.incremental.snapshot.unbounded-chunk-first.enabled' = 'true',\n"
                                + "  'scan.snapshot.fetch.size' = '2000',\n"
                                + "  'debezium.max.batch.size' = '1000',\n"
                                + "  'debezium.max.queue.size' = '1000',\n"
                                + "  'server-time-zone' = '%s'\n"
                                + ");",
                        mysqlHostname, mysqlPort, mysqlUsername, mysqlPassword,
                        mysqlDatabase, mysqlTable, mysqlServerTimezone);

                LOG.info("Creating MySQL CDC source table");
                streamTableEnvironment.executeSql(mysqlSourceDDL);

                // Create OpenSearch Sink Table with the specified schema
                final String opensearchSinkDDL = String.format(
                        "CREATE TABLE user_order_list_20cols_new_sink (\n"
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
                                + "  'connector' = 'opensearch-2',\n"
                                + "  'hosts' = '%s',\n"
                                + "  'index' = '%s',\n"
                                + "  'username' = '%s',\n"
                                + "  'password' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'sink.bulk-flush.max-actions' = '1000',\n"
                                + "  'sink.bulk-flush.interval' = '3s',\n"
                                + "  'sink.bulk-flush.max-size' = '5mb',\n"
                                + "  'sink.bulk-flush.backoff.strategy' = 'EXPONENTIAL',\n"
                                + "  'sink.bulk-flush.backoff.max-retries' = '20',\n"
                                + "  'sink.bulk-flush.backoff.delay' = '1000ms'\n"
                                + ");",
                        opensearchHost, opensearchIndex, opensearchUsername, opensearchPassword);

                LOG.info("Creating OpenSearch sink table");
                streamTableEnvironment.executeSql(opensearchSinkDDL);

                // Execute data pipeline from MySQL CDC to OpenSearch
                LOG.info("Starting data pipeline: MySQL CDC -> OpenSearch");
                streamTableEnvironment.executeSql(
                        "INSERT INTO user_order_list_20cols_new_sink SELECT * FROM user_order_list_20cols_new");

                LOG.info("Pipeline job submitted successfully");

            } catch (Exception e) {
                LOG.error("Error creating and deploying CDC pipeline job", e);
                throw new RuntimeException("Failed to create and deploy CDC pipeline", e);
            }
        }
    }
}
