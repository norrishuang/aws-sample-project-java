package com.amazonaws.java.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink CDC application for streaming data from MySQL to OpenSearch.
 * This application captures change data from MySQL using Flink CDC and sinks to OpenSearch.
 */
public class MySQLCDCToOpenSearch {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCToOpenSearch.class);

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool applicationProperties = ParameterTool.fromArgs(args);

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
                configuration.setString("execution.checkpointing.interval", "1 min");

                // Get configuration parameters (with defaults for testing)
                String mysqlHostname = applicationProperties.get(
                        "mysql.hostname", 
                        "mysql-cdc-db.cghfgy0zyjlk.us-east-1.rds.amazonaws.com");
                String mysqlPort = applicationProperties.get("mysql.port", "3306");
                String mysqlUsername = applicationProperties.get("mysql.username", "admin");
                String mysqlPassword = applicationProperties.get("mysql.password", "Amazon123");
                String mysqlDatabase = applicationProperties.get("mysql.database", "norrisdb");
                String mysqlTable = applicationProperties.get("mysql.table", "user_order_list");
                String mysqlServerTimezone = applicationProperties.get("mysql.server-timezone", "UTC");

                String opensearchHost = applicationProperties.get(
                        "opensearch.host", 
                        "https://search-es-beg-test-bvu7xxqngc6kvt3tyh2rgqywk4.us-east-1.es.amazonaws.com:443");
                String opensearchIndex = applicationProperties.get("opensearch.index", "user_order_list");
                String opensearchUsername = applicationProperties.get("opensearch.username", "admin");
                String opensearchPassword = applicationProperties.get("opensearch.password", "C6VxZyep)8>2");

                LOG.info("MySQL Source Configuration - Host: {}, Port: {}, Database: {}, Table: {}", 
                         mysqlHostname, mysqlPort, mysqlDatabase, mysqlTable);
                LOG.info("OpenSearch Sink Configuration - Host: {}, Index: {}", 
                         opensearchHost, opensearchIndex);

                // Create MySQL CDC Source Table
                final String mysqlSourceDDL = String.format(
                        "CREATE TABLE user_order_list (\n"
                                + "  id INT,\n"
                                + "  uuid STRING,\n"
                                + "  user_name STRING,\n"
                                + "  phone_number STRING,\n"
                                + "  product_id INT,\n"
                                + "  product_name STRING,\n"
                                + "  product_type STRING,\n"
                                + "  manufacturing_date INT,\n"
                                + "  price FLOAT,\n"
                                + "  unit INT,\n"
                                + "  created_at TIMESTAMP(3),\n"
                                + "  updated_at TIMESTAMP(3),\n"
                                + "  test1 INT,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'mysql-cdc',\n"
                                + "  'hostname' = '%s',\n"
                                + "  'port' = '%s',\n"
                                + "  'username' = '%s',\n"
                                + "  'password' = '%s',\n"
                                + "  'database-name' = '%s',\n"
                                + "  'table-name' = '%s',\n"
                                + "  'server-time-zone' = '%s'\n"
                                + ");",
                        mysqlHostname, mysqlPort, mysqlUsername, mysqlPassword,
                        mysqlDatabase, mysqlTable, mysqlServerTimezone);

                LOG.info("Creating MySQL CDC source table");
                streamTableEnvironment.executeSql(mysqlSourceDDL);

                // Create OpenSearch Sink Table using elasticsearch-7 connector for OpenSearch compatibility
                final String opensearchSinkDDL = String.format(
                        "CREATE TABLE user_order_list_sink (\n"
                                + "  id INT,\n"
                                + "  uuid STRING,\n"
                                + "  user_name STRING,\n"
                                + "  phone_number STRING,\n"
                                + "  product_id INT,\n"
                                + "  product_name STRING,\n"
                                + "  product_type STRING,\n"
                                + "  manufacturing_date INT,\n"
                                + "  price FLOAT,\n"
                                + "  unit INT,\n"
                                + "  created_at TIMESTAMP(3),\n"
                                + "  updated_at TIMESTAMP(3),\n"
                                + "  test1 INT,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'elasticsearch-7',\n"
                                + "  'hosts' = '%s',\n"
                                + "  'index' = '%s',\n"
                                + "  'username' = '%s',\n"
                                + "  'password' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'sink.bulk-flush.max-actions' = '1000',\n"
                                + "  'sink.bulk-flush.max-size' = '5mb',\n"
                                + "  'sink.bulk-flush.interval' = '5s',\n"
                                + "  'sink.bulk-flush.backoff.strategy' = 'EXPONENTIAL',\n"
                                + "  'sink.bulk-flush.backoff.max-retries' = '3',\n"
                                + "  'sink.bulk-flush.backoff.delay' = '1s'\n"
                                + ");",
                        opensearchHost, opensearchIndex, opensearchUsername, opensearchPassword);

                LOG.info("Creating OpenSearch sink table");
                streamTableEnvironment.executeSql(opensearchSinkDDL);

                // Execute data pipeline from MySQL CDC to OpenSearch
                LOG.info("Starting data pipeline: MySQL CDC -> OpenSearch");
                streamTableEnvironment.executeSql(
                        "INSERT INTO user_order_list_sink SELECT * FROM user_order_list");

                LOG.info("Pipeline job submitted successfully");

            } catch (Exception e) {
                LOG.error("Error creating and deploying CDC pipeline job", e);
                throw new RuntimeException("Failed to create and deploy CDC pipeline", e);
            }
        }
    }
}
