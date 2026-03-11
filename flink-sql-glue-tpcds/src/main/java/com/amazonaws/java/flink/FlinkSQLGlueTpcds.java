package com.amazonaws.java.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SQL Job: Create Glue Catalog and query tpcds.call_center table.
 *
 * This job runs on EMR 7.12 (Flink 1.20). It:
 * 1. Creates a HiveCatalog backed by AWS Glue Data Catalog
 * 2. Switches to the tpcds database
 * 3. Executes SELECT on the call_center table and prints results
 *
 * Prerequisites:
 * - EMR cluster with Flink + Glue Catalog enabled
 *   (use-glue-catalog classification = true)
 * - tpcds database and call_center table already exist in Glue Data Catalog
 *   (can be created via Athena: CREATE DATABASE tpcds; then run tpcds DDL)
 *
 * Submit command:
 *   flink run -m yarn-cluster \
 *     -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
 *     flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
 */
public class FlinkSQLGlueTpcds {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLGlueTpcds.class);

    // EMR default Hive conf directory (Glue-backed)
    private static final String HIVE_CONF_DIR = "/etc/hive/conf.dist";

    // Catalog / database / table names
    private static final String CATALOG_NAME = "glue_catalog";
    private static final String DATABASE_NAME = "tpcds";
    private static final String TABLE_NAME = "call_center";

    public static void main(String[] args) throws Exception {
        LOG.info("=== Flink SQL Glue TPC-DS Job Starting ===");

        // 1. Create StreamExecutionEnvironment and TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Create Glue Catalog (HiveCatalog type, backed by Glue on EMR)
        String createCatalogSQL = String.format(
                "CREATE CATALOG %s WITH (" +
                "  'type' = 'hive'," +
                "  'default-database' = 'default'," +
                "  'hive-conf-dir' = '%s'" +
                ")",
                CATALOG_NAME, HIVE_CONF_DIR
        );

        LOG.info("Creating Glue Catalog: {}", createCatalogSQL);
        tableEnv.executeSql(createCatalogSQL);
        LOG.info("Glue Catalog '{}' created successfully.", CATALOG_NAME);

        // 3. Switch to glue_catalog and tpcds database
        tableEnv.executeSql(String.format("USE CATALOG %s", CATALOG_NAME));
        LOG.info("Switched to catalog: {}", CATALOG_NAME);

        tableEnv.executeSql(String.format("USE %s", DATABASE_NAME));
        LOG.info("Switched to database: {}", DATABASE_NAME);

        // 4. Show tables in tpcds (for verification)
        LOG.info("Listing tables in {}.{} ...", CATALOG_NAME, DATABASE_NAME);
        TableResult showTablesResult = tableEnv.executeSql("SHOW TABLES");
        showTablesResult.print();

        // 5. Query call_center table
        String selectSQL = String.format(
                "SELECT * FROM %s.%s.%s LIMIT 20",
                CATALOG_NAME, DATABASE_NAME, TABLE_NAME
        );
        LOG.info("Executing query: {}", selectSQL);

        TableResult result = tableEnv.executeSql(selectSQL);
        result.print();

        LOG.info("=== Flink SQL Glue TPC-DS Job Completed ===");
    }
}
