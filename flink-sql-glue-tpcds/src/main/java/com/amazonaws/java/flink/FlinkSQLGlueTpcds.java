package com.amazonaws.java.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Flink SQL Job: Create Glue Catalog and query tpcds.call_center table.
 *
 * This job runs on EMR 7.12 (Flink 1.20). It:
 * 1. Creates a HiveCatalog backed by AWS Glue Data Catalog
 * 2. Switches to the tpcds database
 * 3. Executes SELECT on the call_center table and prints results
 *
 * Supports both session mode (SQL Client) and application mode (yarn-application).
 * In application mode, hive-site.xml must be shipped via:
 *   -Dyarn.ship-files=/etc/hive/conf.dist/hive-site.xml
 *
 * Submit command (application mode):
 *   flink run-application -t yarn-application \
 *     -Dyarn.ship-files=/etc/hive/conf.dist/hive-site.xml \
 *     -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
 *     /home/hadoop/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
 */
public class FlinkSQLGlueTpcds {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLGlueTpcds.class);

    // Catalog / database / table names
    private static final String CATALOG_NAME = "glue_catalog";
    private static final String DATABASE_NAME = "tpcds";
    private static final String TABLE_NAME = "call_center";

    public static void main(String[] args) throws Exception {
        LOG.info("=== Flink SQL Glue TPC-DS Job Starting ===");

        // 1. Resolve hive-conf-dir: prefer EMR default, fallback to current working dir
        //    (yarn-application ships hive-site.xml to the container's working directory)
        String hiveConfDir = resolveHiveConfDir();
        LOG.info("Using hive-conf-dir: {}", hiveConfDir);

        // 2. Create StreamExecutionEnvironment and TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create Glue Catalog (HiveCatalog type, backed by Glue on EMR)
        String createCatalogSQL = String.format(
                "CREATE CATALOG %s WITH (" +
                "  'type' = 'hive'," +
                "  'default-database' = 'default'," +
                "  'hive-conf-dir' = '%s'" +
                ")",
                CATALOG_NAME, hiveConfDir
        );

        LOG.info("Creating Glue Catalog: {}", createCatalogSQL);
        tableEnv.executeSql(createCatalogSQL);
        LOG.info("Glue Catalog '{}' created successfully.", CATALOG_NAME);

        // 4. Switch to glue_catalog and tpcds database
        tableEnv.executeSql(String.format("USE CATALOG %s", CATALOG_NAME));
        LOG.info("Switched to catalog: {}", CATALOG_NAME);

        tableEnv.executeSql(String.format("USE %s", DATABASE_NAME));
        LOG.info("Switched to database: {}", DATABASE_NAME);

        // 5. Show tables in tpcds (for verification)
        LOG.info("Listing tables in {}.{} ...", CATALOG_NAME, DATABASE_NAME);
        TableResult showTablesResult = tableEnv.executeSql("SHOW TABLES");
        showTablesResult.print();

        // 6. Query call_center table
        String selectSQL = String.format(
                "SELECT * FROM %s.%s.%s LIMIT 20",
                CATALOG_NAME, DATABASE_NAME, TABLE_NAME
        );
        LOG.info("Executing query: {}", selectSQL);

        TableResult result = tableEnv.executeSql(selectSQL);
        result.print();

        LOG.info("=== Flink SQL Glue TPC-DS Job Completed ===");
    }

    /**
     * Resolve hive-conf-dir with fallback logic:
     * 1. /etc/hive/conf.dist (EMR session mode / SQL Client)
     * 2. Current working directory (application mode with yarn.ship-files)
     */
    private static String resolveHiveConfDir() {
        // Priority 1: EMR default location
        String emrHiveConf = "/etc/hive/conf.dist";
        if (new File(emrHiveConf, "hive-site.xml").exists()) {
            return emrHiveConf;
        }

        // Priority 2: current working dir (yarn container ships files here)
        String cwd = System.getProperty("user.dir");
        if (new File(cwd, "hive-site.xml").exists()) {
            LOG.info("Found hive-site.xml in working directory: {}", cwd);
            return cwd;
        }

        // Fallback: still use EMR default (will fail with clear error if missing)
        LOG.warn("hive-site.xml not found in {} or {}, falling back to {}", emrHiveConf, cwd, emrHiveConf);
        return emrHiveConf;
    }
}
