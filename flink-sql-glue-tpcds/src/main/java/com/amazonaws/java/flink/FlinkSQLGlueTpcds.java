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
 * Runs on EMR 7.12 (Flink 1.20) in both session and application mode.
 *
 * Application mode submit:
 *   flink run-application -t yarn-application \
 *     -Dyarn.ship-files=/etc/hive/conf.dist/hive-site.xml \
 *     -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
 *     /home/hadoop/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
 */
public class FlinkSQLGlueTpcds {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLGlueTpcds.class);

    private static final String CATALOG_NAME = "glue_catalog";
    private static final String DATABASE_NAME = "tpcds";
    private static final String TABLE_NAME = "call_center";

    public static void main(String[] args) throws Exception {
        LOG.info("=== Flink SQL Glue TPC-DS Job Starting ===");

        // 1. Resolve hive-conf-dir
        String hiveConfDir = resolveHiveConfDir();
        LOG.info("Using hive-conf-dir: {}", hiveConfDir);

        // 2. Create TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create Glue Catalog
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

        // 4. Switch to catalog and database
        tableEnv.executeSql(String.format("USE CATALOG %s", CATALOG_NAME));
        LOG.info("Switched to catalog: {}", CATALOG_NAME);

        tableEnv.executeSql(String.format("USE %s", DATABASE_NAME));
        LOG.info("Switched to database: {}", DATABASE_NAME);

        // 5. Show tables
        LOG.info("Listing tables in {}.{} ...", CATALOG_NAME, DATABASE_NAME);
        TableResult showTablesResult = tableEnv.executeSql("SHOW TABLES");
        showTablesResult.print();

        // 6. Query call_center
        String selectSQL = String.format(
                "SELECT * FROM %s.%s.%s LIMIT 20",
                CATALOG_NAME, DATABASE_NAME, TABLE_NAME
        );
        LOG.info("Executing query: {}", selectSQL);
        TableResult result = tableEnv.executeSql(selectSQL);
        result.print();

        LOG.info("=== Flink SQL Glue TPC-DS Job Completed ===");
    }

    private static String resolveHiveConfDir() {
        // Check common EMR hive conf locations in order
        String[] candidates = {
            "/etc/hive/conf",
            "/etc/hive/conf.dist",
            "/etc/hadoop/conf"
        };
        for (String dir : candidates) {
            if (new File(dir, "hive-site.xml").exists()) {
                LOG.info("Found hive-site.xml in: {}", dir);
                return dir;
            }
        }
        // Fallback: current working dir (yarn container with shipped files)
        String cwd = System.getProperty("user.dir");
        if (new File(cwd, "hive-site.xml").exists()) {
            LOG.info("Found hive-site.xml in working directory: {}", cwd);
            return cwd;
        }
        // Default
        LOG.warn("hive-site.xml not found, falling back to /etc/hive/conf");
        return "/etc/hive/conf";
    }
}
