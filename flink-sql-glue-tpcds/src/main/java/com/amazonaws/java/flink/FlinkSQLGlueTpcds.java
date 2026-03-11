package com.amazonaws.java.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SQL Job: Create Glue Catalog via HiveCatalog API and query tpcds.call_center.
 *
 * Hardcodes Glue metastore config - no hive-site.xml dependency.
 * Works in both session mode and application mode on EMR 7.12.
 *
 * Submit:
 *   flink run-application -t yarn-application \
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

        // 1. Create TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Build HiveConf programmatically - no hive-site.xml needed
        HiveConf hiveConf = new HiveConf();
        // Key: use Glue as the metastore backend
        hiveConf.set("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
        // A dummy thrift URI to satisfy HiveCatalog's non-empty check
        hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");

        LOG.info("HiveConf created with Glue metastore factory class (no hive-site.xml)");

        // 3. Create HiveCatalog via Java API (bypasses hive-conf-dir entirely)
        HiveCatalog hiveCatalog = new HiveCatalog(
                CATALOG_NAME,       // catalog name
                "default",          // default database
                hiveConf,           // programmatic HiveConf
                "3.1.3"             // hive version on EMR 7.12
        );

        // 4. Register catalog in TableEnvironment
        tableEnv.registerCatalog(CATALOG_NAME, hiveCatalog);
        LOG.info("Glue Catalog '{}' registered successfully.", CATALOG_NAME);

        // 5. Switch to catalog and database
        tableEnv.executeSql(String.format("USE CATALOG %s", CATALOG_NAME));
        LOG.info("Switched to catalog: {}", CATALOG_NAME);

        tableEnv.executeSql(String.format("USE %s", DATABASE_NAME));
        LOG.info("Switched to database: {}", DATABASE_NAME);

        // 6. Show tables
        LOG.info("Listing tables in {}.{} ...", CATALOG_NAME, DATABASE_NAME);
        TableResult showTablesResult = tableEnv.executeSql("SHOW TABLES");
        showTablesResult.print();

        // 7. Query call_center
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
