package com.amazonaws.java.flink.kda;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MSKJsonIcebergSink {

  private static String _topics = "topic01";
  private static String _kafkaBootstrapServers =
      "b-1.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092,b-2.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092,b-3.msk-cdc-demo.wzx9f.c2.kafka.us-east-1.amazonaws.com:9092";
  // private static String _s3SinkPath = "s3a://ka-app-code-<username>/data";

  private static String _warehousePath = "s3a://ka-app-code-<username>/warehouse";

  private static String _icebergTable = "user_order_list_flink_iceberg";

  private static String _icebergStatTable = "user_order_info_stat";

  private static String _groupId = "kafka_user_order_list_iceberg_01";

  private static final Logger LOG = LoggerFactory.getLogger(MSKJsonIcebergSink.class);

  private static int _Parallelism = 3;

  private static ParameterTool loadApplicationParameters(
      String[] args, StreamExecutionEnvironment env) throws IOException {
    if (env instanceof LocalStreamEnvironment) {
      return ParameterTool.fromArgs(args);
    } else {
      // Try to load from KDA runtime first
      try {
        Map<String, Properties> applicationProperties =
            KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
        if (flinkProperties != null) {
          Map<String, String> map = new HashMap<>(flinkProperties.size());
          flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
          return ParameterTool.fromMap(map);
        }
      } catch (Exception e) {
        LOG.info(
            "KDA runtime not available, falling back to command line arguments: " + e.getMessage());
      }

      // Fallback to command line arguments for EMR/standalone deployment
      return ParameterTool.fromArgs(args);
    }
  }

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment

    // StreamExecutionEnvironment 用于参数获取
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool applicationProperties = loadApplicationParameters(args, env);
    applicationProperties = applicationProperties.mergeWith(ParameterTool.fromSystemProperties());

    _kafkaBootstrapServers = applicationProperties.get("bootstrap.servers");
    _topics = applicationProperties.get("topics");
    _warehousePath = applicationProperties.get("s3.path");
    _Parallelism = Integer.parseInt(applicationProperties.get("parallelism", "1"));

    _icebergTable = applicationProperties.get("iceberg.table");
    _icebergStatTable = applicationProperties.get("iceberg.stat.table");
    _groupId = applicationProperties.get("group.id", "kafka_user_order_list_iceberg_01");

    // TableEnvironment 用于 SQL 操作
    TableEnvironment tableEnvironment =
        TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

    IcebergSink.createAndDeployJob(tableEnvironment);
  }

  public static class IcebergSink {

    public static void createAndDeployJob(TableEnvironment tableEnvironment) {
      Configuration configuration = tableEnvironment.getConfig().getConfiguration();

      configuration.setString("execution.checkpointing.interval", "1 min");
      //                        tableEnvironment.setParallelism(_Parallelism);

      configuration.setString("execution.checkpointing.interval", "1 min");
      // 更激进的 TTL 配置 - 缩短状态保留时间
      //                        configuration.setString("table.exec.state.ttl", "3600000");
      TableConfig tableConfig = tableEnvironment.getConfig();
      tableConfig.setIdleStateRetention(Duration.ofMinutes(60));

      // 启用状态清理优化
      configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      configuration.setString("table.exec.mini-batch.enabled", "true");
      configuration.setString("table.exec.mini-batch.allow-latency", "5s");
      configuration.setString("table.exec.mini-batch.size", "5000");

      final String icebergCatalog =
          String.format(
              "CREATE CATALOG glue_catalog WITH ( \n"
                  + "'type'='iceberg', \n"
                  + "'warehouse'='%s', \n"
                  + "'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', \n"
                  + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO');",
              _warehousePath);

      LOG.info(icebergCatalog);
      tableEnvironment.executeSql(icebergCatalog);

      // Source
      final String sourceKafkaSQL =
          String.format(
              "CREATE TABLE kafka_source_table (\n"
                  + "ts TIMESTAMP(3),\n"
                  + "uuid STRING,\n"
                  + "user_name STRING,\n"
                  + "phone_number BIGINT,\n"
                  + "product_id STRING,\n"
                  + "product_name STRING,\n"
                  + "product_type STRING,\n"
                  + "manufacturing_date INT,\n"
                  + "price FLOAT,\n"
                  + "unit INT,\n"
                  + "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                  + ") with (\n"
                  + "'connector' = 'kafka',\n"
                  + "'topic' = '%s',\n"
                  + "'properties.bootstrap.servers' = '%s',\n"
                  + "'scan.startup.mode' = 'latest-offset', \n"
                  + "'properties.group.id' = '%s',\n"
                  + "'format' = 'json',\n"
                  + "'json.timestamp-format.standard' = 'SQL'\n"
                  + ");",
              _topics, _kafkaBootstrapServers, _groupId);
      LOG.info(sourceKafkaSQL);
      tableEnvironment.executeSql(sourceKafkaSQL);

      final String s3SinkSql =
          String.format(
              "CREATE TABLE IF NOT EXISTS glue_catalog.icebergdb.%s (\n"
                  + "ts TIMESTAMP(3),\n"
                  + "uuid STRING,\n"
                  + "user_name STRING,\n"
                  + "phone_number BIGINT,\n"
                  + "product_id STRING,\n"
                  + "product_name STRING,\n"
                  + "product_type STRING,\n"
                  + "manufacturing_date INT,\n"
                  + "price FLOAT,\n"
                  + "unit INT\n"
                  + ") WITH (\n"
                  + "'type'='iceberg', \n"
                  + "'catalog-name'='glue_catalog', \n"
                  + "'write.metadata.delete-after-commit.enabled'='true', \n"
                  + "'write.metadata.previous-versions-max'='5', \n"
                  + "'format-version'='2' \n"
                  + ");",
              _icebergTable);

      tableEnvironment.executeSql(s3SinkSql);

      final String Insert_Iceberg =
          String.format(
              "INSERT INTO  " + "glue_catalog.icebergdb.%s " + "SELECT * FROM kafka_source_table;",
              _icebergTable);

      final String CreateIcebergSinkStatTB =
          String.format(
              "CREATE TABLE IF NOT EXISTS glue_catalog.icebergdb.%s ( \n"
                  + "  `product_name` STRING,\n"
                  + "  `total_unit` INT,\n"
                  + "  `avg_price` FLOAT,\n"
                  + "  `startTs` TIMESTAMP(3),\n"
                  + "  `triggerTs` TIMESTAMP(3)\n"
                  + ") with ( \n"
                  + "'type'='iceberg', \n"
                  + "'warehouse'='%s', \n"
                  + "'catalog-name'='glue_catalog', \n"
                  + "'write.metadata.delete-after-commit.enabled'='true', \n"
                  + "'write.metadata.previous-versions-max'='5', \n"
                  + "'format-version'='2' \n"
                  + ");",
              _icebergStatTable, _warehousePath);
      LOG.info(CreateIcebergSinkStatTB);
      tableEnvironment.executeSql(CreateIcebergSinkStatTB);

      final String statSQL =
          String.format(
              "INSERT INTO glue_catalog.icebergdb.%s \n"
                  + "SELECT \n"
                  + "  product_name, "
                  + "  SUM(unit) AS total_unit, "
                  + "  AVG(price) AS avg_price, "
                  + "  window_start AS startTs,"
                  + "  window_end AS triggerTs "
                  + "FROM TABLE(HOP(TABLE kafka_source_table,DESCRIPTOR(ts),INTERVAL '20' SECOND,INTERVAL '60' MINUTE))  "
                  + "GROUP BY product_name, window_start, window_end;",
              _icebergStatTable);

      //                    tableEnvironment.executeSql(Insert_Iceberg);
      StatementSet stmtSet = tableEnvironment.createStatementSet();
      stmtSet.addInsertSql(Insert_Iceberg);
      stmtSet.addInsertSql(statSQL);
      stmtSet.execute();
    }
  }
}
