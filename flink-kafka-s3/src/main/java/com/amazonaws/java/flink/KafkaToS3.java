package com.amazonaws.java.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

public class KafkaToS3 {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool applicationProperties = ParameterTool.fromArgs(args);

    // Process stream using sql API
    OpenSearchSink.createAndDeployJob(env, applicationProperties);
  }

  public static class OpenSearchSink {

    public static void createAndDeployJob(
        StreamExecutionEnvironment env, ParameterTool applicationProperties) {
      StreamTableEnvironment streamTableEnvironment =
          StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

      Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
      configuration.setString("execution.checkpointing.interval", "1 min");

      String kafka_bootstrap_servers = applicationProperties.get("kafka_bootstrap_servers");
      String s3Path = applicationProperties.get("s3.path");
      String gruopid = applicationProperties.get("group.id","flink-workshop-group-test-tb1");

      // ingest data from kafka to s3 with flinksql and used glue data catalog
      final String kafka_topic = applicationProperties.get("topic");


      final String sourceKafka =
          String.format(
              "CREATE TABLE kafka_source_table (\n"
                      + "before STRING,\n"
                      + "after STRING,\n"
                  + "source STRING,\n"
                  + "op STRING,\n"
                  + "ts_ms BIGINT,\n"
                  + "transaction STRING \n"
                  + ") with (\n"
                  + "'connector' = 'kafka',\n"
                  + "'topic' = '%s',\n"
                  + "'properties.bootstrap.servers' = '%s',\n"
                  + "'scan.startup.mode' = 'earliest-offset',\n"
                  + "'properties.group.id' = '%s',\n"
                  + "'format' = 'json'\n"
                  + ")",
              kafka_topic, kafka_bootstrap_servers, gruopid);

      streamTableEnvironment.executeSql(sourceKafka);

      // 创建 S3 Parquet 接收表
      streamTableEnvironment.executeSql("CREATE TABLE s3_sink ("
                      + "before STRING,\n"
                      + "after STRING,\n"
                      + "source STRING,\n"
                      + "op STRING,\n"
                      + "ts_ms BIGINT,\n"
                      + "transaction STRING \n" +
              ") WITH (" +
              "  'connector' = 'filesystem'," +
              "  'path' = '" +s3Path+ "'," +
              "  'format' = 'parquet'" +
              ")");


      streamTableEnvironment.executeSql("INSERT INTO s3_sink " +
              "SELECT * " +
              "FROM kafka_source_table");
    }
  }
}
