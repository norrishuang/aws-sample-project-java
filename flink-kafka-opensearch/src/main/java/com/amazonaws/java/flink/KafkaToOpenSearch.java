package com.amazonaws.java.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToOpenSearch {
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

      //            String aos_host =
      // "https://vpc-vector-db-demo-tzkscvrlfcdid4ykyrkanxgmri.us-east-1.es.amazonaws.com";

      String aos_host = applicationProperties.get("opensearch_host");
      String kafka_topic = applicationProperties.get("topic", "kafka_topic");
      String aos_user = applicationProperties.get("opensearch_user", "admin");
      String aos_password = applicationProperties.get("opensearch_password", "password");
      String kafka_bootstrap_servers = applicationProperties.get("kafka_bootstrap_servers");
      ;

      final String sourceKafka =
          String.format(
              "CREATE TABLE kafka_source_table (\n"
                  + "uuid STRING,\n"
                  + "user_name STRING,\n"
                  + "phone_number BIGINT,\n"
                  + "product_id INT,\n"
                  + "product_name STRING, \n"
                  + "product_type STRING, \n"
                  + "manufacturing_date INT, \n"
                  + "price FLOAT, \n"
                  + "unit INT, \n"
                  + "ts TIMESTAMP(3) \n"
                  + ") with (\n"
                  + "'connector' = 'kafka',\n"
                  + "'topic' = '%s',\n"
                  + "'properties.bootstrap.servers' = '%s',\n"
                  + "'scan.startup.mode' = 'earliest-offset',\n"
                  + "'properties.group.id' = 'flink-workshop-group-test-tb1',\n"
                  + "'json.timestamp-format.standard' = 'ISO-8601', \n"
                  + "'format' = 'debezium-json'\n"
                  + ");",
              kafka_topic, kafka_bootstrap_servers);

      streamTableEnvironment.executeSql(sourceKafka);

      final String sinkAOS =
          String.format(
              "CREATE TABLE myUserTable (\n"
                  + "uuid STRING,\n"
                  + "user_name STRING,\n"
                  + "phone_number BIGINT,\n"
                  + "product_id INT,\n"
                  + "product_name STRING, \n"
                  + "product_type STRING, \n"
                  + "manufacturing_date INT, \n"
                  + "price FLOAT, \n"
                  + "unit INT, \n"
                  + "ts TIMESTAMP(3) \n"
                  + ") WITH (\n"
                  + "  'connector' = 'opensearch',\n"
                  + "  'hosts' = '%s',\n"
                  + "  'username' = '%s',\n"
                  + "  'password' = '%s',\n"
                  + "  'allow-insecure' = 'true',\n"
                  + "  'index' = 'idx_user_order_list'\n"
                  + ");",
              aos_host, aos_user, aos_password);

      streamTableEnvironment.executeSql(sinkAOS);

      streamTableEnvironment.executeSql("INSERT INTO myUserTable SELECT * FROM kafka_source_table");
    }
  }
}
