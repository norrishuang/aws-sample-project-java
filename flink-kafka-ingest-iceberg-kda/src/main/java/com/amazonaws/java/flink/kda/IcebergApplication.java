/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file has been extended from the Apache Flink project skeleton.
 */

package com.amazonaws.java.flink.kda;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class IcebergApplication {
	private static final Logger LOG = LoggerFactory.getLogger(IcebergApplication.class);

	private static final String DEFAULT_KAFKA_TOPIC = "ev_station_data";
	private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String DEFAULT_WAREHOUSE = "thrift://localhost:9083";

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
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool applicationProperties = loadApplicationParameters(args, env);

		//Process stream using sql API
		CDCIcebergSqlExample.createAndDeployJob(env, applicationProperties);
	}


	public static class CDCIcebergSqlExample {

		public static void createAndDeployJob(StreamExecutionEnvironment env, ParameterTool applicationProperties)  {
			StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
					env, EnvironmentSettings.newInstance().build());

			String kafkaTopic = applicationProperties.get("kafka_topic", DEFAULT_KAFKA_TOPIC);
			String kafkaBootstrapServers = applicationProperties.get("kafka_bootstrap_servers", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);

			Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
			configuration.setString("execution.checkpointing.interval", "1 min");
			// 更激进的 TTL 配置 - 缩短状态保留时间
			configuration.setString("table.exec.state.ttl", "10 min");
			// 启用状态清理优化
			configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
			configuration.setString("table.exec.mini-batch.enabled", "true");
			configuration.setString("table.exec.mini-batch.allow-latency", "5s");
			configuration.setString("table.exec.mini-batch.size", "5000");

			String warehousePath = applicationProperties.get("iceberg_warehouse", DEFAULT_WAREHOUSE);

			//在KDA中使用Glue catalog
			final String icebergCatalog = String.format("CREATE CATALOG flink_catalog WITH ( \n" +
					"'type'='iceberg', \n" +
					"'warehouse'='%s', \n" +
					"'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', \n" +
					"'io-impl'='org.apache.iceberg.aws.s3.S3FileIO');", warehousePath);
			LOG.info(icebergCatalog);
			streamTableEnvironment.executeSql(icebergCatalog);

			final String sourceKafka =
				String.format(
					"CREATE TABLE IF NOT EXISTS customer_info (\n"
						+ "  `customerId` BIGINT,\n"
						+ "  `transactionAmount` BIGINT,\n"
						+ "  `sourceIp` STRING,\n"
						+ "  `status` STRING,\n"
						+ "  `transactionTime` TIMESTAMP(3),\n"
						+ "  WATERMARK FOR transactionTime AS transactionTime - INTERVAL '5' SECOND\n"
						+ ")\n"
						+ "WITH (\n"
						+ "  'connector' = 'kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  'scan.startup.mode' = 'latest-offset',\n"
						+ "  'properties.group.id' = 'flink-iceberg-consumer',\n"
						+ "  'format' = 'json'\n"
						+ ");",
					kafkaTopic, kafkaBootstrapServers);

			LOG.info(sourceKafka);
			streamTableEnvironment.executeSql(sourceKafka);

			final String icebergSink =
				String.format(
					"CREATE TABLE IF NOT EXISTS flink_catalog.iceberg_db.customer_info_flinksql_03 ( \n"
						+ "  `customerId` BIGINT,\n"
						+ "  `transactionAmount` BIGINT,\n"
						+ "  `sourceIp` STRING,\n"
						+ "  `status` STRING,\n"
						+ "  `transactionTime` TIMESTAMP(3)\n"
						+ ") with ( \n"
						+ "'type'='iceberg', \n"
						+ "'warehouse'='%s', \n"
						+ "'catalog-name'='flink_catalog', \n"
						+ "'write.metadata.delete-after-commit.enabled'='true', \n"
						+ "'write.metadata.previous-versions-max'='5', \n"
						+ "'format-version'='2');",
					warehousePath);
			LOG.info(icebergSink);
			streamTableEnvironment.executeSql(icebergSink);

			final String icebergSinkStat =
				String.format(
					"CREATE TABLE IF NOT EXISTS flink_catalog.iceberg_db.customer_info_stat ( \n"
						+ "  `customerId` BIGINT,\n"
						+ "  `cnt` BIGINT,\n"
						+ "  `triggerTs` TIMESTAMP_LTZ(3)\n"
						+ ") with ( \n"
						+ "'type'='iceberg', \n"
						+ "'warehouse'='%s', \n"
						+ "'catalog-name'='flink_catalog', \n"
						+ "'write.metadata.delete-after-commit.enabled'='true', \n"
						+ "'write.metadata.previous-versions-max'='5', \n"
						+ "'format-version'='2');",
					warehousePath);
			LOG.info(icebergSinkStat);
			streamTableEnvironment.executeSql(icebergSinkStat);

			final String insertSql = "insert into flink_catalog.iceberg_db.customer_info_flinksql_03 \n" +
					"select * from default_catalog.default_database.customer_info;";

			final String statSQL =
				"insert into flink_catalog.iceberg_db.customer_info_stat SELECT customerId, count(*) cnt, window_end as triggerTs "
					+ "FROM TABLE(HOP(TABLE customer_info,DESCRIPTOR(transactionTime),INTERVAL '20' SECOND,INTERVAL '60' MINUTE)) "
					+ "GROUP BY customerId, window_start, window_end ";

			StatementSet stmtSet = streamTableEnvironment.createStatementSet();
			stmtSet.addInsertSql(insertSql);
			stmtSet.addInsertSql(statSQL);
			stmtSet.execute();
		}
	}
}