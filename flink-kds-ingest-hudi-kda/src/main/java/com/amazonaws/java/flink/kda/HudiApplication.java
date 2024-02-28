package com.amazonaws.java.flink.kda;/*
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
 * Skeleton for a Hudi Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the pom.xml file (simply search for 'mainClass').
 *
 * <p>Disclaimer: This code is not production ready.</p>
 */
public class HudiApplication {
	private static final Logger LOG = LoggerFactory.getLogger(HudiApplication.class);

	private static final String DEFAULT_KINESIS_STREAM = "ev_station_data";

	private static final String DEFAULT_HIVE_METASTORE = "thrift://localhost:9083";

	private static final String DEFAULT_HUDI_S3_PATH = "s3://";


	private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
		if (env instanceof LocalStreamEnvironment) {
			return ParameterTool.fromArgs(args);
		} else {
			Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
			Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
			if (flinkProperties == null) {
				throw new RuntimeException("Unable to load FlinkApplicationProperties properties from runtime properties");
			}
			Map<String, String> map = new HashMap<>(flinkProperties.size());
			flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
			return ParameterTool.fromMap(map);
		}
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		final ParameterTool parameter = ParameterTool.fromArgs(args);
//
//		//read the parameters from the Kinesis Analytics environment
//		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
//		Properties flinkProperties = null;
//
//		String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
//		String brokers = parameter.get("brokers", "");
//		String s3Path = parameter.get("s3Path", "");
//		String hiveMetaStore = parameter.get("hivemetastore", "");
//
//		if (applicationProperties != null) {
//			flinkProperties = applicationProperties.get("FlinkApplicationProperties");
//		}
//
//		if (flinkProperties != null) {
//			kafkaTopic = flinkProperties.get("kafka-topic").toString();
//			brokers = flinkProperties.get("brokers").toString();
//			s3Path = flinkProperties.get("s3Path").toString();
//			hiveMetaStore = flinkProperties.get("hivemetastore").toString();
//		}
//
//		LOG.info("kafkaTopic is :" + kafkaTopic);
//		LOG.info("brokers is :" + brokers);
//		LOG.info("s3Path is :" + s3Path);
//		LOG.info("hiveMetaStore is :" + hiveMetaStore);
//
//		//Create Properties object for the Kafka consumer
//		Properties kafkaProps = new Properties();
//		kafkaProps.setProperty("bootstrap.servers", brokers);

		final ParameterTool applicationProperties = loadApplicationParameters(args, env);

		//Process stream using sql API
		KafkaHudiSqlExample.createAndDeployJob(env, applicationProperties);
	}


	public static class KafkaHudiSqlExample {

		public static void createAndDeployJob(StreamExecutionEnvironment env, ParameterTool applicationProperties)  {
			StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
					env, EnvironmentSettings.newInstance().build());

			Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
			configuration.setString("execution.checkpointing.interval", "1 min");

			String kds_stream = applicationProperties.get("kinesis_stream", DEFAULT_KINESIS_STREAM);

			String s3Path = applicationProperties.get("hudi_s3_path", DEFAULT_HUDI_S3_PATH);

			String hiveMetaSoreUris = applicationProperties.get("hivemetastore", DEFAULT_HUDI_S3_PATH);


//			final String createTableStmt = "CREATE TABLE IF NOT EXISTS CustomerTable (\n" +
//					"  `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format\n" +
//					"  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format\n" +
//					"  `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,\n" +
//					"  `CUST_ID` BIGINT,\n" +
//					"  `NAME` STRING,\n" +
//					"  `MKTSEGMENT` STRING,\n" +
//					"   WATERMARK FOR event_time AS event_time\n" +
//					") WITH (\n" +
//					"  'connector' = 'kafka',\n" +
//					"  'topic' = '"+ kafkaTopic +"',\n" +
//					"  'properties.bootstrap.servers' = '"+  kafkaProperties.get("bootstrap.servers") +"',\n" +
//					"  'properties.group.id' = 'kdaConsumerGroup',\n" +
//					"  'scan.startup.mode' = 'earliest-offset',\n" +
//					"  'value.format' = 'debezium-json'\n" +
//					")";

			final String sourceKinesis = String.format("CREATE TABLE IF NOT EXISTS KinesisTable (\n" +
					"  `customerId` BIGINT,\n" +
					"  `transactionAmount` BIGINT,\n" +
					"  `sourceIp` STRING,\n" +
					"  `status` STRING,\n" +
					"  `transactionTime` STRING\n" +
					")\n" +
					"WITH (\n" +
					"  'connector' = 'kinesis',\n" +
					"  'stream' = '%s',\n" +
					"  'aws.region' = 'us-east-1',\n" +
					"  'scan.stream.initpos' = 'LATEST',\n" +
					"  'format' = 'json'\n" +
					");", kds_stream);

			streamTableEnvironment.executeSql(sourceKinesis);

			final String s3Sink = "CREATE TABLE CustomerHudi (\n" +
					"  `customerId` BIGINT,\n" +
					"  `transactionAmount` BIGINT,\n" +
					"  `sourceIp` STRING,\n" +
					"  `status` STRING,\n" +
					"  `transactionTime` STRING\n" +
					"    ) \n" +
					"    WITH (\n" +
					"    'connector' = 'hudi',\n" +
					"    'compaction.tasks'='1',\n" +
					"    'changelog.enabled'='true',\n" +
					"    'write.task.max.size'='4096',\n" +
					"    'write.bucket_assign.tasks'='1',\n" +
					"    'hoodie.embed.timeline.server'='false',\n" +
					"    'write.merge.max_memory'='1024',\n" +
					"    'write.tasks' = '1',\n" +
					"    'hive_sync.enable' = 'true',\n" +
					"    'hive_sync.db' = 'hudi',\n" +
					"    'hive_sync.table' = 'customer_hudi_auto',\n" +
					"    'hive_sync.mode' = 'hms',\n" +
					"    'hive_sync.metastore.uris' = '"+ hiveMetaSoreUris +"',\n" +
					"    'hive_sync.use_jdbc' = 'false',\n" +
					"    'path' = '" + s3Path + "',\n" +
					"    'table.type' = 'MERGE_ON_READ'\n" +
					"    )";

			streamTableEnvironment.executeSql(s3Sink);

			final String insertSql = "insert into CustomerHudi select * from KinesisTable";
			streamTableEnvironment.executeSql(insertSql);
		}
	}

}



