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
public class IcebergApplication {
	private static final Logger LOG = LoggerFactory.getLogger(IcebergApplication.class);

	private static final String DEFAULT_KINESIS_STREAM = "ev_station_data";

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
		final ParameterTool applicationProperties = loadApplicationParameters(args, env);

		//Process stream using sql API
		CDCIcebergSqlExample.createAndDeployJob(env, applicationProperties);
	}


	public static class CDCIcebergSqlExample {

		public static void createAndDeployJob(StreamExecutionEnvironment env, ParameterTool applicationProperties)  {
			StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
					env, EnvironmentSettings.newInstance().build());

			String kds_stream = applicationProperties.get("kinesis_stream",DEFAULT_KINESIS_STREAM);


			Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
			configuration.setString("execution.checkpointing.interval", "1 min");
////


			String warehousePath = "s3a://emr-hive-us-east-1-812046859005/datalake/iceberg-folder";
			//hive catalog
//			final String icebergCatalog = String.format("CREATE CATALOG flink_catalog WITH (\n" +
//					"   'type'='iceberg',\n" +
//					"   'warehouse'='%s',\n" +
//					"   'catalog-impl'='hive',\n" +
//					"   'clients'='5'," +
//					"	'uri'='thrift://ip-10-192-11-165.ec2.internal:9083'" +
//					" )", warehousePath);

			//在KDA中使用Glue catalog 存在问题
			final String icebergCatalog = String.format("CREATE CATALOG flink_catalog WITH ( \n" +
					"'type'='iceberg', \n" +
					"'warehouse'='%s', \n" +
					"'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', \n" +
					"'io-impl'='org.apache.iceberg.aws.s3.S3FileIO');", warehousePath);
			LOG.info(icebergCatalog);
			streamTableEnvironment.executeSql(icebergCatalog);

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

			LOG.info(sourceKinesis);
			streamTableEnvironment.executeSql(sourceKinesis);

			final String icebergSink = String.format("CREATE TABLE flink_catalog.iceberg_db.customer_info_flinksql_03 ( \n" +
					"  `customerId` BIGINT,\n" +
					"  `transactionAmount` BIGINT,\n" +
					"  `sourceIp` STRING,\n" +
					"  `status` STRING,\n" +
					"  `transactionTime` STRING\n" +
					") with ( \n" +
					"'type'='iceberg', \n" +
					"'warehouse'='%s', \n" +
					"'catalog-name'='flink_catalog', \n" +
					"'write.metadata.delete-after-commit.enabled'='true', \n" +
					"'write.metadata.previous-versions-max'='5', \n" +
					"'format-version'='2');", warehousePath);
			LOG.info(icebergSink);

			streamTableEnvironment.executeSql(icebergSink);

			final String insertSql = "insert into flink_catalog.iceberg_db.customer_info_flinksql_03 \n" +
					"select * from default_catalog.default_database.customer_info;";
			streamTableEnvironment.executeSql(insertSql);
		}
	}

}



