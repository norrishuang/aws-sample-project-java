package com.amazonaws.java.flink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KDAtoOpenSearch {

    private static final Logger LOG = LoggerFactory.getLogger(KDAtoOpenSearch.class);

    private static final String DEFAULT_KINESIS_STREAM = "ev_station_data";

    private static final String DEFAULT_AOS_USER = "user";

    private static final String DEFAULT_AOS_PASSWORD = "";

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

        //read the parameters from the Kinesis Analytics environment
//        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
//        Properties flinkProperties = null;

//		String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
//		String brokers = parameter.get("brokers", "");
//		String s3Path = parameter.get("s3Path", "");
//		String hiveMetaStore = parameter.get("hivemetastore", "");

//        if (applicationProperties != null) {
//            flinkProperties = applicationProperties.get("FlinkApplicationProperties");
//        }

//		if (flinkProperties != null) {
//			kafkaTopic = flinkProperties.get("kafka-topic").toString();
//			brokers = flinkProperties.get("brokers").toString();
//			s3Path = flinkProperties.get("s3Path").toString();
//			hiveMetaStore = flinkProperties.get("hivemetastore").toString();
//		}

//		LOG.info("kafkaTopic is :" + kafkaTopic);
//		LOG.info("brokers is :" + brokers);
//		LOG.info("s3Path is :" + s3Path);
//		LOG.info("hiveMetaStore is :" + hiveMetaStore);
//
//		//Create Properties object for the Kafka consumer
//		Properties kafkaProps = new Properties();
//		kafkaProps.setProperty("bootstrap.servers", brokers);

        //Process stream using sql API
        OpenSearchSink.createAndDeployJob(env, applicationProperties);
    }

    public static class OpenSearchSink{

        public static void createAndDeployJob(StreamExecutionEnvironment env, ParameterTool applicationProperties) {
            StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().build());

            Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
            configuration.setString("execution.checkpointing.interval", "1 min");

//            String aos_host = "https://vpc-vector-db-demo-tzkscvrlfcdid4ykyrkanxgmri.us-east-1.es.amazonaws.com";
            String aos_host = applicationProperties.get("opensearch_host");
            String kds_stream = applicationProperties.get("kinesis_stream",DEFAULT_KINESIS_STREAM);
            String aos_user = applicationProperties.get("opensearch_user", DEFAULT_AOS_USER);
            String aos_password = applicationProperties.get("opensearch_password", DEFAULT_AOS_PASSWORD);

            final String sourceKinesis = String.format("CREATE TABLE KinesisTable (\n" +
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

            final String sinkAOS = String.format("CREATE TABLE myUserTable (\n" +
                    "  customerId BIGINT,\n" +
                    "  transactionAmount BIGINT,\n" +
                    "  sourceIp STRING,\n" +
                    "  status STRING,\n" +
                    "  PtransactionTime STRING\n" +
                    ") WITH (\n" +
                    "  'connector' = 'opensearch',\n" +
                    "  'hosts' = '%s',\n" +
                    "  'username' = '%s',\n" +
                    "  'password' = '%s',\n" +
                    "  'allow-insecure' = 'true',\n" +
                    "  'index' = 'ticket'\n" +
                    ");", aos_host, aos_user, aos_password);

            streamTableEnvironment.executeSql(sinkAOS);

            streamTableEnvironment.executeSql("INSERT INTO myUserTable SELECT * FROM KinesisTable");


        }


    }

}
