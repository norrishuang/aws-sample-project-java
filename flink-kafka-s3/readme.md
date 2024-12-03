# From Kafka to S3

> Flink 1.12.1, There is different method of Kafka source between flink 1.12.1 and 1.17. 

**submit**
```shell
KAFKA_BOOTSTRAP_SERVER=<kafka-host>
TOPIC_NAME=<topic>
S3_PATH=<s3-path>
checkpoints=s3://<s3-bucket>/checkpoint/

flink run-application -t yarn-application \
-Dyarn.application.name=flink-kafka-s3 \
-Dparallelism.default=2 \
-Djobmanager.memory.process.size=2048mb \
-Dtaskmanager.memory.process.size=2048mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-D state.checkpoint-storage=filesystem \
-D state.checkpoints.dir=${checkpoints} \
-D execution.checkpointing.interval=60000 \
-D state.checkpoints.num-retained=2 \
-D execution.checkpointing.mode=EXACTLY_ONCE \
-D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
-D execution.checkpointing.max-concurrent-checkpoints=2 \
-D execution.checkpointing.checkpoints-after-tasks-finish.enabled=true \
-Dclassloader.resolve-order=parent-first \
flink-kafka-s3-1.0-SNAPSHOT.jar \
--kafka_bootstrap_servers $KAFKA_BOOTSTRAP_SERVER \
--topic $TOPIC_NAME \
--s3.path $S3_PATH
--group.id <consumer-group-id> \
--ssl.truststore.location /tmp/kafka.client.truststore.jks \
--ssl.keystore.location /tmp/kafka.client.keystore.jks \
--ssl.keystore.password <keystore-password> \
--ssl.key.password <keypassword>
``` 
> There need to set parameter `-Dclassloader.resolve-order=parent-first` for resolve conflict of kafka.

In [KafkaS3SinkParquet.java](https://github.com/norrishuang/aws-sample-project-java/blob/main/flink-kafka-s3/src/main/java/com/amazonaws/java/flink/KafkaS3SinkParquet.java), It implement connect MSK with mutual TLS. [How to create certification files](https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html)

```shell
cp /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.amzn2.0.1.x86_64/jre/lib/security/cacerts kafka.client.truststore.jks

keytool -genkey -keystore kafka.client.keystore.jks -validity 300 -storepass <password> -keypass <password> -dname "C=CN, O=Example LLC, OU=Engineering, ST=GuangDong, CN=Private Root CA1, L=ShenZhen" -alias kafkaclient -storetype pkcs12

keytool -keystore kafka.client.keystore.jks -certreq -file client-cert-sign-request -alias kafkaclient -storepass <password> -keypass <password>

aws acm-pca issue-certificate --certificate-authority-arn <aws-acm-arn> --csr fileb://client-cert-sign-request --signing-algorithm "SHA256WITHRSA" --validity Value=300,Type="DAYS"

aws acm-pca get-certificate --certificate-authority-arn <aws-acm-arn> --certificate-arn <aws-acm-arn-response-from-above-command>

keytool -keystore kafka.client.keystore.jks -import -file signed-certificate-from-acm -alias kafkaclient -storepass <password> -keypass <password>

```



When enable the TLS, there is a default truststore, set
`truststore.location=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.amzn2.0.1.x86_64/jre/lib/security/cacerts`

