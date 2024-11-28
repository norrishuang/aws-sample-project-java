# From Kafka to S3
    
**submit**
```shell
KAFKA_BOOTSTRAP_SERVER=<kafka-host>
TOPIC_NAME=<topic>
S3_PATH=<s3-path>

flink run-application -t yarn-application \
-Dyarn.application.name=flink-kafka-s3 \
-Dparallelism.default=2 \
-Djobmanager.memory.process.size=1024mb \
-Dtaskmanager.memory.process.size=1024mb \
-Dtaskmanager.numberOfTaskSlots=1 \
-Dclassloader.resolve-order=parent-first \
flink-kafka-s3-1.0-SNAPSHOT.jar \
--kafka_bootstrap_servers $KAFKA_BOOTSTRAP_SERVER \
--topic $TOPIC_NAME \
--s3.path $S3_PATH
``` 
> There need to set parameter `-Dclassloader.resolve-order=parent-first` for resolve conflict of kafka.