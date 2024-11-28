# From Kafka to S3
    
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
``` 
> There need to set parameter `-Dclassloader.resolve-order=parent-first` for resolve conflict of kafka.