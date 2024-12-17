## aws-sample-project-java

该项目工程提供了关于在 Amazon EMR/EMR on EKS/ MSF 上运行 spark 或者 flink 任务的样例。

## MSF Sample

| 项目                                                             | 说明                                 |
|----------------------------------------------------------------|------------------------------------|
| [flink-kds-ingest-hudi-kda](./flink-kds-ingest-hudi-kda)       | 运行在 MSF 中，消费 KDS 以 HUDI 格式写入 S3    |
| [flink-kds-ingest-iceberg-kda](./flink-kds-ingest-iceberg-kda) | 运行在 MSF 中，消费 KDS 以 Iceberg 格式写入 S3 |
| [flink-opensearch-kda](./flink-opensearch-kda)                 | 运行在 MSF 中，消费 KDS 写入 OpenSearch     |


## EMR on EKS Sample

| 项目                           | 说明                                 |
|------------------------------|------------------------------------|
| [flink-kafka-opensearch](./flink-kafka-opensearch ) | 消费 Kafka 数据，写入 OpenSearch          |
|                              |                                    |


>提示
>1. 由于 KDA（MSF）未集成 Iceberg，因此需要通过重写类的方式支持，参见Blog[如何通过 AWS Managed Apache Flink 实现 Iceberg 的实时同步](https://blog.csdn.net/Turk/article/details/138824222)。