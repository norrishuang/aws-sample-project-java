# flink-kafka-iceberg-dynamic

Real-time ingestion from Apache Kafka (Amazon MSK) to Apache Iceberg / Amazon S3 Tables using Flink's DataStream API and Iceberg's **Dynamic Sink API**.

## Architecture

```
Kafka Source (plain JSON)
    │
    ▼
DataStream<String>
    │
    ▼
DynamicIcebergSink
  ├─ KafkaJsonDynamicRecordGenerator  (JSON → DynamicRecord)
  ├─ DynamicRecordProcessor           (schema evolution, table creation)
  ├─ DynamicWriter                    (writes data files)
  └─ DynamicCommitter                 (commits to Iceberg catalog)
```

## Features

- **Schema inference** from JSON values (boolean, int, long, double, string)
- **Predefined schema** support via `iceberg.schema` parameter
- **Dynamic table routing**: FIXED mode (single table) or FIELD mode (route by JSON field value)
- **Multiple catalog support**: Glue, Hive, Hadoop, REST
- **S3 Tables support**: Write to Amazon S3 Tables via REST catalog with SigV4
- **Exactly-once semantics** via Flink checkpointing + Iceberg commits

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `kafka.bootstrap.servers` | `localhost:9092` | MSK broker addresses |
| `kafka.topic` | `game-action-logs` | Kafka topic to consume |
| `kafka.group.id` | `flink-kafka-iceberg-dynamic` | Consumer group ID |
| `kafka.startup.mode` | `latest-offset` | `earliest-offset` / `latest-offset` / `group-offsets` |
| `iceberg.namespace` | `default` | Iceberg namespace |
| `iceberg.table` | `kafka_json_table` | Table name (FIXED mode) or fallback (FIELD mode) |
| `kafka.routing.field` | `null` | JSON field for dynamic table routing (null = FIXED mode) |
| `iceberg.schema` | `null` | Predefined schema: `"field1:type1,field2:type2,..."` |
| `sink.target` | `iceberg` | `iceberg` or `s3tables` |
| `iceberg.catalog.type` | `hadoop` | `hadoop` / `glue` / `rest` / `hive` |
| `iceberg.catalog.name` | `iceberg_catalog` | Catalog name |
| `iceberg.warehouse` | `s3://my-bucket/warehouse` | Warehouse path |
| `aws.region` | `us-east-1` | AWS region |
| `s3tables.warehouse` | - | S3 Tables table bucket ARN (required when `sink.target=s3tables`) |
| `sink.parallelism` | `2` | Write parallelism |
| `iceberg.branch` | `null` | Iceberg branch name |
| `checkpoint.interval` | `60000` | Checkpoint interval in ms |

## Schema Definition Format

When using `iceberg.schema`, specify fields as comma-separated `name:type` pairs:

```
iceberg.schema=user_id:string,action:string,score:int,timestamp:long,amount:decimal(10,2)
```

Supported types: `string`, `int`, `long`, `double`, `boolean`, `timestamp`, `date`, `decimal(p,s)`

## Build

```bash
mvn clean package -DskipTests
```

## Run on EMR (Flink)

```bash
flink run -m yarn-cluster \
  -c com.amazonaws.java.flink.KafkaJsonToIcebergDynamic \
  target/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers boot-xxx.kafka.us-east-1.amazonaws.com:9092 \
  --kafka.topic game-action-logs \
  --sink.target iceberg \
  --iceberg.catalog.type glue \
  --iceberg.warehouse s3://my-bucket/warehouse \
  --iceberg.namespace default \
  --iceberg.table game_action_logs
```

## Run with S3 Tables

```bash
flink run -m yarn-cluster \
  -c com.amazonaws.java.flink.KafkaJsonToIcebergDynamic \
  target/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers boot-xxx.kafka.us-east-1.amazonaws.com:9092 \
  --kafka.topic game-action-logs \
  --sink.target s3tables \
  --s3tables.warehouse arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket \
  --aws.region us-east-1
```

## Dynamic Table Routing Example

Route messages to different Iceberg tables based on a JSON field value:

```bash
flink run ... \
  --kafka.routing.field event_type \
  --iceberg.table default_events
```

With this config, a message `{"event_type": "login", "user": "alice"}` writes to table `login`, while a message without `event_type` writes to `default_events`.
