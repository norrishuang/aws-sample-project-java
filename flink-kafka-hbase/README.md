# flink-kafka-hbase

Flink application that consumes data from Kafka and writes to HBase using Flink SQL.

## Architecture

```
Kafka Topic (JSON / Debezium-JSON)
    │
    ▼
Flink SQL (Kafka Source Table)
    │
    ▼
Flink SQL (HBase Sink Table)
    │
    ▼
HBase (Amazon EMR 7.2)
```

## Version Compatibility

| Component | Version | Notes |
|-----------|---------|-------|
| Flink | 1.20.0 | Amazon Managed Service for Apache Flink |
| Flink Kafka Connector | 3.4.0-1.20 | Official 1.20 build |
| Flink HBase Connector | 4.0.0-1.19 | Latest available, compatible with Flink 1.20 |
| HBase | 2.4.17 | Amazon EMR 7.2 (2.4.17-amzn-6) |
| Hadoop | 3.3.6 | Amazon EMR 7.2 |
| Java | 11 | |

## HBase Table Setup

Pre-create the HBase table with required column families:

```bash
hbase shell
create 'default:user_order', 'info', 'product'
```

### Column Family Mapping

| HBase | Column Family | Columns |
|-------|--------------|---------|
| Row Key | — | `uuid_timestamp` (composite: uuid + '_' + epoch millis) |
| `info` | user info | `user_name`, `phone_number`, `ts` |
| `product` | product info | `product_id`, `product_name`, `product_type`, `manufacturing_date`, `price`, `unit` |

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `kafka_bootstrap_servers` | Yes | — | Kafka bootstrap servers |
| `topic` | No | `kafka_topic` | Kafka topic name |
| `kafka_group_id` | No | `flink-kafka-hbase-group` | Consumer group ID |
| `kafka_format` | No | `json` | Message format: `json` or `debezium-json` |
| `kafka_startup_mode` | No | `earliest-offset` | Startup mode |
| `hbase_table` | No | `default:user_order` | HBase table name |
| `hbase_zookeeper_quorum` | Yes | — | ZooKeeper quorum (e.g., `zk1:2181,zk2:2181`) |
| `hbase_zookeeper_znode` | No | `/hbase` | ZooKeeper znode parent |
| `checkpoint_interval` | No | `1 min` | Checkpoint interval |

## Build

```bash
mvn clean package -DskipTests
```

## Run

### Local

```bash
flink run -c com.amazonaws.java.flink.KafkaToHBase \
  target/flink-kafka-hbase-1.0-SNAPSHOT.jar \
  --kafka_bootstrap_servers localhost:9092 \
  --topic my_topic \
  --hbase_zookeeper_quorum localhost:2181 \
  --hbase_table default:user_order
```

### Amazon EMR 7.2

```bash
flink run -m yarn-cluster \
  -c com.amazonaws.java.flink.KafkaToHBase \
  flink-kafka-hbase-1.0-SNAPSHOT.jar \
  --kafka_bootstrap_servers b-1.kafka.xxx.amazonaws.com:9092 \
  --topic my_topic \
  --hbase_zookeeper_quorum zk1:2181,zk2:2181,zk3:2181 \
  --hbase_table default:user_order \
  --kafka_format debezium-json
```
