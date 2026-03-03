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

Parameters are loaded from **Amazon Managed Flink `FlinkApplicationProperties`** at runtime, with fallback to command-line arguments (for local/EMR).

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `kafka.bootstrap.servers` | Yes | `localhost:9092` | Kafka bootstrap servers |
| `kafka.topic` | No | `kafka_topic` | Kafka topic name |
| `kafka.group.id` | No | `flink-kafka-hbase-group` | Consumer group ID |
| `kafka.format` | No | `json` | Message format: `json` or `debezium-json` |
| `kafka.startup.mode` | No | `earliest-offset` | Startup mode |
| `hbase.table` | No | `default:user_order` | HBase table name |
| `hbase.zookeeper.quorum` | Yes | `localhost:2181` | ZooKeeper quorum (e.g., `zk1:2181,zk2:2181`) |
| `hbase.zookeeper.znode` | No | `/hbase` | ZooKeeper znode parent |
| `checkpoint.interval` | No | `1 min` | Checkpoint interval |

## Build

```bash
mvn clean package -DskipTests
```

## Run

### Local

```bash
flink run -c com.amazonaws.java.flink.KafkaToHBase \
  target/flink-kafka-hbase-1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers localhost:9092 \
  --kafka.topic my_topic \
  --hbase.zookeeper.quorum localhost:2181 \
  --hbase.table default:user_order
```

### Amazon Managed Service for Apache Flink

Configure the following in `FlinkApplicationProperties`:

```
kafka.bootstrap.servers = b-1.kafka.xxx.amazonaws.com:9092
kafka.topic = my_topic
kafka.format = debezium-json
hbase.zookeeper.quorum = zk1:2181,zk2:2181,zk3:2181
hbase.table = default:user_order
```
