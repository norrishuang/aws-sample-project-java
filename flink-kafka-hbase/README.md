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
create 'default:user_order', 'info', 'transaction', 'product', 'work'
```

### Kafka Message Format

```json
{
  "uuid": "67f3fe83-42e7-4ab6-973d-376e909003fc",
  "customerId": 7691936731,
  "transactionAmount": 568678,
  "sourceIp": "192.171.92.115",
  "status": "PENDING",
  "transactionTime": "2026-03-04 01:22:20.717497",
  "user_name": "baitao",
  "phone_number": "13350069536",
  "product_id": 13,
  "product_name": "printer",
  "product_type": "self",
  "manufacturing_date": 2022,
  "price": 733.94,
  "unit": 23.0,
  "email": "fxiao@gmail.com",
  "address": "天津市建军县长寿韩街Z座 216135",
  "city": "柳市",
  "country": "东萨摩亚",
  "ip_address": "198.139.127.194",
  "website": "https://www.yuan.cn/",
  "company_name": "同兴万点信息有限公司",
  "department": "Operations",
  "salary": 982593.74,
  "age": 47,
  "gender": "Female",
  "created_date": "2023-04-28 23:28:08",
  "last_login": "2024-08-04 13:28:02",
  "score": 36
}
```

### Column Family Mapping

| HBase | Column Family | Columns |
|-------|--------------|---------|
| **Row Key** | — | `uuid` |
| **info** | User profile | `user_name`, `phone_number`, `email`, `address`, `city`, `country`, `ip_address`, `website`, `age`, `gender`, `created_date`, `last_login`, `score` |
| **transaction** | Transaction details | `customerId`, `transactionAmount`, `sourceIp`, `status`, `transactionTime` |
| **product** | Product info | `product_id`, `product_name`, `product_type`, `manufacturing_date`, `price`, `unit` |
| **work** | Employment info | `company_name`, `department`, `salary` |

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
