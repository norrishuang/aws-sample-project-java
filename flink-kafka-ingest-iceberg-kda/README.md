# Flink Kafka to Iceberg Integration

这是一个基于 Apache Flink 的实时数据处理应用程序，将 Kafka 数据流实时写入 Iceberg 表。该应用程序支持在 Amazon Kinesis Data Analytics (KDA) 和 EMR Flink 上运行。

## 功能特性

- 从 Kafka 主题读取实时数据流
- 使用 Flink SQL 进行流处理
- 将数据写入 Iceberg 表（存储在 S3）
- 支持实时聚合统计
- 使用 AWS Glue Catalog 作为元数据存储
- 支持状态管理和检查点
- **兼容 EMR Flink 和 KDA 两种部署环境**

## 数据流架构

```
Kafka Topic → Flink Processing → Iceberg Tables (S3)
                    ↓
              Real-time Aggregation
```

## 配置参数

应用程序支持以下配置参数：

- `kafka_topic`: Kafka 主题名称（默认: "ev_station_data"）
- `kafka_bootstrap_servers`: Kafka 集群地址（默认: "localhost:9092"）
- `iceberg_warehouse`: Iceberg 仓库路径（默认: "thrift://localhost:9083"）

### KDA 环境配置

在 KDA 控制台配置应用程序属性：
```json
{
  "FlinkApplicationProperties": {
    "kafka_topic": "your-kafka-topic",
    "kafka_bootstrap_servers": "your-kafka-bootstrap-servers",
    "iceberg_warehouse": "s3a://your-bucket/warehouse"
  }
}
```

### EMR Flink 环境配置

通过命令行参数传递配置：
```bash
flink run -c com.amazonaws.java.flink.kda.IcebergApplication \
  flink-kafka-ingest-iceberg-kda-1.0-SNAPSHOT.jar \
  --kafka_topic your-kafka-topic \
  --kafka_bootstrap_servers your-kafka-bootstrap-servers \
  --iceberg_warehouse s3a://your-bucket/warehouse
```

## 数据模式

输入数据模式（JSON 格式）：
```json
{
  "customerId": 12345,
  "transactionAmount": 1000,
  "sourceIp": "192.168.1.1",
  "status": "SUCCESS",
  "transactionTime": "2024-01-01T10:00:00.000Z"
}
```

## 输出表

应用程序创建两个 Iceberg 表：

1. **customer_info_flinksql_03**: 原始数据表
2. **customer_info_stat**: 聚合统计表（按客户ID统计交易次数）

## 本地开发

### 前置条件

- Java 11+
- Maven 3.6+
- Kafka 集群
- S3 存储桶（用于 Iceberg 数据存储）
- AWS Glue Catalog

### 构建

```bash
mvn clean package
```

### 本地运行

```bash
java -cp target/flink-kafka-ingest-iceberg-kda-1.0-SNAPSHOT.jar \
  com.amazonaws.java.flink.kda.IcebergApplication \
  --kafka_topic your-topic \
  --kafka_bootstrap_servers your-kafka-servers \
  --iceberg_warehouse s3a://your-bucket/warehouse
```

## KDA 部署

1. 构建 JAR 包：
   ```bash
   mvn clean package
   ```

2. 上传 JAR 到 S3

3. 在 KDA 控制台创建应用程序，配置以下应用程序属性：
   ```json
   {
     "FlinkApplicationProperties": {
       "kafka_topic": "your-kafka-topic",
       "kafka_bootstrap_servers": "your-kafka-bootstrap-servers",
       "iceberg_warehouse": "s3a://your-bucket/warehouse"
     }
   }
   ```

## 性能优化

应用程序包含以下性能优化配置：

- 检查点间隔：1分钟
- 状态 TTL：10分钟
- Mini-batch 处理：启用
- 两阶段聚合策略

## 监控

应用程序使用 SLF4J 进行日志记录，可以通过 CloudWatch 监控应用程序状态和性能指标。

## 故障排除

常见问题和解决方案：

1. **Kafka 连接问题**: 检查 bootstrap servers 配置和网络连接
2. **S3 权限问题**: 确保 KDA 执行角色有 S3 读写权限
3. **Glue Catalog 权限**: 确保有 Glue 数据库和表的访问权限
4. **NoClassDefFoundError: org.apache.iceberg.GenericDataFile**: 
   - 这通常是由于依赖版本冲突导致的
   - 确保使用兼容的 Hadoop (3.3.4) 和 Parquet (1.13.1) 版本
   - 检查类路径中是否有冲突的依赖项
   - 如果在 EMR 环境中运行，确保 EMR 版本与 Flink 1.20.0 兼容

## 依赖版本

- Flink: 1.20.0
- Iceberg: 1.10.0
- Kafka Connector: 3.3.0-1.20
- Hadoop: 3.3.4
- Jackson: 2.15.2
- Parquet: 1.13.1

## 部署说明

### EMR Flink 部署

1. 构建 JAR 包：
   ```bash
   mvn clean package
   ```

2. 上传 JAR 到 S3 或 EMR 集群

3. 提交 Flink 作业：
   ```bash
   flink run -c com.amazonaws.java.flink.kda.IcebergApplication \
     flink-kafka-ingest-iceberg-kda-1.0-SNAPSHOT.jar \
     --kafka_topic your-kafka-topic \
     --kafka_bootstrap_servers your-kafka-bootstrap-servers \
     --iceberg_warehouse s3a://your-bucket/warehouse
   ```

### KDA 部署

1. 构建 JAR 包并上传到 S3

2. 在 KDA 控制台创建应用程序，配置应用程序属性（见上面的配置示例）

## 环境兼容性

应用程序会自动检测运行环境：
- **KDA 环境**: 使用 `KinesisAnalyticsRuntime.getApplicationProperties()` 获取配置
- **EMR 环境**: 使用命令行参数获取配置
- **本地环境**: 使用命令行参数获取配置