# KDA Deployment Guide — Flink Kafka → Iceberg Dynamic

Deploy this Flink application on **Amazon Managed Service for Apache Flink** (formerly Kinesis Data Analytics).

---

## 一、上传 JAR 到 S3

```bash
mvn clean package -DskipTests

aws s3 cp target/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar \
  s3://YOUR_BUCKET/flink-jars/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar
```

---

## 二、创建 KDA 应用

### 方式 A：使用 JSON 配置文件（推荐）

**Glue Catalog 场景：**

```bash
aws kinesisanalyticsv2 create-application \
  --cli-input-json file://kda-application-glue.json
```

**S3 Tables 场景：**

```bash
aws kinesisanalyticsv2 create-application \
  --cli-input-json file://kda-application-s3tables.json
```

### 方式 B：AWS CLI 内联命令

```bash
aws kinesisanalyticsv2 create-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --runtime-environment FLINK-1_20 \
  --service-execution-role arn:aws:iam::YOUR_ACCOUNT_ID:role/KDA-Flink-Role \
  --application-configuration '{
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "S3ContentLocation": {
          "BucketARN": "arn:aws:s3:::YOUR_BUCKET",
          "FileKey": "flink-jars/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar"
        }
      },
      "CodeContentType": "ZIPFILE"
    },
    "FlinkApplicationConfiguration": {
      "CheckpointConfiguration": {
        "ConfigurationType": "CUSTOM",
        "CheckpointingEnabled": true,
        "CheckpointInterval": 60000,
        "MinPauseBetweenCheckpoints": 30000
      },
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 4,
        "ParallelismPerKPU": 1,
        "AutoScalingEnabled": true
      }
    },
    "EnvironmentProperties": {
      "PropertyGroups": [{
        "PropertyGroupId": "FlinkApplicationProperties",
        "PropertyMap": {
          "kafka.bootstrap.servers": "YOUR_MSK_BOOTSTRAP_SERVERS",
          "kafka.topic": "game-action-logs",
          "kafka.group.id": "flink-kafka-iceberg-dynamic",
          "kafka.startup.mode": "latest-offset",
          "sink.target": "iceberg",
          "iceberg.catalog.type": "glue",
          "iceberg.catalog.name": "glue_catalog",
          "iceberg.warehouse": "s3://YOUR_WAREHOUSE_BUCKET/warehouse",
          "iceberg.namespace": "default",
          "iceberg.table": "game_action_logs",
          "aws.region": "us-east-1",
          "sink.parallelism": "4",
          "checkpoint.interval": "60000"
        }
      }]
    }
  }'
```

---

## 三、启动 / 停止 / 更新

### 启动应用

```bash
aws kinesisanalyticsv2 start-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --run-configuration '{
    "FlinkRunConfiguration": {
      "AllowNonRestoredState": true
    }
  }'
```

### 停止应用

```bash
aws kinesisanalyticsv2 stop-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --force true
```

### 更新应用代码（上传新 JAR 后）

```bash
# 获取当前版本号
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --query 'ApplicationDetail.ApplicationVersionId' --output text)

aws kinesisanalyticsv2 update-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --current-application-version-id $CURRENT_VERSION \
  --application-configuration-update '{
    "ApplicationCodeConfigurationUpdate": {
      "CodeContentUpdate": {
        "S3ContentLocationUpdate": {
          "BucketARNUpdate": "arn:aws:s3:::YOUR_BUCKET",
          "FileKeyUpdate": "flink-jars/flink-kafka-iceberg-dynamic-1.0-SNAPSHOT.jar"
        }
      }
    }
  }'
```

### 更新环境参数

```bash
aws kinesisanalyticsv2 update-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --current-application-version-id $CURRENT_VERSION \
  --application-configuration-update '{
    "EnvironmentPropertyUpdates": {
      "PropertyGroups": [{
        "PropertyGroupId": "FlinkApplicationProperties",
        "PropertyMap": {
          "kafka.topic": "new-topic-name",
          "sink.parallelism": "8"
        }
      }]
    }
  }'
```

---

## 四、CloudWatch 日志配置

### 创建日志组

```bash
aws logs create-log-group --log-group-name /aws/kinesis-analytics/flink-kafka-iceberg-dynamic-glue

aws logs create-log-stream \
  --log-group-name /aws/kinesis-analytics/flink-kafka-iceberg-dynamic-glue \
  --log-stream-name kinesis-analytics-log-stream
```

### 添加日志配置到应用

```bash
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --query 'ApplicationDetail.ApplicationVersionId' --output text)

aws kinesisanalyticsv2 add-application-cloud-watch-logging-option \
  --application-name flink-kafka-iceberg-dynamic-glue \
  --current-application-version-id $CURRENT_VERSION \
  --cloud-watch-logging-option '{
    "LogStreamARN": "arn:aws:logs:us-east-1:YOUR_ACCOUNT_ID:log-group:/aws/kinesis-analytics/flink-kafka-iceberg-dynamic-glue:log-stream:kinesis-analytics-log-stream"
  }'
```

---

## 五、IAM 权限清单

KDA 执行角色需要以下权限（完整 Policy 见 `iam-policy-glue.json` 和 `iam-policy-s3tables.json`）：

### 通用权限

| 服务 | Actions | 资源 |
|------|---------|------|
| MSK | `kafka-cluster:Connect`, `kafka-cluster:DescribeGroup`, `kafka-cluster:AlterGroup`, `kafka-cluster:DescribeTopic`, `kafka-cluster:ReadData` | MSK Cluster ARN, Topic ARN, Group ARN |
| S3 (代码) | `s3:GetObject` | JAR 所在 S3 bucket/key |
| CloudWatch Logs | `logs:PutLogEvents`, `logs:DescribeLogGroups`, `logs:DescribeLogStreams` | Log group ARN |

### Glue Catalog 场景额外权限

| 服务 | Actions | 资源 |
|------|---------|------|
| S3 (Warehouse) | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` | Warehouse bucket |
| Glue | `glue:GetDatabase`, `glue:CreateDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:GetTables`, `glue:DeleteTable` | Catalog/Database/Table |

### S3 Tables 场景额外权限

| 服务 | Actions | 资源 |
|------|---------|------|
| S3 Tables | `s3tables:GetTableBucket`, `s3tables:ListTableBuckets`, `s3tables:CreateNamespace`, `s3tables:GetNamespace`, `s3tables:ListNamespaces`, `s3tables:CreateTable`, `s3tables:GetTable`, `s3tables:ListTables`, `s3tables:UpdateTableMetadataLocation`, `s3tables:GetTableMetadataLocation`, `s3tables:PutTableData`, `s3tables:GetTableData` | Table bucket ARN |

---

## 六、KDA 应用参数对照表

以下参数通过 `EnvironmentProperties.PropertyGroups[FlinkApplicationProperties].PropertyMap` 传入：

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `kafka.bootstrap.servers` | `localhost:9092` | MSK broker 地址 |
| `kafka.topic` | `game-action-logs` | 消费的 Kafka topic |
| `kafka.group.id` | `flink-kafka-iceberg-dynamic` | Consumer group ID |
| `kafka.startup.mode` | `latest-offset` | `earliest-offset` / `latest-offset` / `group-offsets` |
| `iceberg.namespace` | `default` | Iceberg namespace |
| `iceberg.table` | `kafka_json_table` | 表名（FIXED 模式）或 fallback 表名（FIELD 模式） |
| `kafka.routing.field` | `null` | 动态路由字段名（null = FIXED 模式） |
| `iceberg.schema` | `null` | 预定义 schema：`"field1:type1,field2:type2,..."` |
| `sink.target` | `iceberg` | `iceberg` 或 `s3tables` |
| `iceberg.catalog.type` | `hadoop` | `hadoop` / `glue` / `rest` / `hive` |
| `iceberg.catalog.name` | `iceberg_catalog` | Catalog 名称 |
| `iceberg.warehouse` | `s3://my-bucket/warehouse` | Warehouse 路径 |
| `aws.region` | `us-east-1` | AWS 区域 |
| `s3tables.warehouse` | - | S3 Tables table bucket ARN（`sink.target=s3tables` 时必填） |
| `sink.parallelism` | `2` | 写入并行度 |
| `iceberg.branch` | `null` | Iceberg branch 名称 |
| `checkpoint.interval` | `60000` | Checkpoint 间隔（ms） |

---

## 七、故障排查

### 常见问题

1. **应用启动失败 — ClassNotFoundException**
   - 确认 JAR 使用 `maven-shade-plugin` 打包（uber-jar）
   - 确认 `aws-kinesisanalytics-runtime` 依赖 scope 为 `provided`

2. **无法连接 MSK**
   - 确认 KDA 应用 VPC 配置与 MSK 在同一 VPC/子网
   - 确认安全组允许 9092/9094 端口

3. **Glue 权限错误**
   - 确认 IAM Role 包含 Glue 相关权限
   - 确认 `iceberg.warehouse` S3 路径有读写权限

4. **Checkpoint 失败**
   - 检查 S3 checkpoint 路径权限
   - 适当增大 `checkpoint.interval`
