# AWS Glue Catalog 使用指南

## 概述

AWS Glue Catalog 是 AWS 提供的完全托管的元数据服务，是在 AWS 环境中使用 Iceberg 的推荐方案。

## 为什么选择 Glue Catalog？

### 优势

✅ **完全托管**
- 无需部署和维护 Hive Metastore
- AWS 自动处理备份、高可用和扩展

✅ **深度集成**
- AWS Athena：直接查询 Iceberg 表
- Amazon EMR：使用 Spark/Presto 分析
- AWS Glue ETL：数据转换和处理
- Amazon Redshift Spectrum：联邦查询
- Amazon QuickSight：可视化分析

✅ **安全性**
- 使用 IAM 进行细粒度权限控制
- 支持 VPC 端点，数据不出 VPC
- 与 AWS KMS 集成进行加密

✅ **成本优化**
- 按使用量付费，无固定成本
- 前 100 万次请求/月免费
- 存储元数据免费

### 与其他 Catalog 的对比

| 特性 | Glue Catalog | Hive Metastore | Hadoop Catalog |
|------|-------------|----------------|----------------|
| 托管服务 | ✅ 完全托管 | ❌ 需自行维护 | ❌ 无元数据服务 |
| 高可用 | ✅ 自动 | ⚠️ 需配置 | ❌ 单点故障 |
| AWS 集成 | ✅ 原生支持 | ⚠️ 需配置 | ⚠️ 有限支持 |
| 权限控制 | ✅ IAM | ⚠️ Ranger/Sentry | ❌ 文件系统权限 |
| 成本 | 💰 按量付费 | 💰💰 EC2 成本 | 💰 存储成本 |
| 适用场景 | AWS 生产环境 | 混合云/本地 | 开发测试 |

## 配置指南

### 1. IAM 权限配置

#### 最小权限策略

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchGetPartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:123456789012:catalog",
        "arn:aws:glue:us-east-1:123456789012:database/cdc_*",
        "arn:aws:glue:us-east-1:123456789012:table/cdc_*/*"
      ]
    },
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::my-data-lake/iceberg/*"
    },
    {
      "Sid": "S3BucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::my-data-lake"
    }
  ]
}
```

#### 生产环境推荐策略

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCatalogFullAccess",
      "Effect": "Allow",
      "Action": "glue:*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "us-east-1"
        }
      }
    },
    {
      "Sid": "S3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-lake",
        "arn:aws:s3:::my-data-lake/*"
      ]
    },
    {
      "Sid": "KMSEncryption",
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/your-kms-key-id"
    }
  ]
}
```

### 2. Flink 作业配置

#### 基础配置

```properties
# Glue Catalog 配置
iceberg.catalog.type=glue
iceberg.catalog.name=glue_catalog
iceberg.warehouse=s3://my-data-lake/iceberg
iceberg.namespace=cdc_database
iceberg.table.name=mysql_cdc_table

# AWS 区域
aws.region=us-east-1
```

#### 高级配置

```properties
# Glue Catalog 高级配置
iceberg.catalog.type=glue
iceberg.warehouse=s3://my-data-lake/iceberg

# 指定 AWS 账户 ID（跨账户访问）
iceberg.glue.catalog.id=123456789012

# 跳过 Glue 名称验证（允许特殊字符）
iceberg.glue.skip.name.validation=true

# 自定义 Glue 端点（VPC 端点）
iceberg.glue.endpoint=https://vpce-xxx.glue.us-east-1.vpce.amazonaws.com

# S3 配置
iceberg.s3.endpoint=https://s3.us-east-1.amazonaws.com
iceberg.s3.path.style.access=false

# AWS 区域
aws.region=us-east-1
```

### 3. 部署到不同 AWS 服务

#### Amazon Managed Service for Apache Flink (KDA)

```json
{
  "ApplicationConfiguration": {
    "FlinkApplicationConfiguration": {
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 4,
        "ParallelismPerKPU": 1
      }
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "FlinkApplicationProperties",
          "PropertyMap": {
            "iceberg.catalog.type": "glue",
            "iceberg.warehouse": "s3://my-data-lake/iceberg",
            "iceberg.namespace": "cdc_db",
            "aws.region": "us-east-1",
            "sink.immediate.table.update": "true"
          }
        }
      ]
    }
  }
}
```

#### Amazon EMR on EKS

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
data:
  flink-conf.yaml: |
    taskmanager.numberOfTaskSlots: 2
    parallelism.default: 4
  application.properties: |
    iceberg.catalog.type=glue
    iceberg.warehouse=s3://my-data-lake/iceberg
    aws.region=us-east-1
```

#### Amazon EKS (自托管 Flink)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-cdc-job
spec:
  template:
    spec:
      serviceAccountName: flink-sa  # 需要配置 IRSA
      containers:
      - name: flink-job
        image: flink:1.20.0
        env:
        - name: AWS_REGION
          value: "us-east-1"
        volumeMounts:
        - name: config
          mountPath: /opt/flink/conf
      volumes:
      - name: config
        configMap:
          name: flink-config
```

## 使用场景

### 场景 1: 实时 CDC 到数据湖

```
MySQL → Flink CDC → Iceberg (Glue Catalog) → Athena/EMR
```

**优势**:
- 实时数据同步
- 自动 schema 演化
- 支持时间旅行查询

### 场景 2: 多源数据整合

```
MySQL    ↘
PostgreSQL → Flink → Iceberg (Glue Catalog) → 统一数据湖
MongoDB  ↗
```

**优势**:
- 统一的元数据管理
- 跨数据源查询
- 简化数据治理

### 场景 3: 数据湖分析

```
Iceberg (Glue Catalog) → Athena → QuickSight
                       → EMR Spark → ML 模型
                       → Redshift Spectrum → BI 报表
```

**优势**:
- 多种分析工具支持
- 无需数据复制
- 统一的数据视图

## 最佳实践

### 1. 命名规范

```properties
# 使用有意义的命名
iceberg.namespace=production_cdc  # 而不是 db1
iceberg.table.name=orders_realtime  # 而不是 table1

# 使用环境前缀
iceberg.namespace=dev_cdc    # 开发环境
iceberg.namespace=prod_cdc   # 生产环境
```

### 2. 分区策略

```java
// 按日期分区（推荐用于时序数据）
PartitionSpec.builderFor(schema)
    .day("created_at")
    .build();

// 按小时分区（高频数据）
PartitionSpec.builderFor(schema)
    .hour("created_at")
    .build();

// 多维度分区
PartitionSpec.builderFor(schema)
    .day("created_at")
    .identity("region")
    .build();
```

### 3. 性能优化

```properties
# 增加并行度
sink.parallelism=8

# 调整缓存大小
sink.cache.max.size=200
sink.cache.refresh.ms=30000

# 启用压缩
table.write.format.default=parquet
table.write.parquet.compression-codec=snappy
```

### 4. 监控和告警

#### CloudWatch 指标

```python
# 创建自定义指标
import boto3

cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_data(
    Namespace='FlinkCDC',
    MetricData=[
        {
            'MetricName': 'RecordsProcessed',
            'Value': 1000,
            'Unit': 'Count'
        },
        {
            'MetricName': 'SchemaEvolutions',
            'Value': 1,
            'Unit': 'Count'
        }
    ]
)
```

#### CloudWatch 告警

```bash
# 创建告警
aws cloudwatch put-metric-alarm \
  --alarm-name flink-cdc-high-lag \
  --alarm-description "CDC lag is too high" \
  --metric-name RecordsLag \
  --namespace FlinkCDC \
  --statistic Average \
  --period 300 \
  --threshold 10000 \
  --comparison-operator GreaterThanThreshold
```

### 5. 安全加固

#### 启用 S3 加密

```properties
# 使用 SSE-S3
table.write.object-storage.enabled=true
table.write.object-storage.path=s3://encrypted-bucket/

# 使用 SSE-KMS
s3.sse.type=kms
s3.sse.key=arn:aws:kms:us-east-1:123456789012:key/xxx
```

#### 使用 VPC 端点

```properties
# Glue VPC 端点
iceberg.glue.endpoint=https://vpce-xxx.glue.us-east-1.vpce.amazonaws.com

# S3 VPC 端点
iceberg.s3.endpoint=https://bucket.vpce-xxx.s3.us-east-1.vpce.amazonaws.com
```

## 故障排查

### 常见问题

#### 1. AccessDeniedException

**错误**:
```
AccessDeniedException: User is not authorized to perform: glue:CreateTable
```

**解决方案**:
- 检查 IAM 角色权限
- 确认资源 ARN 正确
- 验证区域配置

#### 2. S3 403 错误

**错误**:
```
Access Denied (Service: Amazon S3; Status Code: 403)
```

**解决方案**:
- 检查 S3 bucket 策略
- 确认 IAM 角色有 S3 权限
- 验证 bucket 区域

#### 3. Glue 数据库不存在

**错误**:
```
EntityNotFoundException: Database cdc_db not found
```

**解决方案**:
```bash
# 手动创建数据库
aws glue create-database \
  --database-input '{"Name":"cdc_db","Description":"CDC database"}'
```

#### 4. Schema 不兼容

**错误**:
```
SchemaEvolutionException: Cannot evolve schema
```

**解决方案**:
- 检查 schema 变更是否兼容
- 使用 `sink.immediate.table.update=true`
- 查看 Flink 日志了解详细错误

### 调试技巧

#### 启用详细日志

```properties
# log4j.properties
log4j.logger.org.apache.iceberg=DEBUG
log4j.logger.org.apache.iceberg.aws.glue=DEBUG
log4j.logger.com.amazonaws.java.flink=DEBUG
```

#### 使用 AWS CLI 检查

```bash
# 查看数据库
aws glue get-database --name cdc_db

# 查看表
aws glue get-table --database-name cdc_db --name orders

# 查看表版本历史
aws glue get-table-versions \
  --database-name cdc_db \
  --table-name orders \
  --max-results 10
```

## 成本优化

### Glue Catalog 定价

- 前 100 万次请求/月：免费
- 超过 100 万次：$1.00 / 100 万次请求
- 元数据存储：免费

### 优化建议

1. **使用缓存**
   ```properties
   sink.cache.max.size=200  # 减少 Glue API 调用
   ```

2. **批量操作**
   ```properties
   sink.parallelism=4  # 而不是 16
   ```

3. **合理的检查点间隔**
   ```properties
   checkpoint.interval=300000  # 5 分钟
   ```

## 参考资料

- [AWS Glue 官方文档](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg AWS 集成](https://iceberg.apache.org/docs/latest/aws/)
- [Flink CDC 文档](https://ververica.github.io/flink-cdc-connectors/)
- [AWS Athena Iceberg 支持](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)

---

**最后更新**: 2026-02-02  
**版本**: v2.2
