# 项目总结

## 项目概述

**flink-cdc-mysql-s3tables** 是一个完整的 Apache Flink 应用，实现了从 MySQL 数据库通过 CDC (Change Data Capture) 实时捕获数据变更，并写入到 AWS S3 Tables (基于 Apache Iceberg 和 REST Catalog)。

## 核心技术栈

- **Apache Flink 1.20.0** - 流处理引擎
- **Flink CDC 3.5.0** - MySQL 变更数据捕获
- **Apache Iceberg 1.6.1** - 表格式和 REST Catalog
- **AWS SDK v2 (2.20.160)** - SigV4 签名认证
  - `software.amazon.awssdk:bundle` - AWS SDK 核心
  - `software.amazon.awssdk:url-connection-client` - HTTP 客户端
- **AWS S3 Tables** - 托管的 Iceberg 表服务
- **Java 11** - 开发语言

## 关键特性

### 1. MySQL CDC 集成
- 实时捕获 MySQL 数据库变更（INSERT、UPDATE、DELETE）
- 支持增量快照读取，提高初始数据加载效率
- 可配置的批处理大小和队列大小
- 自动处理 schema 变更

### 2. S3 Tables 支持
- 使用 Iceberg REST Catalog 协议
- 支持 AWS SigV4 签名认证
- 自动 schema 演化
- 事务性写入保证

### 3. 生产就绪
- 完整的 checkpoint 和故障恢复机制
- 支持本地开发和 AWS Managed Flink 部署
- 详细的监控和日志
- 灵活的配置管理

## 与 Spark 配置对比

本项目参考了 Spark 的 S3 Tables 配置方式，并适配到 Flink：

| 配置项 | Spark | Flink (本项目) |
|--------|-------|---------------|
| Catalog 实现 | SparkCatalog | RESTCatalog |
| SigV4 认证 | rest.sigv4-enabled | rest.sigv4-enabled |
| 签名服务 | rest.signing-name | rest.signing-name |
| 签名区域 | rest.signing-region | rest.signing-region |
| Warehouse | ARN 格式 | ARN 格式 |
| IO 实现 | S3FileIO | S3FileIO |

## 项目文件说明

### 核心代码
- **MySQLCDCToS3Tables.java** (200+ 行)
  - 主应用程序入口
  - MySQL CDC 源配置
  - Iceberg REST Catalog 配置（含 SigV4 认证）
  - 表创建和数据管道逻辑

### 配置文件
- **pom.xml** - Maven 依赖管理，包含所有必需的库
- **config-example.properties** - 配置参数模板
- **.gitignore** - Git 版本控制忽略规则

### 文档
- **README.md** - 主文档，包含功能介绍、配置说明、部署指南
- **QUICKSTART.md** - 5 分钟快速启动指南
- **S3TABLES_CONFIG.md** - S3 Tables REST Catalog 详细配置说明
- **DEPLOYMENT.md** - AWS Managed Flink 完整部署指南

### 脚本
- **run-local.sh** - 本地运行脚本，简化启动流程

## 配置要点

### MySQL CDC 配置
```properties
mysql.hostname=your-mysql-host
mysql.port=3306
mysql.username=flink_cdc
mysql.password=password
mysql.database=testdb
mysql.table=user_order_list
mysql.server.timezone=UTC
```

### S3 Tables 配置（关键）
```properties
# REST Catalog endpoint
iceberg.rest.uri=https://s3tables.us-east-1.amazonaws.com/iceberg

# Warehouse ARN (不是普通 S3 路径)
iceberg.warehouse.arn=arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket

# Namespace 和表名
iceberg.namespace=my_namespace
iceberg.table.name=user_order_list

# AWS 区域
aws.region=us-east-1
```

### Flink Catalog 配置（关键）
```sql
CREATE CATALOG s3tables_catalog WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'https://s3tables.us-east-1.amazonaws.com/iceberg',
  'warehouse' = 'arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  'rest.sigv4-enabled' = 'true',              -- 必需
  'rest.signing-name' = 's3tables',           -- 必需
  'rest.signing-region' = 'us-east-1',        -- 必需
  'client.region' = 'us-east-1',
  'rest-metrics-reporting-enabled' = 'false'
);
```

## 部署选项

### 1. 本地开发
```bash
mvn clean package
./run-local.sh
```

### 2. AWS Managed Flink
- 上传 JAR 到 S3
- 创建 Managed Flink 应用
- 配置应用属性
- 启动应用

详见 [DEPLOYMENT.md](DEPLOYMENT.md)

## IAM 权限要求

应用需要以下权限：
- **S3 Tables**: GetTable, CreateTable, UpdateTable, PutTableData
- **S3**: GetObject, PutObject, DeleteObject, ListBucket
- **CloudWatch**: PutMetricData, CreateLogStream, PutLogEvents
- **VPC**: 如果 MySQL 在 VPC 内，需要网络接口权限

## 监控指标

- **KPUs**: Kinesis Processing Units 使用量
- **cpuUtilization**: CPU 使用率
- **heapMemoryUtilization**: 堆内存使用率
- **numRecordsIn**: 输入记录数
- **numRecordsOut**: 输出记录数
- **lastCheckpointDuration**: Checkpoint 持续时间

## 性能特点

- **吞吐量**: 取决于 MySQL binlog 速率和 Flink 并行度
- **延迟**: 秒级（从 MySQL 变更到 S3 Tables）
- **可扩展性**: 支持水平扩展（增加并行度）
- **容错性**: 基于 checkpoint 的精确一次语义

## 使用场景

1. **实时数据湖**: 将 MySQL 数据实时同步到数据湖
2. **数据仓库 ETL**: 构建实时数据仓库
3. **数据分析**: 为分析工具提供实时数据
4. **数据备份**: 实时备份 MySQL 数据到 S3
5. **多数据源整合**: 与其他数据源整合到统一的 Iceberg 表

## 优势

1. **实时性**: 毫秒级捕获 MySQL 变更
2. **可靠性**: Flink checkpoint 保证数据不丢失
3. **可扩展**: 支持大规模数据处理
4. **托管服务**: 使用 AWS Managed Flink 减少运维负担
5. **标准化**: 基于 Apache Iceberg 开放表格式
6. **兼容性**: 数据可被 Spark、Athena、Presto 等查询

## 限制和注意事项

1. **MySQL 版本**: 需要 MySQL 5.7+ 或 8.0+
2. **Binlog 格式**: 必须是 ROW 格式
3. **网络**: MySQL 和 S3 Tables 需要网络可达
4. **权限**: 需要适当的 MySQL 和 AWS IAM 权限
5. **成本**: AWS Managed Flink 和 S3 Tables 会产生费用

## 后续改进方向

1. **多表支持**: 支持同时捕获多个 MySQL 表
2. **Schema 注册**: 集成 Schema Registry
3. **数据转换**: 添加数据清洗和转换逻辑
4. **监控增强**: 添加自定义业务指标
5. **性能优化**: 批量写入优化

## 技术亮点

1. **SigV4 认证**: 正确实现了 AWS S3 Tables 的 SigV4 签名认证
2. **REST Catalog**: 使用 Iceberg REST Catalog 协议
3. **增量快照**: 优化了初始数据加载性能
4. **配置灵活**: 支持多种配置方式（命令行、KDA 属性）
5. **文档完善**: 提供了详细的配置和部署文档

## 参考资料

- [Apache Flink](https://flink.apache.org/)
- [Flink CDC Connectors](https://github.com/ververica/flink-cdc-connectors)
- [Apache Iceberg](https://iceberg.apache.org/)
- [AWS S3 Tables](https://docs.aws.amazon.com/s3tables/)
- [AWS Managed Flink](https://docs.aws.amazon.com/managed-flink/)

## 版本信息

- **项目版本**: 1.0-SNAPSHOT
- **Flink 版本**: 1.20.0
- **Flink CDC 版本**: 3.5.0
- **Iceberg 版本**: 1.6.1
- **Java 版本**: 11

## 许可证

Apache License 2.0

---

**创建日期**: 2026-01-22
**最后更新**: 2026-01-22
