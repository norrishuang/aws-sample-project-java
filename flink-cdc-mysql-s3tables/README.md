# Flink CDC MySQL to AWS S3 Tables

这个项目实现了从 MySQL 数据库通过 Flink CDC 实时捕获变更数据，并写入到 AWS S3 Tables (使用 Iceberg REST Catalog)。


## 📁 项目结构

```
flink-cdc-mysql-s3tables/
├── src/main/java/com/amazonaws/java/flink/
│   └── MySQLCDCToS3Tables.java          # 主应用程序
├── pom.xml                               # Maven 依赖配置
├── README.md                             # 项目说明文档
├── config-example.properties             # 配置参数示例
└── .gitignore                            # Git 忽略文件
```

## 功能特性

- **MySQL CDC 源**: 使用 Flink CDC 连接器实时捕获 MySQL 数据库的变更
- **增量快照**: 支持增量快照读取，提高初始数据加载效率
- **S3 Tables 集成**: 使用 Iceberg REST Catalog 写入 AWS S3 Tables
- **自动 Schema 管理**: Iceberg 自动处理 schema 演化
- **高可用性**: 支持 checkpoint 和故障恢复

## 架构

```
MySQL Database (CDC) 
    ↓
Flink CDC Connector
    ↓
Flink Stream Processing
    ↓
Iceberg REST Catalog (S3 Tables)
    ↓
AWS S3 (Data Storage)
```

## 依赖说明

### 核心依赖

1. **Flink CDC 3.5.0** - MySQL 变更数据捕获
2. **Apache Iceberg 1.6.1** - 表格式和 REST Catalog
3. **AWS SDK v2 (2.20.160)** - AWS SigV4 签名认证（S3 Tables 必需）
   - `software.amazon.awssdk:bundle` - AWS SDK 核心功能
   - `software.amazon.awssdk:url-connection-client` - HTTP 连接客户端

### 为什么需要 AWS SDK v2？

S3 Tables 使用 REST Catalog 协议，并要求所有请求使用 AWS SigV4 签名。这两个依赖提供：

- **bundle**: AWS SDK 核心功能，包括 SigV4 签名器
- **url-connection-client**: HTTP 客户端，用于发送签名的 REST 请求

虽然 `iceberg-aws-bundle` 包含了一些 AWS 功能，但显式添加这些依赖可以确保：
- SigV4 签名正确实现
- HTTP 连接稳定可靠
- 版本兼容性

## 前置条件

1. **MySQL 数据库**
   - MySQL 5.7+ 或 8.0+
   - 启用 binlog (ROW 格式)
   - 创建具有复制权限的用户

2. **AWS S3 Tables**
   - 创建 S3 Tables warehouse
   - 配置 IAM 权限访问 S3 Tables
   - 获取 REST Catalog endpoint

3. **Flink 环境**
   - Apache Flink 1.20.0
   - Java 11+
   - AWS Managed Service for Apache Flink (可选)

## MySQL 配置

确保 MySQL 启用了 binlog:

```sql
-- 检查 binlog 状态
SHOW VARIABLES LIKE 'log_bin';

-- 检查 binlog 格式
SHOW VARIABLES LIKE 'binlog_format';

-- 创建 CDC 用户
CREATE USER 'flink_cdc'@'%' IDENTIFIED BY 'password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';
FLUSH PRIVILEGES;
```

## 配置参数

详细的 S3 Tables 配置说明请参考 [S3TABLES_CONFIG.md](S3TABLES_CONFIG.md)

### MySQL 配置
- `mysql.hostname`: MySQL 主机地址
- `mysql.port`: MySQL 端口 (默认: 3306)
- `mysql.username`: MySQL 用户名
- `mysql.password`: MySQL 密码
- `mysql.database`: 数据库名称
- `mysql.table`: 表名称
- `mysql.server.timezone`: 服务器时区 (默认: UTC)

### S3 Tables / Iceberg 配置
- `iceberg.rest.uri`: REST Catalog URI (例如: `https://s3tables.us-east-1.amazonaws.com/iceberg`)
- `iceberg.warehouse.arn`: Warehouse ARN (例如: `arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket`)
- `iceberg.namespace`: 命名空间 (默认: my_namespace)
- `iceberg.table.name`: 目标表名称
- `aws.region`: AWS 区域

## 构建项目

```bash
cd flink-cdc-mysql-s3tables
mvn clean package
```

构建完成后，会在 `target/` 目录生成 JAR 文件。

## 本地运行

```bash
java -jar target/flink-cdc-mysql-s3tables-1.0-SNAPSHOT.jar \
  --mysql.hostname localhost \
  --mysql.port 3306 \
  --mysql.username flink_cdc \
  --mysql.password password \
  --mysql.database testdb \
  --mysql.table user_order_list \
  --iceberg.rest.uri https://s3tables.us-east-1.amazonaws.com/iceberg \
  --iceberg.warehouse.arn arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket \
  --iceberg.namespace my_namespace \
  --iceberg.table.name user_order_list \
  --aws.region us-east-1
```

## 在 AWS Managed Flink 上部署

1. **上传 JAR 到 S3**
```bash
aws s3 cp target/flink-cdc-mysql-s3tables-1.0-SNAPSHOT.jar s3://your-bucket/flink-apps/
```

2. **创建 Managed Flink 应用**
   - 在 AWS Console 中创建新的 Flink 应用
   - 选择上传的 JAR 文件
   - 配置应用属性 (FlinkApplicationProperties)

3. **配置应用属性**
```json
{
  "FlinkApplicationProperties": {
    "mysql.hostname": "your-mysql-host",
    "mysql.port": "3306",
    "mysql.username": "flink_cdc",
    "mysql.password": "your-password",
    "mysql.database": "testdb",
    "mysql.table": "user_order_list",
    "iceberg.rest.uri": "https://s3tables.us-east-1.amazonaws.com/iceberg",
    "iceberg.warehouse.arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    "iceberg.namespace": "my_namespace",
    "iceberg.table.name": "user_order_list",
    "aws.region": "us-east-1"
  }
}
```

4. **配置 IAM 角色**
   - S3 访问权限
   - S3 Tables 访问权限
   - CloudWatch Logs 权限

## S3 Tables REST Catalog 配置

AWS S3 Tables 使用 Iceberg REST Catalog 协议，并要求 AWS SigV4 签名认证。

### Flink 配置示例

```sql
CREATE CATALOG s3tables_catalog WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'https://s3tables.<region>.amazonaws.com/iceberg',
  'warehouse' = 'arn:aws:s3tables:<region>:<accountID>:bucket/<bucketname>',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  'rest.sigv4-enabled' = 'true',
  'rest.signing-name' = 's3tables',
  'rest.signing-region' = '<region>',
  'client.region' = '<region>',
  'rest-metrics-reporting-enabled' = 'false'
);
```

### 关键配置说明

1. **SigV4 认证** (必需)
   - `rest.sigv4-enabled = 'true'`: 启用 AWS SigV4 签名
   - `rest.signing-name = 's3tables'`: 签名服务名称
   - `rest.signing-region`: AWS 区域

2. **Warehouse ARN** (必需)
   - 格式: `arn:aws:s3tables:<region>:<accountID>:bucket/<bucketname>`
   - 这是 S3 Tables bucket 的完整 ARN

3. **REST Catalog URI** (必需)
   - 格式: `https://s3tables.<region>.amazonaws.com/iceberg`
   - 区域特定的 S3 Tables REST endpoint

### 与 Spark 配置对比

| Spark 配置 | Flink 配置 | 说明 |
|-----------|-----------|------|
| `spark.sql.catalog.spark_catalog.rest.sigv4-enabled` | `rest.sigv4-enabled` | 启用 SigV4 |
| `spark.sql.catalog.spark_catalog.rest.signing-name` | `rest.signing-name` | 签名服务名 |
| `spark.sql.catalog.spark_catalog.rest.signing-region` | `rest.signing-region` | 签名区域 |
| `spark.sql.catalog.spark_catalog.warehouse` | `warehouse` | Warehouse ARN |
| `spark.sql.catalog.spark_catalog.uri` | `uri` | REST endpoint |
| `spark.sql.catalog.spark_catalog.io-impl` | `io-impl` | S3 FileIO |

## 监控和调试

### 查看 Flink 作业状态
- 访问 Flink Web UI (本地: http://localhost:8081)
- 在 AWS Managed Flink 中查看应用监控

### 查看 S3 Tables 数据
```sql
-- 使用 Athena 或 Spark 查询
SELECT * FROM s3tables_catalog.my_namespace.user_order_list LIMIT 10;
```

### 日志
- 本地: 查看 `log/` 目录
- AWS Managed Flink: CloudWatch Logs

## 性能优化

1. **调整 checkpoint 间隔**
```java
configuration.setString("execution.checkpointing.interval", "5 min");
```

2. **调整 CDC 参数**
- `scan.incremental.snapshot.chunk.size`: 增量快照块大小
- `scan.snapshot.fetch.size`: 快照获取大小
- `debezium.max.batch.size`: Debezium 批处理大小

3. **Iceberg 写入优化**
- `write.metadata.delete-after-commit.enabled`: 自动清理旧元数据
- `write.metadata.previous-versions-max`: 保留的历史版本数

## 故障排查

### MySQL 连接问题
- 检查网络连接和防火墙规则
- 验证用户权限
- 确认 binlog 已启用

### S3 Tables 访问问题
- 验证 IAM 权限
- 检查 REST Catalog URI 是否正确
- 确认 warehouse 名称存在

### 性能问题
- 增加并行度
- 调整 checkpoint 间隔
- 优化 MySQL 查询性能

## 参考资料

- [Flink CDC Connectors](https://github.com/ververica/flink-cdc-connectors)
- [Apache Iceberg](https://iceberg.apache.org/)
- [AWS S3 Tables Documentation](https://docs.aws.amazon.com/s3tables/)
- [AWS Managed Service for Apache Flink](https://docs.aws.amazon.com/managed-flink/)

## License

Apache License 2.0
