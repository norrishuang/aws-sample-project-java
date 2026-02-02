# Flink CDC MySQL to Iceberg with Dynamic Schema

这个项目使用 Flink DataStream API 和 Iceberg Dynamic Sink 实现 MySQL CDC 到 Iceberg 的数据同步，支持动态 schema 变更。

## 系统要求

- ☕ **Java 17+** (必需 - Iceberg 1.11.0-SNAPSHOT 需要 Java 17)
- 🔧 Maven 3.6+
- 🐬 MySQL 5.7+ (需启用 binlog)
- 🌊 Flink 1.20.0

⚠️ **重要**: 由于使用了 Iceberg 1.11.0-SNAPSHOT，必须使用 Java 17 或更高版本。如果使用 Java 11 会出现 `UnsupportedClassVersionError`。

## 核心特性

- ✅ **DataStream API**: 使用 Flink DataStream API 而非 Table API/SQL
- ✅ **Iceberg Dynamic Sink**: 使用官方的 Dynamic Sink 功能
- ✅ **Automatic Schema Evolution**: 通过 `immediateTableUpdate(true)` 自动处理 schema 变更
- ✅ **Zero Manual Schema Handling**: 无需手动处理 schema，Iceberg 自动完成
- ✅ **CDC 操作**: 支持 INSERT、UPDATE、DELETE 操作
- ✅ **多种 Catalog**: 支持 Hadoop、Hive、Glue、REST Catalog
- ✅ **AWS Glue Catalog**: 推荐用于 AWS 环境的完全托管元数据服务
- ✅ **AWS S3 Tables**: 支持 AWS S3 Tables 的 REST Catalog 和 SigV4 认证

## 架构说明

```
MySQL Database
    ↓ (CDC Events)
Flink CDC Source
    ↓ (JSON Events)
CDCToDynamicRecordConverter
    ↓ (DynamicRecord: TableIdentifier + Schema + RowData)
Iceberg Dynamic Sink (immediateTableUpdate=true)
    ↓ (Automatic Schema Evolution)
Iceberg Table
```

### 关键组件

1. **MySQLCDCToIcebergDynamic**: 主程序，配置 CDC source 和 Dynamic Sink
2. **CDCToDynamicRecordConverter**: 将 CDC JSON 转换为 DynamicRecord
3. **Iceberg Dynamic Sink**: Iceberg 官方提供的动态 sink，自动处理 schema 演进

## 核心优势：使用 Iceberg Dynamic Sink

根据 [Iceberg 官方文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)，Dynamic Sink 提供：

### 1. 自动 Schema Evolution

```java
DynamicIcebergSink.forInput(dataStream)
    .immediateTableUpdate(true)  // 关键配置：立即更新表 schema
    .append();
```

当设置 `immediateTableUpdate(true)` 时：
- **自动检测 schema 变化**: 从 DynamicRecord 中的 Schema 检测变更
- **立即更新表结构**: 无需等待，实时更新 Iceberg 表 schema
- **支持的变更**: 添加列、修改兼容类型
- **不支持的变更**: 删除列、重命名列（避免数据丢失）

### 2. DynamicRecord 结构

```java
new DynamicRecord(
    TableIdentifier.of("db", "table"),  // 目标表
    "branch",                            // 分支（可选）
    schema,                              // 记录的 schema
    rowData,                             // 实际数据
    PartitionSpec.unpartitioned(),       // 分区规范
    DistributionMode.HASH,               // 分发模式
    2                                    // 写入并行度
)
```

### 3. Schema 缓存机制

Dynamic Sink 维护两个缓存：
- **Table Metadata Cache**: 缓存表元数据，减少 Catalog 访问
- **Input Schema Cache**: 缓存输入 schema，提高匹配效率

```java
.cacheMaxSize(100)           // 表元数据缓存大小
.cacheRefreshMs(60000L)      // 缓存刷新间隔
```

## 与传统方案的对比

### Flink SQL 方式（传统）

```sql
-- 必须预定义 schema
CREATE TABLE mysql_source (
  id INT,
  name STRING,
  age INT,
  -- 如果源表增加字段，必须修改 DDL 并重启作业
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' = 'mysql-cdc', ...);
```

**缺点**:
- Schema 固定，无法动态变更
- 需要重启作业才能应用 schema 变更
- 维护成本高

### DataStream + Dynamic Sink 方式（本项目）

```java
// 从 CDC 事件中提取 schema
Schema schema = extractSchema(cdcData);

// 创建 DynamicRecord
DynamicRecord record = new DynamicRecord(
    tableId, branch, schema, rowData, ...
);

// Dynamic Sink 自动处理 schema 演进
DynamicIcebergSink.forInput(stream)
    .immediateTableUpdate(true)  // 自动更新 schema
    .append();
```

**优点**:
- 完全动态，无需预定义 schema
- 自动检测和应用 schema 变更
- 无需重启作业
- 代码简洁，Iceberg 处理所有复杂逻辑

## 配置说明

### MySQL CDC 配置

```properties
mysql.hostname=localhost
mysql.port=3306
mysql.username=root
mysql.password=password
mysql.database=testdb
mysql.tables=.*                    # 支持正则表达式匹配多表
mysql.server.timezone=UTC
```

### Iceberg Catalog 配置

#### 1. Hadoop Catalog (本地或 S3)
```properties
iceberg.catalog.type=hadoop
iceberg.warehouse=s3://my-bucket/warehouse
iceberg.namespace=default
iceberg.table.name=cdc_table
```

#### 2. Hive Catalog
```properties
iceberg.catalog.type=hive
iceberg.hive.uri=thrift://localhost:9083
iceberg.warehouse=s3://my-bucket/warehouse
```

#### 3. AWS Glue Catalog (推荐用于 AWS 环境)
```properties
iceberg.catalog.type=glue
iceberg.warehouse=s3://my-bucket/warehouse
iceberg.namespace=default
iceberg.table.name=cdc_table
aws.region=us-east-1

# 可选配置
# iceberg.glue.catalog.id=123456789012  # AWS 账户 ID（默认使用当前账户）
# iceberg.glue.skip.name.validation=false  # 跳过 Glue 名称验证
# iceberg.glue.endpoint=  # 自定义 Glue 端点（用于 VPC 端点）
```

**Glue Catalog 优势**:
- ✅ 完全托管的元数据服务
- ✅ 与 AWS 服务深度集成（Athena、EMR、Redshift Spectrum）
- ✅ 自动备份和高可用
- ✅ 细粒度的 IAM 权限控制
- ✅ 无需维护 Hive Metastore

**所需 IAM 权限**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetTables"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/*",
        "arn:aws:s3:::my-bucket"
      ]
    }
  ]
}
```

#### 4. REST Catalog (AWS S3 Tables)
```properties
iceberg.catalog.type=rest
iceberg.rest.uri=https://s3tables.us-east-1.amazonaws.com/iceberg
iceberg.rest.sigv4.enabled=true
iceberg.warehouse=arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket
aws.region=us-east-1
```

### Dynamic Sink 配置

```properties
sink.parallelism=2                    # Sink 并行度
sink.upsert=true                      # 启用 upsert 模式
sink.immediate.table.update=true      # 关键：立即更新表 schema
sink.cache.max.size=100               # 表元数据缓存大小
sink.cache.refresh.ms=60000           # 缓存刷新间隔（毫秒）
```

## 构建和运行

### 1. 构建项目

```bash
mvn clean package
```

### 2. 本地运行

```bash
flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --mysql.hostname localhost \
  --mysql.database testdb \
  --mysql.tables "orders" \
  --iceberg.warehouse s3://my-bucket/warehouse \
  --sink.immediate.table.update true
```

### 3. 在 Amazon Managed Service for Apache Flink 上运行

配置 Application Properties:

```json
{
  "FlinkApplicationProperties": {
    "mysql.hostname": "mysql.example.com",
    "mysql.database": "production_db",
    "mysql.tables": "orders,customers",
    "iceberg.catalog.type": "rest",
    "iceberg.rest.uri": "https://s3tables.us-east-1.amazonaws.com/iceberg",
    "iceberg.rest.sigv4.enabled": "true",
    "iceberg.warehouse": "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    "sink.immediate.table.update": "true",
    "sink.parallelism": "4"
  }
}
```

## Dynamic Schema 工作原理

### 1. CDC 事件流

```json
{
  "op": "c",
  "after": {
    "id": 1,
    "name": "John",
    "email": "john@example.com"  // 新增字段
  }
}
```

### 2. 转换为 DynamicRecord

```java
// 从 CDC 数据中提取 schema
Schema schema = new Schema(
    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "email", Types.StringType.get())  // 新字段
);

// 创建 DynamicRecord
DynamicRecord record = new DynamicRecord(
    tableId, branch, schema, rowData, ...
);
```

### 3. Dynamic Sink 自动处理

```
DynamicRecord → Schema Comparison → Schema Evolution → Write Data
                      ↓
            immediateTableUpdate=true
                      ↓
            立即更新 Iceberg 表 schema
```

## Schema Evolution 示例

### 场景：MySQL 表添加新列

1. **初始表结构**
```sql
CREATE TABLE orders (
  id INT PRIMARY KEY,
  customer_name VARCHAR(100),
  amount DECIMAL(10,2)
);
```

2. **添加新列**
```sql
ALTER TABLE orders ADD COLUMN email VARCHAR(100);
```

3. **自动处理流程**
   - CDC 捕获包含新字段的数据
   - CDCToDynamicRecordConverter 从数据中提取新 schema
   - Dynamic Sink 检测到 schema 变化
   - `immediateTableUpdate=true` 触发立即更新
   - Iceberg 表自动添加 `email` 列
   - 新数据正常写入，旧数据 email 字段为 NULL

**无需任何手动干预！**

## 性能优化

### 1. Schema 缓存

```properties
sink.cache.max.size=100        # 增加缓存大小以减少 Catalog 访问
sink.cache.refresh.ms=60000    # 根据 schema 变更频率调整
```

### 2. 写入并行度

```properties
sink.parallelism=4             # 根据数据量和集群资源调整
```

### 3. Checkpoint 配置

```properties
checkpoint.interval=300000     # 5分钟，根据数据量调整
```

## 监控和调试

### 查看 Schema 历史

```sql
-- 使用 Spark SQL
SELECT * FROM iceberg_catalog.default.cdc_table.history;

-- 查看当前 schema
DESCRIBE EXTENDED iceberg_catalog.default.cdc_table;
```

### 日志配置

```properties
logger.cdc.name = com.amazonaws.java.flink
logger.cdc.level = INFO
```

## 故障排查

### 问题 1: Schema 更新失败

**症状**: 日志显示 schema 不兼容

**解决方案**:
- 检查 `immediateTableUpdate` 是否设置为 `true`
- 验证 schema 变更是否兼容（不支持删除列、重命名列）
- 查看 Iceberg 表的 schema 历史

### 问题 2: 性能问题

**症状**: 写入延迟高

**解决方案**:
- 增加 `sink.parallelism`
- 调整 `sink.cache.max.size` 和 `sink.cache.refresh.ms`
- 检查 Catalog 访问延迟

### 问题 3: 缓存问题

**症状**: Schema 更新不及时

**解决方案**:
- 减小 `sink.cache.refresh.ms`
- 或设置 `immediateTableUpdate=true` 确保立即更新

## 最佳实践

1. **始终启用 immediateTableUpdate**: `sink.immediate.table.update=true`
2. **合理设置缓存**: 平衡性能和实时性
3. **监控 Schema 变更**: 记录和审计 schema 演进
4. **测试 Schema 兼容性**: 在生产前验证 schema 变更
5. **使用分支**: 对于重大变更，使用 Iceberg 分支功能

## 参考资料

- [Iceberg Dynamic Sink 官方文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)
- [Flink CDC Connectors](https://github.com/ververica/flink-cdc-connectors)
- [Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)

## License

This project is licensed under the MIT License.
