# Flink CDC MySQL to Iceberg with Dynamic Sink

使用 Flink DataStream API + Iceberg 官方 **Dynamic Sink API** 实现 MySQL CDC → Iceberg 数据同步，支持动态表路由、自动 Schema Evolution、自动建表。

## 系统要求

- ☕ **Java 17+**（Iceberg 1.11.0-SNAPSHOT 需要 Java 17）
- 🔧 Maven 3.6+
- 🐬 MySQL 5.7+（需启用 binlog）
- 🌊 Flink 1.20.0

## 核心架构

```
MySQL Database
    ↓ (Debezium CDC Events, JSON)
Flink CDC Source (MySqlSource)
    ↓ DataStream<String>
DynamicIcebergSink
  ├─ CDCDynamicRecordGenerator  → 实现 DynamicRecordGenerator<String> 接口
  │    将 CDC JSON 转换为 DynamicRecord（含 TableIdentifier, Schema, RowData 等）
  ├─ DynamicRecordProcessor     → Iceberg 内置，处理 schema evolution / 表创建 / 路由
  ├─ DynamicWriter              → Iceberg 内置，写数据文件
  └─ DynamicCommitter           → Iceberg 内置，提交到 Catalog
```

### 关键设计

**本项目正确使用了 Iceberg 官方的 `DynamicRecordGenerator` 接口：**

```java
// 1. 传入原始 DataStream<String>（CDC JSON），不做预处理
// 2. 通过 .generator() 设置自定义 DynamicRecordGenerator
// 3. 所有 schema evolution / 表创建 / 路由 由 Dynamic Sink 内部处理

DynamicIcebergSink
    .<String>forInput(cdcStream)                    // 原始 JSON 流
    .generator(new CDCDynamicRecordGenerator(...))  // 转换逻辑
    .catalogLoader(catalogLoader)
    .immediateTableUpdate(true)
    .append();
```

**而不是**（旧的错误用法）：
```java
// ❌ 先 ProcessFunction 手动转成 DynamicRecord 再传入
DataStream<DynamicRecord> records = cdcStream.process(new Converter());
DynamicIcebergSink.forInput(records)  // 没有 .generator()
    .append();
```

## 核心组件

### 1. CDCDynamicRecordGenerator

实现 `org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator<String>` 接口：

```java
public class CDCDynamicRecordGenerator implements DynamicRecordGenerator<String> {
    @Override
    public void generate(String jsonString, Collector<DynamicRecord> out) {
        // 1. 解析 Debezium CDC JSON
        // 2. 提取 source.table → TableIdentifier（动态路由）
        // 3. 从 Debezium schema 元数据构建 Iceberg Schema
        // 4. 转换 JSON 数据为 Flink RowData
        // 5. 设置 UpsertMode + EqualityFields（如启用）
        // 6. 输出 DynamicRecord
    }
}
```

**职责边界：**
- ✅ 解析 CDC JSON，构建 `DynamicRecord`
- ✅ 从 Debezium schema 提取准确的列类型
- ✅ 提取主键信息用于 equality fields
- ❌ 不管 schema evolution — Dynamic Sink 自动处理
- ❌ 不管表创建 — Dynamic Sink 根据 `DynamicRecord` 自动建表
- ❌ 不管表路由 — 通过 `DynamicRecord.tableIdentifier()` 自动路由

### 2. MySQLCDCToIcebergDynamic

主程序，负责：
- 配置 MySQL CDC Source
- 创建 CatalogLoader（支持 Hadoop / Hive / Glue / REST）
- 构建 `DynamicIcebergSink` 并设置 generator

## DynamicRecord 属性

每条 CDC 记录转换为 `DynamicRecord`，包含：

| 属性 | 说明 | 来源 |
|---|---|---|
| TableIdentifier | 目标 Iceberg 表 | Debezium `source.table` |
| Branch | 写入分支 | 配置参数（可选） |
| Schema | 记录的 Iceberg Schema | Debezium schema 元数据 |
| RowData | 实际数据 | CDC `after`/`before` 字段 |
| PartitionSpec | 分区规范 | 默认 unpartitioned |
| DistributionMode | 分发模式 | HASH（Dynamic Sink 推荐） |
| WriteParallelism | 写入并行度 | 配置参数 |
| UpsertMode | 是否 upsert | 配置参数（需 v2 表格式） |
| EqualityFields | 等值字段（主键） | Debezium schema 中的 required 字段 |

## 配置参数

### MySQL CDC

| 参数 | 默认值 | 说明 |
|---|---|---|
| `mysql.hostname` | localhost | MySQL 地址 |
| `mysql.port` | 3306 | MySQL 端口 |
| `mysql.username` | root | 用户名 |
| `mysql.password` | password | 密码 |
| `mysql.database` | testdb | 数据库名 |
| `mysql.tables` | `.*` | 表名（支持正则匹配多表） |
| `mysql.server.timezone` | UTC | 时区 |

### Iceberg

| 参数 | 默认值 | 说明 |
|---|---|---|
| `iceberg.catalog.type` | hadoop | Catalog 类型（hadoop/hive/rest/glue） |
| `iceberg.catalog.name` | iceberg_catalog | Catalog 名称 |
| `iceberg.warehouse` | s3://my-bucket/warehouse | 仓库路径 |
| `iceberg.namespace` | default | 命名空间 |
| `iceberg.branch` | null | 写入分支（可选） |

### Dynamic Sink

| 参数 | 默认值 | 说明 |
|---|---|---|
| `sink.parallelism` | 2 | 写入并行度 |
| `sink.upsert` | false | 启用 upsert 模式 |
| `sink.immediate.table.update` | true | 立即更新表 schema |
| `sink.cache.max.size` | 100 | 表元数据缓存大小 |
| `sink.cache.refresh.ms` | 60000 | 缓存刷新间隔（ms） |
| `sink.write.format` | null | 写入格式（parquet/orc/avro） |
| `sink.target.file.size.bytes` | null | 目标文件大小 |

### Catalog 特定配置

#### REST Catalog（AWS S3 Tables）
```
iceberg.rest.uri=https://s3tables.us-east-1.amazonaws.com/iceberg
iceberg.rest.sigv4.enabled=true
aws.region=us-east-1
```

#### Hive Catalog
```
iceberg.hive.uri=thrift://localhost:9083
```

#### AWS Glue Catalog
```
aws.region=us-east-1
iceberg.glue.catalog.id=123456789012  # 可选
iceberg.glue.endpoint=                # 可选，VPC 端点
```

## 构建和运行

```bash
# 构建
mvn clean package

# 运行
flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --mysql.hostname localhost \
  --mysql.database testdb \
  --mysql.tables "orders|customers" \
  --iceberg.warehouse s3://my-bucket/warehouse \
  --sink.immediate.table.update true \
  --sink.upsert true
```

## Schema Evolution

Dynamic Sink 自动处理以下 schema 变更（**无需重启 Flink 作业**）：

| 变更类型 | 支持 | 说明 |
|---|---|---|
| 添加列 | ✅ | 新列自动添加，旧数据为 NULL |
| 类型拓宽 | ✅ | Integer→Long, Float→Double |
| Required→Optional | ✅ | 列变为可空 |
| 删除列 | ❌ | 不支持 |
| 重命名列 | ❌ | 不支持 |

## 参考

- [Iceberg Flink Dynamic Sink 文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)
- [DynamicRecordGenerator 接口](https://github.com/apache/iceberg/blob/main/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/DynamicRecordGenerator.java)
- [DynamicRecord 类](https://github.com/apache/iceberg/blob/main/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/DynamicRecord.java)
- [DynamicIcebergSink Builder](https://github.com/apache/iceberg/blob/main/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/DynamicIcebergSink.java)
- [Flink CDC Connectors](https://github.com/ververica/flink-cdc-connectors)

## License

This project is licensed under the MIT License.
