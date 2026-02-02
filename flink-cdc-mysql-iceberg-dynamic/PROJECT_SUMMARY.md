# 项目总结：Flink CDC MySQL to Iceberg with Dynamic Schema

## 项目概述

这是一个使用 Flink DataStream API 实现的 MySQL CDC 到 Iceberg 数据同步项目，核心特性是支持动态 schema 变更。

## 技术栈

- **Flink 1.20.0**: 流处理引擎
- **Flink CDC 3.5.0**: MySQL CDC 连接器
- **Iceberg 1.6.1**: 数据湖表格式
- **Hadoop 3.3.4**: 文件系统支持
- **Java 11**: 开发语言

## 核心实现

### 1. 主程序 (MySQLCDCToIcebergDynamic.java)

负责：
- 配置和启动 Flink 作业
- 创建 MySQL CDC Source
- 配置 Iceberg Catalog 和 Sink
- 支持多种 Catalog 类型（Hadoop、Hive、REST）

关键配置：
```java
MySqlSource<String> source = MySqlSource.<String>builder()
    .includeSchemaChanges(true)  // 启用 schema 变更检测
    .scanNewlyAddedTableEnabled(true)  // 检测新增表
    .build();
```

### 2. Dynamic Schema Processor (DynamicSchemaProcessor.java)

核心功能：
- **Schema 变更检测**: 识别 CDC 流中的 schema change events
- **动态 Schema 维护**: 实时更新内部 schema 映射
- **数据类型转换**: 将 JSON 数据转换为 Flink RowData
- **操作类型处理**: 支持 INSERT、UPDATE、DELETE

工作流程：
```
CDC Event → Schema Detection → Data Conversion → RowData Output
```

### 3. 配置管理

支持两种配置方式：
1. **命令行参数**: 本地开发和测试
2. **KDA Runtime Properties**: Amazon Managed Flink 部署

## 与传统方案的对比

### Flink SQL 方式（传统）

**优点**:
- SQL 语法简单易懂
- 适合固定 schema 场景

**缺点**:
- Schema 必须预先定义
- Schema 变更需要修改 DDL 并重启作业
- 灵活性较差

示例：
```sql
CREATE TABLE mysql_source (
  id INT,
  name STRING,
  -- 如果源表增加字段，必须修改这里
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' = 'mysql-cdc', ...);
```

### DataStream + Dynamic Schema 方式（本项目）

**优点**:
- 无需预定义 schema
- 自动检测和适应 schema 变更
- 更灵活的数据处理逻辑
- 支持复杂的转换和过滤

**缺点**:
- 代码相对复杂
- 需要手动处理数据类型转换

示例：
```java
// 自动从 CDC 事件中提取 schema
processElement(String json, Context ctx, Collector<RowData> out) {
    if (isSchemaChange(json)) {
        updateSchema(json);
    } else {
        RowData data = convertWithCurrentSchema(json);
        out.collect(data);
    }
}
```

## Dynamic Schema 工作原理

### 1. Schema Change Event 结构

```json
{
  "schema": {
    "fields": [
      {"field": "id", "type": "int32", "optional": false},
      {"field": "name", "type": "string", "optional": true},
      {"field": "email", "type": "string", "optional": true}
    ]
  }
}
```

### 2. Data Event 结构

```json
{
  "op": "c",  // c=insert, u=update, d=delete, r=read
  "before": null,
  "after": {
    "id": 1,
    "name": "John",
    "email": "john@example.com"
  }
}
```

### 3. 处理流程

```
MySQL Table Schema Change
    ↓
Debezium CDC Captures Change
    ↓
Schema Change Event Emitted
    ↓
DynamicSchemaProcessor Updates Schema Map
    ↓
Subsequent Data Events Use New Schema
    ↓
Iceberg Auto-Evolves Table Schema
```

## Iceberg Dynamic Sink 特性

Iceberg 的 Dynamic Sink 提供：

1. **自动 Schema Evolution**: 
   - 自动添加新列
   - 支持列类型兼容性变更
   - 维护 schema 版本历史

2. **Upsert 支持**:
   - 基于主键的更新
   - 自动处理 INSERT/UPDATE/DELETE

3. **事务保证**:
   - ACID 事务
   - Exactly-once 语义

## 部署场景

### 1. 本地开发

```bash
flink run -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --mysql.hostname localhost \
  --iceberg.catalog.type hadoop
```

### 2. Amazon Managed Flink

- 上传 JAR 到 S3
- 配置 Application Properties
- 设置 IAM 角色权限
- 启动应用程序

### 3. 自建 Flink 集群

- 部署到 Kubernetes
- 使用 Flink Operator
- 配置 HA 和状态后端

## 性能考虑

### 1. Checkpoint 策略

- 间隔: 5-10 分钟（根据数据量调整）
- 增量 checkpoint: 减少状态大小
- 超时设置: 避免长时间阻塞

### 2. 并行度配置

- CDC Source: 1（保证顺序）
- Schema Processor: 1（维护 schema 状态）
- Iceberg Sink: 2-8（根据吞吐量）

### 3. 资源分配

- TaskManager 内存: 4-8 GB
- Slot 数量: 根据并行度
- 网络缓冲区: 适当增加

## 监控指标

### 关键指标

1. **CDC Source**:
   - 读取速率
   - Binlog 延迟
   - Schema 变更次数

2. **Processor**:
   - 处理延迟
   - 转换错误率
   - Schema 更新频率

3. **Iceberg Sink**:
   - 写入速率
   - 文件大小
   - Commit 延迟

## 故障恢复

### Checkpoint 恢复

- 从最近的 checkpoint 恢复
- 保证 exactly-once 语义
- 自动重试失败的操作

### Schema 不一致处理

- 记录 schema 版本
- 提供 schema 回滚机制
- 数据验证和修复

## 扩展性

### 支持多表同步

```java
// 使用正则表达式匹配多表
.tableList(database + ".(orders|customers|products)")
```

### 自定义数据转换

```java
// 在 DynamicSchemaProcessor 中添加转换逻辑
private Object transformValue(Object value, String fieldName) {
    // 自定义转换逻辑
}
```

### 添加数据过滤

```java
// 在 processElement 中添加过滤条件
if (shouldFilter(dataNode)) {
    return;
}
```

## 最佳实践

1. **启用 Schema 变更检测**: `includeSchemaChanges(true)`
2. **合理设置 Checkpoint 间隔**: 平衡性能和恢复时间
3. **监控 Binlog 延迟**: 避免数据积压
4. **定期清理旧快照**: 控制存储成本
5. **使用增量快照**: 加快初始同步速度

## 限制和注意事项

1. **Schema 变更类型**:
   - 支持: 添加列、修改列类型（兼容）
   - 不支持: 删除列、重命名列（需要手动处理）

2. **数据类型映射**:
   - 某些 MySQL 类型可能需要自定义转换
   - 注意时区处理

3. **性能影响**:
   - Schema 变更会触发 Iceberg 表更新
   - 频繁变更可能影响性能

## 未来改进

1. **Schema Registry 集成**: 统一管理 schema
2. **更智能的类型推断**: 自动处理复杂类型
3. **Schema 变更通知**: 发送告警
4. **数据质量检查**: 验证数据完整性
5. **多 Sink 支持**: 同时写入多个目标

## 总结

这个项目展示了如何使用 Flink DataStream API 和 Iceberg Dynamic Sink 实现灵活的 CDC 数据同步方案。相比传统的 Flink SQL 方式，它提供了更好的 schema 灵活性和扩展性，特别适合需要频繁 schema 变更的场景。
