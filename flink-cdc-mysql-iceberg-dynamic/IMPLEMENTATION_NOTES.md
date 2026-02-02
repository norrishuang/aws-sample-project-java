# 实现说明：使用 Iceberg Dynamic Sink

## 核心改进

根据你的建议和 [Iceberg 官方文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)，我们简化了实现，直接使用 Iceberg 的 Dynamic Sink 功能。

## 关键配置

只需一行配置即可实现动态 schema 更新：

```java
DynamicIcebergSink.forInput(dataStream)
    .immediateTableUpdate(true)  // 关键：启用立即 schema 更新
    .append();
```

## 实现对比

### 之前的实现（复杂）

```java
// 需要手动处理 schema 变更
class DynamicSchemaProcessor {
    - 维护 schema 状态
    - 检测 schema 变更事件
    - 手动更新 schema 映射
    - 转换数据格式
}

// 使用普通 FlinkSink
FlinkSink.forRowData(rowDataStream)
    .tableLoader(tableLoader)
    .upsert(true)
    .build();
```

**问题**:
- 代码复杂，需要手动管理 schema 状态
- 容易出错
- 维护成本高

### 现在的实现（简洁）

```java
// 只需转换为 DynamicRecord
class CDCToDynamicRecordConverter {
    - 从 CDC 数据提取 schema
    - 创建 DynamicRecord
    - 其他交给 Iceberg 处理
}

// 使用 Dynamic Sink
DynamicIcebergSink.forInput(dynamicRecordStream)
    .immediateTableUpdate(true)  // Iceberg 自动处理 schema 演进
    .append();
```

**优势**:
- 代码简洁，逻辑清晰
- Iceberg 官方支持，稳定可靠
- 自动处理 schema 演进
- 无需手动维护状态

## DynamicRecord 结构

```java
new DynamicRecord(
    TableIdentifier.of("db", "table"),  // 目标表
    "branch",                            // 分支（可选）
    schema,                              // 从 CDC 数据提取的 schema
    rowData,                             // 实际数据
    PartitionSpec.unpartitioned(),       // 分区规范
    DistributionMode.HASH,               // 分发模式
    2                                    // 写入并行度
)
```

## Schema Evolution 流程

```
CDC Event → Extract Schema → Create DynamicRecord → Dynamic Sink
                                                          ↓
                                            immediateTableUpdate=true
                                                          ↓
                                            Compare with Table Schema
                                                          ↓
                                            Auto Update if Needed
                                                          ↓
                                            Write Data
```

## 支持的 Schema 变更

### ✅ 支持

- **添加列**: 自动添加新列到表
- **兼容类型变更**: 如 int → long

### ❌ 不支持

- **删除列**: 避免数据丢失
- **重命名列**: 需要额外元数据

## 配置参数

### 必需配置

```properties
sink.immediate.table.update=true    # 启用立即 schema 更新
```

### 可选配置

```properties
sink.cache.max.size=100              # 表元数据缓存大小
sink.cache.refresh.ms=60000          # 缓存刷新间隔
sink.parallelism=2                   # 写入并行度
sink.upsert=true                     # 启用 upsert 模式
```

## 性能考虑

### 1. immediateTableUpdate 的影响

- **true**: 立即更新 schema，可能增加 Catalog 负载
- **false**: 通过中央执行器更新，可能引入反压

**建议**: 对于 schema 变更不频繁的场景，使用 `true`

### 2. 缓存策略

```java
.cacheMaxSize(100)           // 缓存更多表元数据
.cacheRefreshMs(60000L)      // 根据变更频率调整
```

### 3. Schema 复用

```java
// 重要：复用相同的 Schema 实例以提高缓存命中率
Schema schema = extractSchema(data);
// 如果 schema 未变，复用之前的实例
```

## 最佳实践

1. **始终启用 immediateTableUpdate**: 确保 schema 及时更新
2. **合理设置缓存**: 平衡性能和实时性
3. **监控 Schema 变更**: 记录和审计所有 schema 演进
4. **测试兼容性**: 在生产前验证 schema 变更
5. **使用分支**: 对于重大变更，使用 Iceberg 分支功能

## 总结

通过使用 Iceberg 官方的 Dynamic Sink 和 `immediateTableUpdate(true)` 配置，我们实现了：

- ✅ 完全自动的 schema 演进
- ✅ 简洁的代码实现
- ✅ 稳定可靠的官方支持
- ✅ 零手动 schema 管理

这正是 Iceberg Dynamic Sink 设计的初衷！
