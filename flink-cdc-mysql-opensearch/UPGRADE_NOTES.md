# 升级说明 - Flink CDC MySQL to OpenSearch

## 概述

本项目已成功升级以支持以下版本：
- **Apache Flink**: 1.19.1
- **MySQL CDC**: 3.5.0
- **OpenSearch**: 2.x (使用 2.11.1 核心库，兼容 OpenSearch 2.19 集群)

## 主要变更

### 1. 依赖升级

#### Maven 依赖更新
- **Flink CDC MySQL Connector**: 从 3.2.1 升级到 3.5.0
- **OpenSearch 连接器**: 使用官方 Apache Flink OpenSearch Connector 2.0.0-1.19
- **OpenSearch 客户端**: 使用 OpenSearch 2.11.1 核心库（与 flink-connector-opensearch2 兼容）

#### 版本兼容性修复
```xml
<properties>
    <!-- 使用 OpenSearch 2.11.1，与 flink-connector-opensearch2:2.0.0-1.19 兼容 -->
    <opensearch.version>2.11.1</opensearch.version>
</properties>
```

#### 依赖管理优化
```xml
<dependencyManagement>
    <dependencies>
        <!-- 强制使用与 Flink 连接器兼容的 OpenSearch 版本 -->
        <dependency>
            <groupId>org.opensearch</groupId>
            <artifactId>opensearch-core</artifactId>
            <version>2.11.1</version>
        </dependency>
        <dependency>
            <groupId>org.opensearch.client</groupId>
            <artifactId>opensearch-java</artifactId>
            <version>2.6.0</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### 2. 连接器配置

#### 从 Elasticsearch 连接器迁移到 OpenSearch 连接器
- **之前**: 使用 `flink-sql-connector-elasticsearch7` 兼容 OpenSearch
- **现在**: 使用官方 `flink-connector-opensearch2` 连接器

#### SQL DDL 更新
```sql
-- 现在使用原生 opensearch-2 连接器
'connector' = 'opensearch-2'
```

### 3. 运行时错误修复

#### ClassNotFoundException 和 NoClassDefFoundError 解决方案

**问题**: 
- `ClassNotFoundException: org.opensearch.common.io.stream.NamedWriteable`
- `NoClassDefFoundError: org/opensearch/core/common/io/stream/StreamOutput`

**根本原因**: 
OpenSearch 依赖版本不匹配。Flink OpenSearch 连接器 `2.0.0-1.19` 期望特定版本的 OpenSearch 核心库。

**解决方案**:
1. **版本对齐**: 使用 OpenSearch 2.11.1 核心库（与连接器兼容）
2. **依赖完整性**: 包含所有必需的 OpenSearch 核心依赖
3. **Shaded JAR**: 确保所有 OpenSearch 类都包含在最终 JAR 中

#### 兼容性矩阵
| 组件 | 版本 | 兼容性 |
|------|------|--------|
| Flink | 1.19.1 | ✅ |
| flink-connector-opensearch2 | 2.0.0-1.19 | ✅ |
| OpenSearch 核心库 | 2.11.1 | ✅ |
| OpenSearch 集群 | 2.19 | ✅ |

### 4. 增强的 CDC 配置
新增了以下 CDC 优化参数：
- `scan.incremental.snapshot.enabled = 'true'`
- `scan.incremental.snapshot.chunk.size = '8092'`
- `scan.incremental.snapshot.unbounded-chunk-first.enabled = 'true'`
- `scan.snapshot.fetch.size = '2000'`
- `debezium.max.batch.size = '1000'`
- `debezium.max.queue.size = '1000'`

### 5. 表结构升级
- **之前**: 13 列简化表结构
- **现在**: 28 列完整表结构（与 Elasticsearch 项目一致）

### 6. Sink 配置优化
更新了以下 sink 参数以提高性能：
- `sink.bulk-flush.interval`: 从 5s 改为 3s
- `sink.bulk-flush.backoff.max-retries`: 从 3 改为 20
- `sink.bulk-flush.backoff.delay`: 从 1s 改为 1000ms

### 7. 兼容性说明

#### JDK 要求
- **最低要求**: JDK 11+（OpenSearch 2.x 客户端要求）
- **推荐**: JDK 11 或更高版本

#### OpenSearch 版本兼容性
- **支持**: OpenSearch 2.x 系列
- **测试**: 与 OpenSearch 2.19 集群兼容
- **客户端**: 使用 OpenSearch 2.11.1 核心库

### 8. 构建和部署

#### 构建命令
```bash
mvn clean package -DskipTests
```

#### 输出 JAR
- 位置: `target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar`
- 类型: Uber JAR（包含所有依赖）
- 大小: ~150MB（包含所有 OpenSearch 依赖）

### 9. 配置参数

所有现有的配置参数保持不变：

#### MySQL 配置
- `mysql.hostname`
- `mysql.port`
- `mysql.username`
- `mysql.password`
- `mysql.database`
- `mysql.table`
- `mysql.server-timezone`

#### OpenSearch 配置
- `opensearch.host`
- `opensearch.index`
- `opensearch.username`
- `opensearch.password`

## 验证步骤

1. **编译验证**: ✅ 项目成功编译
2. **依赖检查**: ✅ 所有依赖正确解析
3. **JAR 构建**: ✅ Uber JAR 成功生成
4. **版本兼容性**: ✅ OpenSearch 依赖版本对齐
5. **运行时错误**: ✅ ClassNotFoundException 和 NoClassDefFoundError 已修复

## 故障排除

### 常见问题

#### 1. ClassNotFoundException
**症状**: `java.lang.ClassNotFoundException: org.opensearch.common.io.stream.NamedWriteable`
**解决**: 已通过版本对齐修复，使用 OpenSearch 2.11.1

#### 2. NoClassDefFoundError
**症状**: `java.lang.NoClassDefFoundError: org/opensearch/core/common/io/stream/StreamOutput`
**解决**: 已包含完整的 opensearch-core 依赖

#### 3. 版本不兼容
**症状**: 连接器初始化失败
**解决**: 确保使用兼容的版本组合（见兼容性矩阵）

### 检查清单

如遇到问题，请检查：
1. ✅ JDK 版本是否为 11+
2. ✅ OpenSearch 集群是否可访问
3. ✅ MySQL 权限配置是否正确
4. ✅ 网络连接是否正常
5. ✅ 使用正确的 JAR 文件（包含所有依赖）

## 迁移建议

从旧版本迁移时：
1. 备份现有配置
2. 更新 Flink 集群到 1.19.1
3. 使用新的 JAR 文件
4. 验证连接配置
5. 测试数据流管道
6. 监控运行时错误日志

## 注意事项

1. **版本兼容性**: 严格使用兼容的版本组合
2. **OpenSearch 集群**: 支持 OpenSearch 2.19 集群
3. **JDK 版本**: 必须使用 JDK 11 或更高版本
4. **网络配置**: 确保 Flink 集群可以访问 MySQL 和 OpenSearch 端点
5. **依赖完整性**: 使用 Shaded JAR 确保所有依赖都包含在内