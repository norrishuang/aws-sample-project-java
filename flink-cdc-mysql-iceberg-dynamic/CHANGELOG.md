# 更新日志

## v2.4 - KDA Java 17 支持指南 (2026-02-02)

### 📚 新增文档

**Amazon Managed Apache Flink (KDA) Java 17 支持指南**

由于 KDA 默认使用 Java 11，而本项目需要 Java 17，新增了详细的解决方案文档。

#### 提供的解决方案

1. **KDA 自定义镜像**（推荐）
   - 使用 Flink 1.20 + Java 17 Docker 镜像
   - 完全托管，运维简单
   - 详细的 Dockerfile 和部署步骤

2. **EMR on EKS**
   - 平衡托管和灵活性
   - 支持 Java 17
   - 完整的集群创建和作业提交示例

3. **自托管 Flink on EKS**
   - 使用 Flink Kubernetes Operator
   - 最大灵活性和控制
   - FlinkDeployment YAML 配置示例

4. **EC2 自托管**
   - 成本最低
   - 适合开发测试
   - 完整的安装和运行步骤

#### 文档内容

- ✅ 4 种完整的解决方案
- ✅ 详细的步骤说明和代码示例
- ✅ IAM 权限配置
- ✅ 成本和复杂度对比
- ✅ 推荐方案指导

#### 新增文件

- `KDA_JAVA17_GUIDE.md` - KDA Java 17 支持完整指南

---

## v2.3 - 升级到 Java 17 (2026-02-02)

### ⚠️ 重大变更

**升级 Java 版本要求从 11 到 17**

由于 Iceberg 1.11.0-SNAPSHOT 使用 Java 17 编译，必须升级项目的 Java 版本。

#### 变更内容
- ✅ 更新 `pom.xml` 中的 `target.java.version` 从 11 到 17
- ✅ 更新所有文档说明 Java 17 要求
- ✅ 添加 Java 版本检查说明

#### 影响
如果使用 Java 11 运行，会出现以下错误：
```
java.lang.UnsupportedClassVersionError: org/apache/iceberg/types/Type 
has been compiled by a more recent version of the Java Runtime 
(class file version 61.0)
```

#### 升级指南
1. 安装 Java 17:
   - macOS: `brew install openjdk@17`
   - Ubuntu: `sudo apt install openjdk-17-jdk`
   - Amazon Linux 2: `sudo amazon-linux-extras install java-openjdk17`

2. 验证版本:
   ```bash
   java -version
   # 应该显示 17.x.x
   ```

3. 重新编译项目:
   ```bash
   mvn clean package
   ```

#### 兼容性
- ✅ Flink 1.20.0 支持 Java 17
- ✅ Flink CDC 3.0.1 支持 Java 17
- ✅ 所有依赖都兼容 Java 17

---

## v2.2 - 添加 AWS Glue Catalog 支持 (2026-02-02)

### ✨ 新功能

**新增 AWS Glue Catalog 支持**

- ✅ 支持使用 AWS Glue 作为 Iceberg Catalog
- ✅ 完全托管的元数据服务，无需维护 Hive Metastore
- ✅ 与 AWS 服务深度集成（Athena、EMR、Redshift Spectrum）
- ✅ 细粒度的 IAM 权限控制

#### 配置示例

```properties
iceberg.catalog.type=glue
iceberg.warehouse=s3://my-bucket/warehouse
aws.region=us-east-1

# 可选配置
iceberg.glue.catalog.id=123456789012
iceberg.glue.skip.name.validation=false
iceberg.glue.endpoint=https://glue.us-east-1.amazonaws.com
```

#### 支持的 Catalog 类型

现在支持 4 种 Catalog 类型：
1. **hadoop** - Hadoop Catalog（本地或 S3）
2. **hive** - Hive Metastore Catalog
3. **glue** - AWS Glue Catalog（推荐用于 AWS 环境）⭐ NEW
4. **rest** - REST Catalog（如 AWS S3 Tables）

### 📝 文档更新

- 新增 `config-glue-example.properties` - Glue Catalog 配置示例
- 更新 `README.md` - 添加 Glue Catalog 配置说明和 IAM 权限示例
- 更新 `QUICKSTART.md` - 添加 Glue Catalog 快速开始指南
- 更新 `config-example.properties` - 添加 Glue 相关配置选项

### 🔧 代码改进

- 在 `buildCatalogProperties()` 方法中添加 Glue Catalog 配置逻辑
- 支持可选的 Glue Catalog ID、端点和名称验证配置
- 添加详细的日志输出用于调试

### 📚 集成示例

文档中新增了以下集成示例：
- AWS Athena 查询示例
- AWS Glue Studio ETL 作业
- Amazon EMR Spark 读取示例
- Amazon QuickSight 可视化
- AWS CLI 管理命令

---

## v2.1 - 修复编译问题 (2026-02-02)

### ✅ 问题解决

**修复了 DynamicIcebergSink 和 DynamicRecord 类找不到的编译错误**

#### 根本原因
- 这些类在 Iceberg 1.11.0-SNAPSHOT 中的包路径是 `org.apache.iceberg.flink.sink.dynamic.*`
- 而不是文档中提到的 `org.apache.iceberg.flink.sink.*`

#### 解决方案
1. **更新 import 语句**：
   ```java
   // 之前（错误）
   import org.apache.iceberg.flink.sink.DynamicIcebergSink;
   import org.apache.iceberg.flink.sink.DynamicRecord;
   
   // 现在（正确）
   import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
   import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
   ```

2. **配置 Apache Snapshot 仓库**：
   ```xml
   <repository>
       <id>apache-snapshots</id>
       <url>https://repository.apache.org/content/repositories/snapshots/</url>
   </repository>
   ```

3. **修复 RoaringBitmap 依赖**：
   - 排除错误的 `com.github.RoaringBitmap.RoaringBitmap:roaringbitmap`
   - 添加正确的 `org.roaringbitmap:RoaringBitmap:1.3.0`

### ✅ 编译和打包成功

- ✅ 编译成功
- ✅ 打包成功
- ✅ JAR 文件生成：`flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar` (163MB)

### ⚠️ 注意事项

- 当前使用 Iceberg `1.11.0-SNAPSHOT` 版本
- Snapshot 版本可能不稳定，建议充分测试后再用于生产
- 等待 Iceberg 1.11.0 正式版发布后可升级到稳定版本

---

## v2.0 - 使用 Iceberg Dynamic Sink

### 重大改进

根据用户建议和 [Iceberg 官方文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)，完全重构实现，使用 Iceberg 官方的 Dynamic Sink 功能。

### 核心变更

#### 1. 使用 Dynamic Sink API

**之前**:
```java
FlinkSink.forRowData(rowDataStream)
    .tableLoader(tableLoader)
    .upsert(true)
    .build();
```

**现在**:
```java
DynamicIcebergSink.forInput(dynamicRecordStream)
    .immediateTableUpdate(true)  // 关键配置
    .append();
```

#### 2. 简化 Schema 处理

**之前**:
- `DynamicSchemaProcessor`: 200+ 行代码
- 手动维护 schema 状态
- 手动检测和处理 schema 变更
- 复杂的类型转换逻辑

**现在**:
- `CDCToDynamicRecordConverter`: 150 行代码
- 只需提取 schema 并创建 DynamicRecord
- Iceberg 自动处理所有 schema 演进
- 简洁清晰的实现

#### 3. 新增配置参数

```properties
sink.immediate.table.update=true    # 立即更新表 schema
sink.cache.max.size=100              # 表元数据缓存大小
sink.cache.refresh.ms=60000          # 缓存刷新间隔
```

### 优势

1. **代码简洁**: 减少 30% 代码量
2. **官方支持**: 使用 Iceberg 官方 API，稳定可靠
3. **自动化**: 完全自动的 schema 演进，无需手动干预
4. **性能优化**: 内置缓存机制，减少 Catalog 访问
5. **易维护**: 逻辑清晰，易于理解和维护

### 新增文档

- `IMPLEMENTATION_NOTES.md`: 实现说明和技术细节
- `QUICKSTART.md`: 5 分钟快速开始指南
- `CHANGELOG.md`: 更新日志

### 升级指南

如果你使用的是 v1.0 版本，升级到 v2.0 需要：

1. 更新配置文件，添加 Dynamic Sink 配置
2. 重新构建项目
3. 重启 Flink 作业

**注意**: v2.0 与 v1.0 生成的 Iceberg 表完全兼容。

---

## v1.0 - 初始实现 (已废弃)

### 特性

- 使用 DataStream API
- 手动处理 schema 变更
- 支持基本的 CDC 操作

### 问题

- 代码复杂，难以维护
- 手动 schema 管理容易出错
- 性能不够优化

### 为什么废弃

Iceberg 官方提供了更好的 Dynamic Sink API，无需手动处理 schema 演进。
