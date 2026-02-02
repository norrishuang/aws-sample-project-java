# 问题解决总结

## 问题描述

在使用 Iceberg Dynamic Sink 实现 MySQL CDC 到 Iceberg 的数据同步时，遇到编译错误：
```
cannot find symbol: class DynamicIcebergSink
cannot find symbol: class DynamicRecord
```

## 调查过程

### 1. 初步分析
- 官方文档显示这些类在 Iceberg 1.10.0+ 中可用
- Maven Central 的 Iceberg 1.10.0 版本不包含这些类
- 怀疑需要使用 Snapshot 版本

### 2. 配置 Snapshot 仓库
```xml
<repository>
    <id>apache-snapshots</id>
    <url>https://repository.apache.org/content/repositories/snapshots/</url>
    <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
    </snapshots>
</repository>
```

使用版本：`<iceberg.version>1.11.0-SNAPSHOT</iceberg.version>`

### 3. 问题依然存在
即使配置了 Snapshot 仓库，编译仍然失败。

### 4. 深入调查 JAR 内容
```bash
jar tf iceberg-flink-1.20-1.11.0-SNAPSHOT.jar | grep -i "dynamic"
```

**关键发现**：
```
org/apache/iceberg/flink/sink/dynamic/DynamicIcebergSink.class
org/apache/iceberg/flink/sink/dynamic/DynamicRecord.class
```

类确实存在，但在 `org.apache.iceberg.flink.sink.dynamic.*` 包中！

## 解决方案

### 修改 1：更新 Import 语句

**MySQLCDCToIcebergDynamic.java**:
```java
// 错误的 import
import org.apache.iceberg.flink.sink.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.DynamicRecord;

// 正确的 import
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
```

**CDCToDynamicRecordConverter.java**:
```java
// 错误的 import
import org.apache.iceberg.flink.sink.DynamicRecord;

// 正确的 import
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
```

### 修改 2：修复 RoaringBitmap 依赖

在 `pom.xml` 中：

1. 排除错误的依赖：
```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
    <version>${iceberg.version}</version>
    <exclusions>
        <exclusion>
            <groupId>com.github.RoaringBitmap.RoaringBitmap</groupId>
            <artifactId>roaringbitmap</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

2. 添加正确的依赖：
```xml
<dependency>
    <groupId>org.roaringbitmap</groupId>
    <artifactId>RoaringBitmap</artifactId>
    <version>1.3.0</version>
</dependency>
```

## 结果

✅ **编译成功**
```
[INFO] BUILD SUCCESS
[INFO] Total time:  6.933 s
```

✅ **打包成功**
```
[INFO] BUILD SUCCESS
[INFO] Total time:  25.200 s
```

✅ **JAR 文件生成**
```
flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar (163MB)
```

## 经验教训

1. **不要完全依赖文档**：官方文档可能不够详细或有滞后
2. **检查实际的 JAR 内容**：使用 `jar tf` 命令查看类的实际位置
3. **Snapshot 版本的包结构可能与正式版不同**：API 在开发过程中可能会重构
4. **依赖冲突需要仔细排查**：Maven 的传递依赖可能引入错误的版本

## 后续建议

1. **等待正式版本**：Iceberg 1.11.0 正式发布后，包路径可能会稳定
2. **充分测试**：Snapshot 版本可能不稳定，需要充分测试
3. **关注官方更新**：定期检查 Iceberg 的发布说明和 API 变更

## 参考资料

- [Apache Iceberg Snapshot Repository](https://repository.apache.org/content/repositories/snapshots/)
- [Iceberg Flink Dynamic Sink 文档](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink)
- [Maven JAR Plugin - jar:tf](https://maven.apache.org/plugins/maven-jar-plugin/)

---

**解决时间**: 2026-02-02  
**解决者**: Kiro AI Assistant
