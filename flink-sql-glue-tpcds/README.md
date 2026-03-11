# flink-sql-glue-tpcds

Flink SQL Job：通过 HiveCatalog 接入 AWS Glue Data Catalog，查询 TPC-DS `call_center` 表。

支持 **Session Mode** 和 **Application Mode** 在 Amazon EMR 上运行。

## 架构原理

```
Flink SQL  →  HiveCatalog (Java API)  →  AWSGlueDataCatalogHiveClientFactory  →  AWS Glue Data Catalog
```

**关键设计**：代码中通过 `HiveConf` 直接硬编码 Glue metastore 配置，**不依赖 `hive-site.xml` 文件**。
这避免了 Application Mode 下 YARN 容器无法正确加载 `hive-site.xml` 导致的 `Embedded metastore is not allowed` 错误。

```java
HiveConf hiveConf = new HiveConf();
hiveConf.set("hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");

HiveCatalog hiveCatalog = new HiveCatalog("glue_catalog", "default", hiveConf, "3.1.3");
tableEnv.registerCatalog("glue_catalog", hiveCatalog);
```

## 环境要求

| 组件 | 版本 |
|------|------|
| Amazon EMR | 7.12.0 |
| Apache Flink | 1.20.0 |
| Apache Hive | 3.1.3 |
| Apache Hadoop | 3.4.1 |
| Java | 11+ |

## 前置条件

### 1. Glue Data Catalog 中存在 tpcds 数据库和 call_center 表

可通过 Athena 或 Glue Console 创建：

```sql
CREATE DATABASE IF NOT EXISTS tpcds;
-- 创建 call_center 表（DDL 参考 TPC-DS 标准定义）
```

### 2. EMR 集群配置

创建 EMR 7.12 集群时需要包含 **Flink** 应用，并启用 Glue Data Catalog 作为 Hive Metastore。

---

## EMR 集群配置（重要）

在 EMR 上使用 Flink Application Mode 运行 HiveCatalog + Glue 需要额外的依赖配置。
EMR 默认的 Flink classpath **不包含 Spark 相关 jar**，而 `aws-glue-datacatalog-hive3-client.jar` 间接依赖了 Spark 类（如 `DriverTransferable`、`sparkproject.guava.cache.CacheLoader` 等），会导致 `NoClassDefFoundError`。

### 方式一：通过 EMR Console / CLI 配置（推荐）

在创建 EMR 集群时，通过 **Software settings** 添加以下 Configuration：

```json
[
  {
    "Classification": "hive-site",
    "Properties": {
      "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
  },
  {
    "Classification": "flink-conf",
    "Properties": {
      "classloader.resolve-order": "parent-first",
      "yarn.application.classpath": "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,/usr/lib/hadoop-lzo/lib/*,/usr/share/aws/emr/emrfs/conf,/usr/share/aws/emr/emrfs/lib/*,/usr/share/aws/emr/emrfs/auxlib/*,/usr/share/aws/emr/lib/*,/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar,/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar,/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar,/usr/lib/spark/yarn/lib/datanucleus-api-jdo.jar,/usr/lib/spark/yarn/lib/datanucleus-core.jar,/usr/lib/spark/yarn/lib/datanucleus-rdbms.jar,/usr/share/aws/emr/cloudwatch-sink/lib/*,/usr/share/aws/aws-java-sdk/*,/usr/share/aws/aws-java-sdk-v2/*,/usr/lib/spark/jars/*"
    }
  }
]
```

使用 AWS CLI 创建集群时：

```bash
aws emr create-cluster \
  --release-label emr-7.12.0 \
  --applications Name=Flink Name=Hive \
  --configurations '[
    {
      "Classification": "hive-site",
      "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
    {
      "Classification": "flink-conf",
      "Properties": {
        "classloader.resolve-order": "parent-first",
        "yarn.application.classpath": "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,/usr/lib/hadoop-lzo/lib/*,/usr/share/aws/emr/emrfs/conf,/usr/share/aws/emr/emrfs/lib/*,/usr/share/aws/emr/emrfs/auxlib/*,/usr/share/aws/emr/lib/*,/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar,/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar,/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar,/usr/lib/spark/yarn/lib/datanucleus-api-jdo.jar,/usr/lib/spark/yarn/lib/datanucleus-core.jar,/usr/lib/spark/yarn/lib/datanucleus-rdbms.jar,/usr/share/aws/emr/cloudwatch-sink/lib/*,/usr/share/aws/aws-java-sdk/*,/usr/share/aws/aws-java-sdk-v2/*,/usr/lib/spark/jars/*"
      }
    }
  ]' \
  --instance-type m5.xlarge \
  --instance-count 3 \
  ...
```

### 方式二：在已有 EMR 集群上手动配置

SSH 到 EMR Master 节点后执行以下步骤：

#### Step 1：将 Spark 依赖链接到 Flink lib 目录

`aws-glue-datacatalog-hive3-client.jar` 间接依赖 Spark 类库。在 Application Mode 下，Flink 只会将 `/usr/lib/flink/lib/` 目录中的 jar 文件上传到 YARN 容器的 classpath。需要创建符号链接：

```bash
# 将所有 Spark jar 链接到 Flink lib 目录
for jar in /usr/lib/spark/jars/spark-*.jar; do
  name=$(basename $jar)
  if [ ! -f "/usr/lib/flink/lib/$name" ] && [ ! -L "/usr/lib/flink/lib/$name" ]; then
    sudo ln -sf $jar /usr/lib/flink/lib/$name
  fi
done

# 验证
ls -la /usr/lib/flink/lib/spark-* | head -5
```

以下是需要链接的关键 jar（EMR 7.12.0）：

| Jar | 作用 |
|-----|------|
| `spark-core_2.12-3.5.6-amzn-1.jar` | Spark 核心库 |
| `spark-tags_2.12-3.5.6-amzn-1.jar` | 包含 `DriverTransferable` 等注解类 |
| `spark-network-common_2.12-3.5.6-amzn-1.jar` | 包含 Spark shade 的 Guava (`org.sparkproject.guava`) |
| `spark-sql_2.12-3.5.6-amzn-1.jar` | Spark SQL |
| `spark-catalyst_2.12-3.5.6-amzn-1.jar` | Spark Catalyst |
| `spark-unsafe_2.12-3.5.6-amzn-1.jar` | Spark Unsafe |
| `spark-common-utils_2.12-3.5.6-amzn-1.jar` | Spark 通用工具 |
| `spark-launcher_2.12-3.5.6-amzn-1.jar` | Spark Launcher |
| `spark-kvstore_2.12-3.5.6-amzn-1.jar` | Spark KV Store |
| `spark-network-shuffle_2.12-3.5.6-amzn-1.jar` | Spark Network Shuffle |

> **建议**：直接用上面的循环命令链接所有 `spark-*.jar`，避免遗漏传递依赖。

#### Step 2：修改 Flink 配置文件

编辑 `/etc/flink/conf/flink-conf.yaml`，在末尾追加：

```yaml
# Add Spark jars to YARN container classpath for Glue Catalog dependency
yarn.application.classpath: $HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,/usr/lib/hadoop-lzo/lib/*,/usr/share/aws/emr/emrfs/conf,/usr/share/aws/emr/emrfs/lib/*,/usr/share/aws/emr/emrfs/auxlib/*,/usr/share/aws/emr/lib/*,/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar,/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar,/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar,/usr/lib/spark/yarn/lib/datanucleus-api-jdo.jar,/usr/lib/spark/yarn/lib/datanucleus-core.jar,/usr/lib/spark/yarn/lib/datanucleus-rdbms.jar,/usr/share/aws/emr/cloudwatch-sink/lib/*,/usr/share/aws/aws-java-sdk/*,/usr/share/aws/aws-java-sdk-v2/*,/usr/lib/spark/jars/*
```

或用命令行追加：

```bash
sudo bash -c 'cat >> /etc/flink/conf/flink-conf.yaml << "EOF"

# Add Spark jars to YARN container classpath for Glue Catalog dependency
yarn.application.classpath: $HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,/usr/lib/hadoop-lzo/lib/*,/usr/share/aws/emr/emrfs/conf,/usr/share/aws/emr/emrfs/lib/*,/usr/share/aws/emr/emrfs/auxlib/*,/usr/share/aws/emr/lib/*,/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar,/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar,/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar,/usr/lib/spark/yarn/lib/datanucleus-api-jdo.jar,/usr/lib/spark/yarn/lib/datanucleus-core.jar,/usr/lib/spark/yarn/lib/datanucleus-rdbms.jar,/usr/share/aws/emr/cloudwatch-sink/lib/*,/usr/share/aws/aws-java-sdk/*,/usr/share/aws/aws-java-sdk-v2/*,/usr/lib/spark/jars/*
EOF'
```

---

## 编译

```bash
git clone git@github.com:norrishuang/aws-sample-project-java.git
cd aws-sample-project-java
mvn clean package -pl flink-sql-glue-tpcds -am -DskipTests
```

生成 jar：`flink-sql-glue-tpcds/target/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar`

所有 Flink/Hadoop/Hive 依赖均为 `provided` scope（EMR 已包含），打包后的 jar 仅 ~5KB。

---

## 部署和运行

### 上传 jar 到 EMR Master

```bash
scp -i <your-key.pem> \
  flink-sql-glue-tpcds/target/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar \
  hadoop@<emr-master-public-dns>:/home/hadoop/
```

### 方式一：Application Mode（推荐用于生产）

```bash
flink run-application -t yarn-application \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
  /home/hadoop/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
```

### 方式二：Session Mode

```bash
# 设置 Spark 依赖到 classpath（Session Mode 需要）
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/spark/jars/*

# 启动 YARN Session
flink-yarn-session -d

# 提交作业
flink run \
  -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
  /home/hadoop/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
```

### 方式三：EMR Step

```bash
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps '[{
    "Name": "Flink SQL Glue TPC-DS",
    "ActionOnFailure": "CONTINUE",
    "Type": "CUSTOM_JAR",
    "Jar": "command-runner.jar",
    "Args": [
      "flink", "run-application",
      "-t", "yarn-application",
      "-Djobmanager.memory.process.size=1024m",
      "-Dtaskmanager.memory.process.size=2048m",
      "-c", "com.amazonaws.java.flink.FlinkSQLGlueTpcds",
      "s3://<your-bucket>/jars/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar"
    ]
  }]'
```

---

## 输出

程序会打印：
1. `tpcds` 数据库下的所有表列表（`SHOW TABLES`）
2. `call_center` 表的前 20 条数据（`SELECT * FROM ... LIMIT 20`）

---

## 常见问题

### Q: `Embedded metastore is not allowed`

**原因**：HiveCatalog 无法加载 Glue metastore factory class，退化为 embedded metastore。

**解决**：本项目已通过 Java API 硬编码 `HiveConf` 解决，不再依赖 `hive-site.xml`。如果你仍使用 SQL DDL (`CREATE CATALOG ... WITH (...)`) 方式创建 catalog，需要确保 `hive-conf-dir` 指向的目录中有正确的 `hive-site.xml`，且 Glue client jar 在 classpath 中。

### Q: `NoClassDefFoundError: org/apache/spark/serializer/DriverTransferable`

**原因**：`aws-glue-datacatalog-hive3-client.jar` 传递依赖了 Spark 类。Application Mode 的 YARN 容器 classpath 默认不包含 Spark jar。

**解决**：将 Spark jar 链接到 `/usr/lib/flink/lib/`，详见上方 [Step 1](#step-1将-spark-依赖链接到-flink-lib-目录)。

### Q: `NoClassDefFoundError: org/sparkproject/guava/cache/CacheLoader`

**原因**：`org.sparkproject.guava` 是 Spark 内部 shade 的 Guava 库，位于 `spark-network-common_2.12-*.jar` 中。

**解决**：同上，确保 Spark jar 已链接到 Flink lib 目录。

### Q: Application Mode 和 Session Mode 的区别？

| | Session Mode | Application Mode |
|---|---|---|
| JM/TM classpath | 继承提交节点的 `HADOOP_CLASSPATH` | 由 Flink 从 `lib/` 目录构建 |
| 解决 Spark 依赖 | `export HADOOP_CLASSPATH=...:/usr/lib/spark/jars/*` | 将 jar 链接到 `/usr/lib/flink/lib/` |
| 适用场景 | 开发调试 | 生产部署 |

---

## 项目结构

```
flink-sql-glue-tpcds/
├── pom.xml                          # Maven 配置（所有依赖 provided）
├── README.md                        # 本文档
└── src/
    └── main/
        ├── java/
        │   └── com/amazonaws/java/flink/
        │       └── FlinkSQLGlueTpcds.java   # 主程序
        └── resources/
            └── log4j2.properties            # 日志配置
```
