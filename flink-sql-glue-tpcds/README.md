# flink-sql-glue-tpcds

Flink SQL Job：通过 Glue Catalog 查询 TPC-DS `call_center` 表。

## 概述

使用 Flink Table API 创建一个 HiveCatalog（底层接入 AWS Glue Data Catalog），然后 SELECT 查询 `tpcds.call_center` 表。

## 环境要求

| 组件 | 版本 |
|------|------|
| EMR | 7.12.0 |
| Flink | 1.20.0 |
| Java | 11+ |

## 前置条件

1. **EMR 集群配置**：创建 EMR 7.12 集群时需要包含 Flink 应用，并启用 Glue Catalog：
   ```json
   [
     {
       "Classification": "hive-site",
       "Properties": {
         "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
       }
     }
   ]
   ```

2. **Glue Data Catalog 中存在 tpcds 数据库和 call_center 表**。可通过 Athena 执行：
   ```sql
   CREATE DATABASE IF NOT EXISTS tpcds;
   -- 然后创建 call_center 表（DDL 参考 TPC-DS 标准定义）
   ```

## 编译

```bash
cd aws-sample-project-java
mvn clean package -pl flink-sql-glue-tpcds -am
```

## 提交到 EMR

将 jar 上传到 S3 后，SSH 到 EMR Master 节点执行：

```bash
# 启动 YARN Session
flink-yarn-session -d

# 提交作业
flink run \
  -c com.amazonaws.java.flink.FlinkSQLGlueTpcds \
  flink-sql-glue-tpcds-1.0-SNAPSHOT.jar
```

或者使用 EMR Step 方式提交：

```bash
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps '[{
    "Name": "Flink SQL Glue TPC-DS",
    "ActionOnFailure": "CONTINUE",
    "Type": "CUSTOM_JAR",
    "Jar": "command-runner.jar",
    "Args": [
      "flink", "run",
      "-m", "yarn-cluster",
      "-c", "com.amazonaws.java.flink.FlinkSQLGlueTpcds",
      "s3://<your-bucket>/jars/flink-sql-glue-tpcds-1.0-SNAPSHOT.jar"
    ]
  }]'
```

## 输出

程序会打印：
1. `tpcds` 数据库下的所有表列表（`SHOW TABLES`）
2. `call_center` 表的前 20 条数据（`SELECT * FROM ... LIMIT 20`）
