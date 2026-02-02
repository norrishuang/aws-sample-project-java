# 快速开始指南

## 5 分钟快速体验

### 前置条件

- ☕ Java 17+ (必需)
- Maven 3.6+
- MySQL 5.7+ (启用 binlog)
- Flink 1.20.0

⚠️ **重要**: 必须使用 Java 17 或更高版本！Iceberg 1.11.0-SNAPSHOT 需要 Java 17。

检查 Java 版本：
```bash
java -version
# 应该显示 java version "17.x.x" 或更高
```

如果是 Java 11，会出现错误：
```
UnsupportedClassVersionError: class file version 61.0
```

### 步骤 1: 准备 MySQL

```sql
-- 创建测试数据库和表
CREATE DATABASE testdb;
USE testdb;

CREATE TABLE orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(100),
  amount DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO orders (customer_name, amount) VALUES 
  ('Alice', 100.50),
  ('Bob', 200.75);

-- 确保 binlog 已启用
SHOW VARIABLES LIKE 'log_bin';
```

### 步骤 2: 构建项目

```bash
cd flink-cdc-mysql-iceberg-dynamic
mvn clean package
```

### 步骤 3: 运行作业

```bash
flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --mysql.hostname localhost \
  --mysql.port 3306 \
  --mysql.username root \
  --mysql.password your_password \
  --mysql.database testdb \
  --mysql.tables "orders" \
  --iceberg.catalog.type hadoop \
  --iceberg.warehouse /tmp/iceberg-warehouse \
  --iceberg.namespace default \
  --iceberg.table.name orders_iceberg \
  --sink.immediate.table.update true
```

### 步骤 4: 测试 Dynamic Schema

在另一个终端中，向 MySQL 添加新列：

```sql
-- 添加新列
ALTER TABLE orders ADD COLUMN email VARCHAR(100);

-- 插入包含新字段的数据
INSERT INTO orders (customer_name, amount, email) VALUES 
  ('Charlie', 300.00, 'charlie@example.com');
```

### 步骤 5: 验证结果

使用 Spark SQL 查询 Iceberg 表：

```sql
-- 查看表结构（应该包含新增的 email 列）
DESCRIBE iceberg_catalog.default.orders_iceberg;

-- 查看数据
SELECT * FROM iceberg_catalog.default.orders_iceberg;

-- 查看 schema 历史
SELECT * FROM iceberg_catalog.default.orders_iceberg.history;
```

## 预期结果

1. **初始数据**: 包含 id, customer_name, amount, created_at
2. **Schema 演进**: 自动添加 email 列
3. **新数据**: 包含 email 字段
4. **旧数据**: email 字段为 NULL

## 关键观察点

### 1. 无需重启作业

```
MySQL: ALTER TABLE → CDC 捕获 → Dynamic Sink 自动更新 → 继续写入
```

### 2. 日志输出

```
INFO  CDCToDynamicRecordConverter - Extracting schema from CDC data
INFO  MySQLCDCToIcebergDynamic - Pipeline configured with Dynamic Iceberg Sink
```

### 3. Iceberg 表变化

```sql
-- Schema 版本增加
SELECT snapshot_id, manifest_list FROM iceberg_catalog.default.orders_iceberg.snapshots;
```

## 常见问题

### Q1: 作业启动失败

**检查**:
- MySQL binlog 是否启用
- 网络连接是否正常
- 用户权限是否足够

### Q2: Schema 未更新

**检查**:
- `sink.immediate.table.update` 是否设置为 `true`
- 查看 Flink 日志是否有错误
- 验证 Iceberg catalog 配置

### Q3: 数据未写入

**检查**:
- Checkpoint 是否成功
- Iceberg warehouse 路径是否可写
- 查看 Flink Web UI 的 metrics

## 下一步

- 尝试更多 schema 变更（修改类型、添加多列）
- 配置 AWS S3 作为 warehouse
- 使用 REST Catalog (S3 Tables)
- 添加分区配置
- 配置 upsert 模式

## 清理

```bash
# 停止 Flink 作业
flink cancel <job-id>

# 清理 Iceberg warehouse
rm -rf /tmp/iceberg-warehouse

# 清理 MySQL 测试数据
DROP DATABASE testdb;
```


---

## AWS Glue Catalog 快速开始

### 使用 AWS Glue 作为 Iceberg Catalog（推荐用于 AWS 环境）

#### 步骤 1: 配置 IAM 权限

确保你的 Flink 作业（EC2、EKS、KDA）有以下 IAM 权限：

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
        "arn:aws:s3:::your-bucket/*",
        "arn:aws:s3:::your-bucket"
      ]
    }
  ]
}
```

#### 步骤 2: 创建配置文件

```bash
cp config-glue-example.properties config.properties
```

编辑 `config.properties`:

```properties
# MySQL 配置
mysql.hostname=your-mysql-host.rds.amazonaws.com
mysql.database=production_db
mysql.username=admin
mysql.password=your-password

# Glue Catalog 配置
iceberg.catalog.type=glue
iceberg.warehouse=s3://your-data-lake/iceberg
iceberg.namespace=cdc_db
aws.region=us-east-1

# Dynamic Sink 配置
sink.immediate.table.update=true
```

#### 步骤 3: 运行作业

```bash
flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --config config.properties
```

#### 步骤 4: 使用 AWS Athena 查询数据

```sql
-- 在 Athena 中查询 Iceberg 表
SELECT * FROM cdc_db.mysql_cdc_table LIMIT 10;

-- 查看表的 schema
DESCRIBE cdc_db.mysql_cdc_table;

-- 时间旅行查询（查看历史数据）
SELECT * FROM cdc_db.mysql_cdc_table 
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-01 10:00:00';
```

#### 步骤 5: 测试 Schema 演化

在 MySQL 中添加新列：

```sql
ALTER TABLE orders ADD COLUMN email VARCHAR(100);
INSERT INTO orders (customer_name, amount, email) 
VALUES ('Charlie', 300.00, 'charlie@example.com');
```

Iceberg 表会自动更新 schema，无需重启 Flink 作业！

在 Athena 中验证：

```sql
-- 新列会自动出现
DESCRIBE cdc_db.mysql_cdc_table;

-- 可以查询新列
SELECT customer_name, email FROM cdc_db.mysql_cdc_table 
WHERE email IS NOT NULL;
```

### Glue Catalog 的优势

1. **完全托管**: 无需维护 Hive Metastore
2. **高可用**: AWS 自动处理备份和故障转移
3. **集成性强**: 与 Athena、EMR、Redshift Spectrum 无缝集成
4. **权限控制**: 使用 IAM 进行细粒度权限管理
5. **成本优化**: 按使用量付费，无固定成本

### 与其他 AWS 服务集成

#### 使用 AWS Glue Studio

1. 打开 AWS Glue Studio
2. 创建新的 ETL 作业
3. 选择 Iceberg 表作为数据源
4. 进行数据转换和分析

#### 使用 Amazon EMR

```python
# PySpark 示例
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Iceberg from Glue") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .getOrCreate()

# 读取 Iceberg 表
df = spark.table("glue_catalog.cdc_db.mysql_cdc_table")
df.show()
```

#### 使用 Amazon QuickSight

1. 在 QuickSight 中创建新数据集
2. 选择 Athena 作为数据源
3. 查询 Glue Catalog 中的 Iceberg 表
4. 创建可视化仪表板

### 监控和调试

#### 查看 Glue Catalog 元数据

```bash
# 使用 AWS CLI 查看数据库
aws glue get-database --name cdc_db

# 查看表信息
aws glue get-table --database-name cdc_db --name mysql_cdc_table

# 列出所有表
aws glue get-tables --database-name cdc_db
```

#### CloudWatch 日志

Flink 作业的日志会自动发送到 CloudWatch Logs，可以监控：
- Schema 演化事件
- 数据写入统计
- 错误和异常

### 故障排查

#### 问题 1: 权限不足

```
AccessDeniedException: User is not authorized to perform: glue:CreateTable
```

**解决方案**: 检查 IAM 角色是否有 Glue 权限。

#### 问题 2: S3 访问被拒绝

```
Access Denied (Service: Amazon S3; Status Code: 403)
```

**解决方案**: 确保 IAM 角色有 S3 读写权限。

#### 问题 3: Glue 数据库不存在

```
EntityNotFoundException: Database cdc_db not found
```

**解决方案**: 
- 确保配置中的 `iceberg.namespace` 正确
- 或手动创建 Glue 数据库：
  ```bash
  aws glue create-database --database-input '{"Name":"cdc_db"}'
  ```
