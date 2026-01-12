# Flink CDC MySQL to Elasticsearch

This project implements a real-time data synchronization pipeline from MySQL to Elasticsearch using Flink CDC (Change Data Capture). It supports deployment on both AWS Managed Flink (KDA) and EMR Flink environments.

## Features

- Real-time MySQL CDC streaming to Elasticsearch
- Configurable MySQL and Elasticsearch connection parameters
- Optimized bulk flush settings for Elasticsearch
- Support for 28-column table schema with various data types
- Automatic retry and backoff strategies
- Compatible with AWS Managed Flink and EMR Flink

## Prerequisites

- Java 11 or higher
- Apache Flink 1.20.0
- MySQL database with CDC enabled
- Elasticsearch cluster (version 7.10.2 compatible)

## Configuration Parameters

The application supports different parameter loading methods depending on the deployment environment:

### AWS Managed Flink (KDA) Parameters
For AWS Managed Flink, use underscore format in the FlinkApplicationProperties:

#### MySQL Configuration
- `mysql_hostname`: MySQL server hostname
- `mysql_port`: MySQL server port (default: 3306)
- `mysql_username`: MySQL username
- `mysql_password`: MySQL password
- `mysql_database`: MySQL database name
- `mysql_table`: MySQL table name
- `mysql_server_timezone`: MySQL server timezone (default: UTC)

#### Elasticsearch Configuration
- `elasticsearch_host`: Elasticsearch cluster endpoint
- `elasticsearch_index`: Elasticsearch index name
- `elasticsearch_username`: Elasticsearch username
- `elasticsearch_password`: Elasticsearch password

### EMR Flink / Local Development Parameters
For EMR Flink or local development, use command-line arguments:

#### MySQL Configuration
- `--mysql_hostname`: MySQL server hostname
- `--mysql_port`: MySQL server port (default: 3306)
- `--mysql_username`: MySQL username
- `--mysql_password`: MySQL password
- `--mysql_database`: MySQL database name
- `--mysql_table`: MySQL table name
- `--mysql_server_timezone`: MySQL server timezone (default: UTC)

#### Elasticsearch Configuration
- `--elasticsearch_host`: Elasticsearch cluster endpoint
- `--elasticsearch_index`: Elasticsearch index name
- `--elasticsearch_username`: Elasticsearch username
- `--elasticsearch_password`: Elasticsearch password

## Table Schema

The application is designed to work with the following MySQL table schema:

```sql
CREATE TABLE user_order_list_20cols_new (
    id INT,
    uuid STRING,
    user_name STRING,
    phone_number STRING,
    product_id INT,
    product_name STRING,
    product_type STRING,
    manufacturing_date INT,
    price DECIMAL(6,2),
    unit INT,
    email STRING,
    address STRING,
    city STRING,
    country STRING,
    ip_address STRING,
    website STRING,
    company_name STRING,
    department STRING,
    salary DECIMAL(8,2),
    age INT,
    gender STRING,
    status STRING,
    created_date TIMESTAMP,
    last_login TIMESTAMP,
    score INT,
    description STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
);
```

## Building the Project

```bash
mvn clean package
```

This will create a shaded JAR file in the `target/` directory.

## Running the Application

### AWS Managed Flink (KDA)

1. Upload the JAR file to S3
2. Create a Managed Flink application with the following configuration:

**Application Properties (FlinkApplicationProperties):**
```json
{
  "mysql_hostname": "your-mysql-host",
  "mysql_port": "3306",
  "mysql_username": "your-mysql-user",
  "mysql_password": "your-mysql-password",
  "mysql_database": "your-database",
  "mysql_table": "your-table",
  "mysql_server_timezone": "UTC",
  "elasticsearch_host": "https://your-elasticsearch-host:443",
  "elasticsearch_index": "your-index",
  "elasticsearch_username": "your-es-user",
  "elasticsearch_password": "your-es-password"
}
```

### EMR Flink

```bash
flink run -c com.amazonaws.java.flink.MySQLCDCToElasticsearch \
  s3://your-bucket/flink-cdc-mysql-elasticsearch-1.0-SNAPSHOT.jar \
  --mysql_hostname your-mysql-host \
  --mysql_port 3306 \
  --mysql_username your-mysql-user \
  --mysql_password your-mysql-password \
  --mysql_database your-database \
  --mysql_table your-table \
  --mysql_server_timezone UTC \
  --elasticsearch_host https://your-elasticsearch-host:443 \
  --elasticsearch_index your-index \
  --elasticsearch_username your-es-user \
  --elasticsearch_password your-es-password
```

### Local Development

```bash
java -cp target/flink-cdc-mysql-elasticsearch-1.0-SNAPSHOT.jar \
  com.amazonaws.java.flink.MySQLCDCToElasticsearch \
  --mysql_hostname your-mysql-host \
  --mysql_port 3306 \
  --mysql_username your-mysql-user \
  --mysql_password your-mysql-password \
  --mysql_database your-database \
  --mysql_table your-table \
  --mysql_server_timezone UTC \
  --elasticsearch_host https://your-elasticsearch-host:443 \
  --elasticsearch_index your-index \
  --elasticsearch_username your-es-user \
  --elasticsearch_password your-es-password
```

## Elasticsearch Sink Configuration

The application uses optimized Elasticsearch sink settings:

- **Bulk flush max actions**: 1000 documents per batch
- **Bulk flush interval**: 3 seconds
- **Bulk flush max size**: 5MB per batch
- **Backoff strategy**: Exponential with 20 max retries
- **Backoff delay**: 1000ms initial delay
- **Flush on checkpoint**: Enabled for exactly-once processing

## MySQL CDC Configuration

The application includes optimized CDC settings:

- **Incremental snapshot chunk size**: 5000 rows
- **Snapshot fetch size**: 5000 rows
- **Debezium max batch size**: 2000 events
- **Debezium max queue size**: 20000 events

## Monitoring

The application includes comprehensive logging for monitoring:

- Connection status for MySQL and Elasticsearch
- Data pipeline progress
- Error handling and retry attempts
- Performance metrics

## Troubleshooting

### Common Issues

1. **Connection timeouts**: Check network connectivity and firewall settings
2. **Authentication failures**: Verify credentials and permissions
3. **Schema mismatches**: Ensure MySQL and Elasticsearch schemas are compatible
4. **Memory issues**: Adjust JVM heap size for large datasets

### Logs

Check Flink logs for detailed error messages and performance metrics:
```bash
tail -f flink/log/flink-*.log
```

## Performance Tuning

For high-throughput scenarios, consider adjusting:

- Flink parallelism settings
- Elasticsearch bulk flush parameters
- MySQL CDC chunk sizes
- JVM memory allocation
- Network buffer sizes

## License

This project is licensed under the Apache License 2.0.