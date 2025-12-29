# Flink CDC MySQL to OpenSearch

This Flink application streams data from MySQL to OpenSearch using Change Data Capture (CDC).

## Features

- **MySQL CDC Source**: Captures change data from MySQL database using Flink CDC 3.x connector
- **OpenSearch Sink**: Writes data to OpenSearch using Elasticsearch-7 connector (for compatibility)
- **Flink 1.20**: Built on Apache Flink 1.20 with Table API/SQL
- **Real-time Pipeline**: Streams INSERT, UPDATE, and DELETE operations from MySQL to OpenSearch

## Architecture

```
MySQL Database (CDC) --> Flink Stream Processing --> OpenSearch Index
```

## Configuration

The application accepts the following parameters:

### MySQL Source Configuration
- `mysql.hostname`: MySQL server hostname (default: mysql-cdc-db.cghfgy0zyjlk.us-east-1.rds.amazonaws.com)
- `mysql.port`: MySQL server port (default: 3306)
- `mysql.username`: MySQL username (default: admin)
- `mysql.password`: MySQL password (default: Amazon123)
- `mysql.database`: Database name (default: norrisdb)
- `mysql.table`: Table name (default: user_order_list)
- `mysql.server-timezone`: Server timezone (default: UTC)

### OpenSearch Sink Configuration
- `opensearch.host`: OpenSearch endpoint URL (default: https://search-es-beg-test-bvu7xxqngc6kvt3tyh2rgqywk4.us-east-1.es.amazonaws.com:443)
- `opensearch.index`: OpenSearch index name (default: user_order_list)
- `opensearch.username`: OpenSearch username (default: admin)
- `opensearch.password`: OpenSearch password (default: C6VxZyep)8>2)

## Schema

The pipeline processes the following schema:

| Field | Type | Description |
|-------|------|-------------|
| id | INT | Primary key |
| uuid | STRING | Unique identifier |
| user_name | STRING | User name |
| phone_number | STRING | Phone number |
| product_id | INT | Product identifier |
| product_name | STRING | Product name |
| product_type | STRING | Product type |
| manufacturing_date | INT | Manufacturing date |
| price | FLOAT | Product price |
| unit | INT | Quantity unit |
| created_at | TIMESTAMP(3) | Creation timestamp |
| updated_at | TIMESTAMP(3) | Update timestamp |
| test1 | INT | Test field |

## Building

Build the uber JAR using Maven:

```bash
mvn clean package
```

The output JAR will be located at: `target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar`

## Running

### On EMR Flink

```bash
flink run -c com.amazonaws.java.flink.MySQLCDCToOpenSearch \
  target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar \
  --mysql.hostname your-mysql-host \
  --mysql.username your-username \
  --mysql.password your-password \
  --opensearch.host https://your-opensearch-domain:443 \
  --opensearch.username your-os-username \
  --opensearch.password your-os-password
```

### On Amazon Managed Apache Flink (KDA)

1. Upload the JAR to S3
2. Create a Flink application in KDA console
3. Configure the application properties in KDA with the parameters listed above
4. Start the application

## Bulk Flush Settings

The OpenSearch sink is configured with optimized bulk flush settings:

- Max actions: 1000
- Max size: 5MB
- Interval: 5 seconds
- Backoff strategy: EXPONENTIAL
- Max retries: 3
- Backoff delay: 1 second

## Error Handling

The application includes:
- Proper error logging using SLF4J
- Exception handling with descriptive error messages
- Automatic retry mechanism for OpenSearch writes

## Dependencies

- Apache Flink 1.20.0
- Flink CDC MySQL Connector 3.2.1
- Flink Elasticsearch-7 SQL Connector 3.0.1-1.17
- Jackson 2.18.2

## Notes

- The application uses the `elasticsearch-7` connector for OpenSearch compatibility
- Checkpointing is configured with 1-minute intervals
- The Elasticsearch-7 connector is compatible with OpenSearch 1.x and 2.x
- For production use, ensure proper security configurations and credential management
