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

The application accepts configuration parameters from multiple sources in priority order:
1. **Command-line arguments**: Passed via `--param.name value` format
2. **Environment variables**: Fallback if command-line arguments are not provided
3. **Default values**: Placeholders indicating required configuration

### MySQL Source Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `mysql.hostname` | `MYSQL_HOSTNAME` | `<mysql-host>` | MySQL server hostname |
| `mysql.port` | `MYSQL_PORT` | `3306` | MySQL server port |
| `mysql.username` | `MYSQL_USERNAME` | `<mysql-username>` | MySQL username |
| `mysql.password` | `MYSQL_PASSWORD` | `<mysql-password>` | MySQL password |
| `mysql.database` | `MYSQL_DATABASE` | `<mysql-database>` | Database name |
| `mysql.table` | `MYSQL_TABLE` | `<mysql-table>` | Table name |
| `mysql.server-timezone` | `MYSQL_SERVER_TIMEZONE` | `UTC` | Server timezone |

### OpenSearch Sink Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `opensearch.host` | `OPENSEARCH_HOST` | `<opensearch-host>` | OpenSearch endpoint URL (e.g., https://your-domain:443) |
| `opensearch.index` | `OPENSEARCH_INDEX` | `<opensearch-index>` | OpenSearch index name |
| `opensearch.username` | `OPENSEARCH_USERNAME` | `<opensearch-username>` | OpenSearch username |
| `opensearch.password` | `OPENSEARCH_PASSWORD` | `<opensearch-password>` | OpenSearch password |

**Note**: Parameters with placeholder defaults (e.g., `<mysql-host>`) must be provided via command-line arguments or environment variables. The application will not function correctly with placeholder values.

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

#### Using Command-line Arguments

```bash
flink run -c com.amazonaws.java.flink.MySQLCDCToOpenSearch \
  target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar \
  --mysql.hostname your-mysql-host.rds.amazonaws.com \
  --mysql.username your-username \
  --mysql.password your-password \
  --mysql.database your-database \
  --mysql.table your-table \
  --opensearch.host https://your-opensearch-domain:443 \
  --opensearch.index your-index \
  --opensearch.username your-os-username \
  --opensearch.password your-os-password
```

#### Using Environment Variables

```bash
export MYSQL_HOSTNAME=your-mysql-host.rds.amazonaws.com
export MYSQL_USERNAME=your-username
export MYSQL_PASSWORD=your-password
export MYSQL_DATABASE=your-database
export MYSQL_TABLE=your-table
export OPENSEARCH_HOST=https://your-opensearch-domain:443
export OPENSEARCH_INDEX=your-index
export OPENSEARCH_USERNAME=your-os-username
export OPENSEARCH_PASSWORD=your-os-password

flink run -c com.amazonaws.java.flink.MySQLCDCToOpenSearch \
  target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar
```

### On Amazon Managed Apache Flink (KDA)

1. Upload the JAR to S3
2. Create a Flink application in KDA console
3. Configure the application properties in KDA with the parameters listed above (e.g., `mysql.hostname`, `mysql.username`, etc.)
4. Alternatively, configure environment variables in the KDA application settings
5. Start the application

**Example Application Properties Configuration for KDA:**

```json
{
  "PropertyGroups": [
    {
      "PropertyGroupId": "MySQLSource",
      "PropertyMap": {
        "mysql.hostname": "your-mysql-host.rds.amazonaws.com",
        "mysql.username": "your-username",
        "mysql.password": "your-password",
        "mysql.database": "your-database",
        "mysql.table": "your-table"
      }
    },
    {
      "PropertyGroupId": "OpenSearchSink",
      "PropertyMap": {
        "opensearch.host": "https://your-opensearch-domain:443",
        "opensearch.index": "your-index",
        "opensearch.username": "your-os-username",
        "opensearch.password": "your-os-password"
      }
    }
  ]
}
```

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

## Security Best Practices

**Important**: This application no longer contains hardcoded credentials. Always follow these security best practices:

1. **Never commit sensitive credentials to source control**
2. **Use secure credential management**:
   - AWS Secrets Manager for storing database and OpenSearch credentials
   - IAM roles and policies for AWS service access
   - Environment variables for containerized deployments
3. **For Amazon Managed Apache Flink (KDA)**:
   - Store sensitive parameters in AWS Secrets Manager
   - Reference secrets in KDA application configuration
   - Use IAM roles for service-to-service authentication when possible
4. **For EMR Flink**:
   - Use environment variables or secure parameter stores
   - Avoid passing passwords in command-line arguments (visible in process lists)
   - Consider using configuration files with restricted permissions
5. **Network Security**:
   - Ensure MySQL and OpenSearch endpoints are in private subnets
   - Use VPC peering or PrivateLink for secure connectivity
   - Enable SSL/TLS for all connections
