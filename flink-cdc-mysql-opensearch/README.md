# Flink CDC MySQL to OpenSearch

This Flink application streams data from MySQL to OpenSearch using Change Data Capture (CDC).

## Recent Updates (January 2026)

✅ **Version Compatibility Fixed**: Resolved `ClassNotFoundException` and `NoClassDefFoundError` issues by aligning OpenSearch dependency versions  
✅ **OpenSearch 2.19 Support**: Tested and verified compatibility with OpenSearch 2.19 clusters  
✅ **Enhanced CDC Configuration**: Added optimized incremental snapshot settings for better performance  
✅ **Dual Deployment Support**: Full support for both Amazon Managed Apache Flink (KDA) and EMR Flink  

For detailed upgrade information, see [UPGRADE_NOTES.md](UPGRADE_NOTES.md).

## Features

- **MySQL CDC Source**: Captures change data from MySQL database using Flink CDC 3.5.0 connector
- **OpenSearch Sink**: Writes data to OpenSearch 2.x using official Apache Flink OpenSearch Connector 2.0.0-1.19
- **Flink 1.19.1**: Built on Apache Flink 1.19.1 with Table API/SQL
- **Real-time Pipeline**: Streams INSERT, UPDATE, and DELETE operations from MySQL to OpenSearch
- **Version Compatibility**: Optimized dependency versions for compatibility with OpenSearch 2.19 clusters
- **Dual Deployment**: Supports both Amazon Managed Apache Flink (KDA) and EMR Flink deployments

## Architecture

```
MySQL Database (CDC) --> Flink Stream Processing --> OpenSearch 2.x Index
```

## Configuration

The application accepts configuration parameters from multiple sources in priority order:
1. **KDA Application Properties**: For Amazon Managed Apache Flink (KDA) deployments
2. **Command-line arguments**: Passed via `--param.name value` format for EMR Flink
3. **Default values**: Fallback values for development and testing

### MySQL Source Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `mysql.hostname` | `MYSQL_HOSTNAME` | `<mysql-host>` | MySQL server hostname |
| `mysql.port` | `MYSQL_PORT` | `3306` | MySQL server port |
| `mysql.username` | `MYSQL_USERNAME` | `admin` | MySQL username |
| `mysql.password` | `MYSQL_PASSWORD` | `<password>` | MySQL password |
| `mysql.database` | `MYSQL_DATABASE` | `norrisdb` | Database name |
| `mysql.table` | `MYSQL_TABLE` | `user_order_list_20cols_new` | Table name |
| `mysql_server_timezone` | `MYSQL_SERVER_TIMEZONE` | `UTC` | Server timezone |

### OpenSearch Sink Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `opensearch.host` | `OPENSEARCH_HOST` | `https://<host>:443` | OpenSearch endpoint URL |
| `opensearch.index` | `OPENSEARCH_INDEX` | `user_order_list_20cols_new` | OpenSearch index name |
| `opensearch.username` | `OPENSEARCH_USERNAME` | `admin` | OpenSearch username |
| `opensearch.password` | `OPENSEARCH_PASSWORD` | `ABCDEF` | OpenSearch password |

**Note**: Parameters with placeholder defaults (e.g., `<mysql-host>`) must be provided via command-line arguments or environment variables. The application will not function correctly with placeholder values.

## Schema

The pipeline processes the following schema (28 columns):

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
| price | DECIMAL(6,2) | Product price |
| unit | INT | Quantity unit |
| email | STRING | Email address |
| address | STRING | Address |
| city | STRING | City |
| country | STRING | Country |
| ip_address | STRING | IP address |
| website | STRING | Website |
| company_name | STRING | Company name |
| department | STRING | Department |
| salary | DECIMAL(8,2) | Salary |
| age | INT | Age |
| gender | STRING | Gender |
| status | STRING | Status |
| created_date | TIMESTAMP | Creation date |
| last_login | TIMESTAMP | Last login timestamp |
| score | INT | Score |
| description | STRING | Description |
| created_at | TIMESTAMP | Creation timestamp |
| updated_at | TIMESTAMP | Update timestamp |

## Building

Build the uber JAR using Maven:

```bash
mvn clean package -DskipTests
```

The output JAR will be located at: `target/flink-cdc-mysql-opensearch-1.0-SNAPSHOT.jar`

**Build Output**:
- JAR size: ~150MB (includes all dependencies)
- Type: Shaded/Uber JAR with all OpenSearch dependencies
- Main class: `com.amazonaws.java.flink.MySQLCDCToOpenSearch`

**Requirements**:
- Maven 3.6+
- JDK 11+
- Internet connection (for downloading dependencies)

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

The OpenSearch sink is configured with optimized bulk flush settings for high-throughput data ingestion:

- **Max actions**: 1000 (maximum number of actions per bulk request)
- **Max size**: 5MB (maximum size of bulk request)
- **Interval**: 3 seconds (time interval between bulk flushes)
- **Backoff strategy**: EXPONENTIAL (retry strategy for failed requests)
- **Max retries**: 20 (maximum number of retry attempts)
- **Backoff delay**: 1000ms (initial delay before retry)

These settings are optimized for:
- High throughput data ingestion
- Efficient network utilization
- Automatic retry on transient failures
- Balance between latency and throughput

## Error Handling

The application includes comprehensive error handling:

- **Logging**: Proper error logging using SLF4J with detailed context
- **Exception Handling**: Descriptive error messages for troubleshooting
- **Automatic Retry**: Built-in retry mechanism for OpenSearch writes with exponential backoff
- **Graceful Degradation**: Handles transient failures without stopping the pipeline
- **Checkpoint Recovery**: Automatic recovery from failures using Flink checkpoints

### CDC Configuration Optimizations

The application uses enhanced CDC configuration for better performance:

```properties
scan.incremental.snapshot.enabled = true
scan.incremental.snapshot.chunk.size = 8092
scan.incremental.snapshot.unbounded-chunk-first.enabled = true
scan.snapshot.fetch.size = 2000
debezium.max.batch.size = 1000
debezium.max.queue.size = 1000
```

These settings optimize:
- Initial snapshot performance
- Memory usage during CDC
- Throughput for change data capture
- Queue management for high-volume changes

## Dependencies

### Core Dependencies
- **Apache Flink**: 1.19.1
- **Flink CDC MySQL Connector**: 3.5.0
- **Apache Flink OpenSearch Connector**: 2.0.0-1.19 (Official)
- **AWS Kinesis Analytics Runtime**: 1.2.0

### OpenSearch Dependencies
- **OpenSearch Core**: 2.11.1 (compatible with OpenSearch 2.19 clusters)
- **OpenSearch Java Client**: 2.6.0
- **OpenSearch REST Client**: 2.11.1
- **OpenSearch REST High-Level Client**: 2.11.1

### Other Dependencies
- **Jackson**: 2.18.2
- **JDK**: 11+ (required for OpenSearch 2.x client libraries)

### Version Compatibility Matrix

| Component | Version | Compatibility |
|-----------|---------|---------------|
| Flink | 1.19.1 | ✅ |
| flink-connector-opensearch2 | 2.0.0-1.19 | ✅ |
| OpenSearch Core Libraries | 2.11.1 | ✅ |
| OpenSearch Cluster | 2.19 | ✅ Compatible |
| MySQL CDC | 3.5.0 | ✅ |
| JDK | 11+ | ✅ Required |

**Important**: The OpenSearch dependency versions (2.11.1) are specifically chosen for compatibility with the Flink OpenSearch connector. These versions work correctly with OpenSearch 2.19 clusters.

## Notes

- The application uses the official Apache Flink `opensearch-2` connector for OpenSearch 2.x compatibility
- Checkpointing is configured with 5-minute intervals (matching Elasticsearch project)
- The OpenSearch connector provides native support for OpenSearch 2.x features and APIs
- Enhanced CDC configuration with incremental snapshot and optimized batch settings
- Supports both Amazon Managed Apache Flink (KDA) and EMR Flink deployments
- Uses OpenSearch 2.11.1 core libraries (compatible with OpenSearch 2.19 clusters)
- JDK 11+ is required for OpenSearch 2.x client libraries
- For production use, ensure proper security configurations and credential management

## Troubleshooting

### Common Issues

#### ClassNotFoundException or NoClassDefFoundError
**Symptoms**: 
- `ClassNotFoundException: org.opensearch.common.io.stream.NamedWriteable`
- `NoClassDefFoundError: org/opensearch/core/common/io/stream/StreamOutput`

**Solution**: These issues have been resolved by aligning OpenSearch dependency versions. Ensure you're using the latest build of the application with OpenSearch 2.11.1 dependencies.

#### Version Compatibility Issues
**Symptom**: Connector initialization failures or runtime errors

**Solution**: Verify that you're using the correct version combination:
- Flink 1.19.1
- flink-connector-opensearch2:2.0.0-1.19
- OpenSearch core libraries 2.11.1
- OpenSearch cluster 2.19 (or any 2.x version)

#### Connection Issues
**Symptoms**: Unable to connect to MySQL or OpenSearch

**Checklist**:
1. ✅ Verify network connectivity from Flink cluster to MySQL and OpenSearch
2. ✅ Check security group and firewall rules
3. ✅ Validate credentials (username/password)
4. ✅ Ensure OpenSearch endpoint URL includes protocol (https://)
5. ✅ Verify MySQL binlog is enabled for CDC

#### Performance Issues
**Symptoms**: Slow data ingestion or high latency

**Optimization Tips**:
1. Adjust bulk flush settings based on your workload
2. Increase parallelism for higher throughput
3. Monitor checkpoint duration and adjust interval if needed
4. Review MySQL CDC configuration parameters
5. Check OpenSearch cluster health and capacity

For more detailed troubleshooting information, see [UPGRADE_NOTES.md](UPGRADE_NOTES.md).

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
