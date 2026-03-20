# AWS Sample Project Java

A collection of Apache Flink streaming application samples designed to run on AWS managed services, including **Amazon EMR**, **EMR on EKS**, and **Amazon Managed Service for Apache Flink (MSF)**. These samples demonstrate real-time data ingestion, CDC pipelines, and analytics workloads using various AWS data sources and sinks.

## Sub-Projects

### CDC Pipelines

Real-time Change Data Capture from MySQL to various target systems using Flink CDC.

| Project | Description | Runtime | Source → Sink |
|---------|-------------|---------|---------------|
| [flink-cdc-mysql-elasticsearch](./flink-cdc-mysql-elasticsearch) | Real-time sync from MySQL to Elasticsearch via Flink CDC | MSF / EMR | MySQL → Elasticsearch |
| [flink-cdc-mysql-iceberg-dynamic](./flink-cdc-mysql-iceberg-dynamic) | Dynamic Sink API with auto schema evolution and dynamic table routing | EMR | MySQL → Iceberg |
| [flink-cdc-mysql-opensearch](./flink-cdc-mysql-opensearch) | Real-time sync from MySQL to OpenSearch via Flink CDC | EMR | MySQL → OpenSearch |
| [flink-cdc-mysql-s3tables](./flink-cdc-mysql-s3tables) | CDC to AWS S3 Tables using Iceberg REST Catalog | EMR | MySQL → S3 Tables (Iceberg) |

### Kafka Ingestion

Consume data from Apache Kafka (Amazon MSK) and write to various storage systems.

| Project | Description | Runtime | Source → Sink |
|---------|-------------|---------|---------------|
| [flink-kafka-hbase](./flink-kafka-hbase) | Kafka to HBase ingestion using Flink SQL | EMR | Kafka → HBase |
| [flink-kafka-ingest-iceberg-kda](./flink-kafka-ingest-iceberg-kda) | Kafka to Iceberg real-time ingestion | MSF / EMR | Kafka → Iceberg |
| [flink-kafka-opensearch](./flink-kafka-opensearch) | Kafka to OpenSearch ingestion | EMR on EKS | Kafka → OpenSearch |
| [flink-kafka-s3](./flink-kafka-s3) | Kafka to S3 in Parquet format (supports mTLS) | EMR | Kafka → S3 (Parquet) |
| [flink-kafka-s3-kda](./flink-kafka-s3-kda) | Kafka to S3 in Parquet format (Flink 1.18+, supports mTLS) | MSF | Kafka → S3 (Parquet) |

### Kinesis Data Streams Ingestion

Consume data from Amazon Kinesis Data Streams (KDS) and write to various storage systems.

| Project | Description | Runtime | Source → Sink |
|---------|-------------|---------|---------------|
| [flink-kds-ingest-hudi-kda](./flink-kds-ingest-hudi-kda) | KDS to S3 in Apache Hudi format | MSF | KDS → S3 (Hudi) |
| [flink-kds-ingest-iceberg-kda](./flink-kds-ingest-iceberg-kda) | KDS to S3 in Apache Iceberg format | MSF | KDS → S3 (Iceberg) |
| [flink-kds-ingest-s3-cloudfront-logs](./flink-kds-ingest-s3-cloudfront-logs) | Ingest CloudFront access logs from KDS to S3 | MSF | KDS (CloudFront Logs) → S3 |
| [flink-opensearch-kda](./flink-opensearch-kda) | KDS to OpenSearch ingestion | MSF | KDS → OpenSearch |

### SQL & Analytics

Flink SQL-based analytics workloads.

| Project | Description | Runtime | Source → Sink |
|---------|-------------|---------|---------------|
| [flink-sql-glue-tpcds](./flink-sql-glue-tpcds) | Flink SQL queries against TPC-DS data via AWS Glue Data Catalog | EMR (Session / Application Mode) | Glue Catalog → Query Results |

## Prerequisites

- **Java** 18+
- **Apache Maven** 3.6+
- **AWS Account** with appropriate service access (EMR, MSF, MSK, OpenSearch, S3, KDS, etc.)
- **AWS CLI** configured with valid credentials

## Build

```bash
# Build all sub-projects
mvn clean package

# Build a specific sub-project
mvn clean package -pl flink-cdc-mysql-opensearch
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
