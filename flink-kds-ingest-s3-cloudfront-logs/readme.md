# CloudFront Realtime Log Ingestion with Flink

This project demonstrates how to ingest CloudFront logs in realtime using Flink. The project consumer the Kinesis Data Stream, writes the logs to S3 in Parquet format.

perquisites:
    The cloudfront logs need to be enabled realtimelogging, and the logs will be sent to Kinesis Data Stream.

You need select the following fields, and create the table in Athena.
> The fields are separated by tab. So if you want to custom the fields, you need to change the delimiter in the code.

```asciidoc
timestamp
c-ip
s-ip
time-to-first-byte
sc-status
sc-bytes
cs-method
cs-protocol
cs-host
cs-uri-stem
cs-bytes
x-edge-location
x-edge-request-id
x-host-header
time-taken
cs-protocol-version
c-ip-version
cs-user-agent
cs-referer
cs-cookie
cs-uri-query
x-edge-response-result-type
x-forwarded-for
ssl-protocol
ssl-cipher
x-edge-result-type
fle-encrypted-fields
fle-status
sc-content-type
sc-content-len
sc-range-start
sc-range-end
c-port
x-edge-detailed-result-type
c-country
cs-accept-encoding
cs-accept
cache-behavior-path-pattern
cs-headers
cs-header-names
cs-headers-count
primary-distribution-id
primary-distribution-dns-name
origin-fbl
origin-lbl
asn
r-host
sr-reason
x-edge-mqcs
```

# Create Table in Athena
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS s3_db.cloudfront_real_time_logs ( 
  `cport` int, 
  `cachebehaviorpathpattern` string, 
  `cip` string, 
  `cipcountry` string, 
  `cipversion` string, 
  `csaccept` string, 
  `csacceptencoding` string, 
  `csbytes` int, 
  `cscookie` string, 
  `csheadernames` string, 
  `csheaders` string, 
  `csheaderscount` int, 
  `cshost` string, 
  `csmethod` string, 
  `csproto` string, 
  `csrefer` string, 
  `csuriquery` string, 
  `csurl` string, 
  `csuseragent` string, 
  `csversion` string, 
  `fleencryptedfields` string, 
  `flestatus` string, 
  `scbytes` int, 
  `sccontentlen` int, 
  `sccontenttype` string, 
  `scrangeend` int, 
  `scrangestart` int, 
  `scstatus` int, 
  `sip` string, 
  `sslcipher` string, 
  `sslprotocol` string, 
  `timetaken` float, 
  `timetofirstbyte` float, 
  `timestamp` float, 
  `xedgedetailedresulttype` string, 
  `xedgelocation` string, 
  `xedgerequestid` string, 
  `xedgeresponseresulttype` string, 
  `xedgeresulttype` string, 
  `xforwardedfor` string, 
  `xhostheader` string)
PARTITIONED BY (
  YEAR INT,
  MONTH INT,
  DAY INT
)
STORED AS PARQUET
LOCATION 's3://<s3bucket>/data/cloudfront-logs/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

## Refresh partitions
need refresh the partitions after the logs are written to S3.
```sql
MSCK REPAIR TABLE s3_db.cloudfront_real_time_logs
```

# Build the project
```shell
mvn clean package
```

# Submit the job to Flink
> from EMR 7.5.0, you can run the flink job from S3 directly, no need to upload the jar to EMR.

**upload the jar to S3**
```shell
aws s3 cp target/flink-kds-ingest-s3-cloudfront-logs-1.0-SNAPSHOT.jar s3://<s3-bucket>/jars/
```

**submit**
```shell
DATA_STREAM=<datastream>
S3_PATH=<s3-path>
AWS_REGION=<aws-region>
checkpoints=s3://<s3-bucket>/checkpoint/

flink run-application -t yarn-application \
-Dyarn.application.name=flink-kds-cloudfront-logs \
-Dparallelism.default=2 \
-Djobmanager.memory.process.size=2048mb \
-Dtaskmanager.memory.process.size=2048mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-D state.checkpoint-storage=filesystem \
-D state.checkpoints.dir=${checkpoints} \
-D execution.checkpointing.interval=60000 \
-D state.checkpoints.num-retained=2 \
-D execution.checkpointing.mode=EXACTLY_ONCE \
-D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
-D execution.checkpointing.max-concurrent-checkpoints=2 \
-D execution.checkpointing.checkpoints-after-tasks-finish.enabled=true \
-Dclassloader.resolve-order=parent-first \
s3://<s3-bucket>/jars/ \
--kinesis.stream.name $DATA_STREAM \
--aws.region $AWS_REGION \
--s3.path $S3_PATH
```
