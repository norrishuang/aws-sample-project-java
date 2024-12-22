# From Kafka to S3

> Flink 1.18.1, Running in Managed Apache Flink Service. 

In [KafkaS3SinkParquet.java](https://github.com/norrishuang/aws-sample-project-java/blob/main/flink-kafka-s3/src/main/java/com/amazonaws/java/flink/KafkaS3SinkParquet.java), It implement connect MSK with mutual TLS. [How to create certification files](https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html)

```shell
cp /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.amzn2.0.1.x86_64/jre/lib/security/cacerts kafka.client.truststore.jks

keytool -genkey -keystore kafka.client.keystore.jks -validity 300 -storepass <password> -keypass <password> -dname "C=CN, O=Example LLC, OU=Engineering, ST=GuangDong, CN=Private Root CA1, L=ShenZhen" -alias kafkaclient -storetype pkcs12

keytool -keystore kafka.client.keystore.jks -certreq -file client-cert-sign-request -alias kafkaclient -storepass <password> -keypass <password>

aws acm-pca issue-certificate --certificate-authority-arn <aws-acm-arn> --csr fileb://client-cert-sign-request --signing-algorithm "SHA256WITHRSA" --validity Value=300,Type="DAYS"

aws acm-pca get-certificate --certificate-authority-arn <aws-acm-arn> --certificate-arn <aws-acm-arn-response-from-above-command>

keytool -keystore kafka.client.keystore.jks -import -file signed-certificate-from-acm -alias kafkaclient -storepass <password> -keypass <password>

```



When enable the TLS, there is a default truststore, set
`truststore.location=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.amzn2.0.1.x86_64/jre/lib/security/cacerts`

