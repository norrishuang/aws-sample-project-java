# Amazon Managed Apache Flink (KDA) Java 17 支持指南

## 问题说明

本项目需要 Java 17，但 Amazon Managed Apache Flink 默认使用 Java 11。

## 解决方案

### 方案 1: 使用自定义 Flink 镜像（推荐）

Amazon Managed Apache Flink 支持使用自定义 Docker 镜像，可以包含 Java 17。

#### 步骤 1: 创建 Dockerfile

```dockerfile
# 使用 Flink 1.20 with Java 17 作为基础镜像
FROM flink:1.20.0-java17

# 设置工作目录
WORKDIR /opt/flink

# 复制应用 JAR
COPY target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar /opt/flink/usrlib/

# 复制配置文件（可选）
COPY config.properties /opt/flink/conf/

# 设置 Java 17 环境变量
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 验证 Java 版本
RUN java -version
```

#### 步骤 2: 构建并推送镜像到 ECR

```bash
# 构建项目
mvn clean package

# 构建 Docker 镜像
docker build -t flink-cdc-iceberg:java17 .

# 登录到 ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  123456789012.dkr.ecr.us-east-1.amazonaws.com

# 创建 ECR 仓库（如果不存在）
aws ecr create-repository \
  --repository-name flink-cdc-iceberg \
  --region us-east-1

# 标记镜像
docker tag flink-cdc-iceberg:java17 \
  123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-cdc-iceberg:java17

# 推送镜像
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-cdc-iceberg:java17
```

#### 步骤 3: 创建 KDA 应用使用自定义镜像

使用 AWS CLI:

```bash
aws kinesisanalyticsv2 create-application \
  --application-name MySQLCDCToIceberg \
  --runtime-environment FLINK-1_20 \
  --service-execution-role arn:aws:iam::123456789012:role/KDA-Role \
  --application-configuration '{
    "FlinkApplicationConfiguration": {
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 2,
        "ParallelismPerKPU": 1,
        "AutoScalingEnabled": true
      }
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "FlinkApplicationProperties",
          "PropertyMap": {
            "mysql.hostname": "your-mysql-host",
            "mysql.port": "3306",
            "mysql.database": "testdb",
            "iceberg.catalog.type": "glue",
            "iceberg.warehouse": "s3://your-bucket/warehouse",
            "aws.region": "us-east-1"
          }
        }
      ]
    },
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-cdc-iceberg:java17"
      },
      "CodeContentType": "IMAGE"
    }
  }'
```

使用 CloudFormation:

```yaml
Resources:
  FlinkApplication:
    Type: AWS::KinesisAnalyticsV2::Application
    Properties:
      ApplicationName: MySQLCDCToIceberg
      RuntimeEnvironment: FLINK-1_20
      ServiceExecutionRole: !GetAtt KDARole.Arn
      ApplicationConfiguration:
        FlinkApplicationConfiguration:
          ParallelismConfiguration:
            ConfigurationType: CUSTOM
            Parallelism: 2
            ParallelismPerKPU: 1
            AutoScalingEnabled: true
        EnvironmentProperties:
          PropertyGroups:
            - PropertyGroupId: FlinkApplicationProperties
              PropertyMap:
                mysql.hostname: your-mysql-host
                mysql.port: "3306"
                mysql.database: testdb
                iceberg.catalog.type: glue
                iceberg.warehouse: s3://your-bucket/warehouse
                aws.region: us-east-1
        ApplicationCodeConfiguration:
          CodeContent:
            ImageUri: 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-cdc-iceberg:java17
          CodeContentType: IMAGE
```

### 方案 2: 使用 Amazon EMR on EKS

如果 KDA 的自定义镜像方案不可行，可以使用 EMR on EKS 运行 Flink 作业。

#### 步骤 1: 创建 EKS 集群

```bash
eksctl create cluster \
  --name flink-cluster \
  --region us-east-1 \
  --nodegroup-name standard-workers \
  --node-type m5.xlarge \
  --nodes 3
```

#### 步骤 2: 设置 EMR on EKS

```bash
# 创建 EMR 虚拟集群
aws emr-containers create-virtual-cluster \
  --name flink-virtual-cluster \
  --container-provider '{
    "id": "flink-cluster",
    "type": "EKS",
    "info": {
      "eksInfo": {
        "namespace": "default"
      }
    }
  }'
```

#### 步骤 3: 提交 Flink 作业

```bash
aws emr-containers start-job-run \
  --virtual-cluster-id <virtual-cluster-id> \
  --name mysql-cdc-iceberg \
  --execution-role-arn arn:aws:iam::123456789012:role/EMR-EKS-Role \
  --release-label emr-6.15.0-flink-latest \
  --job-driver '{
    "sparkSubmitJobDriver": {
      "entryPoint": "s3://your-bucket/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar",
      "entryPointArguments": ["--config", "s3://your-bucket/config.properties"],
      "sparkSubmitParameters": "--class com.amazonaws.java.flink.MySQLCDCToIcebergDynamic"
    }
  }' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "flink-conf",
        "properties": {
          "env.java.home": "/usr/lib/jvm/java-17-amazon-corretto"
        }
      }
    ]
  }'
```

### 方案 3: 自托管 Flink on EKS

完全控制 Flink 环境，使用 Flink Kubernetes Operator。

#### 步骤 1: 安装 Flink Kubernetes Operator

```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

#### 步骤 2: 创建 FlinkDeployment

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: mysql-cdc-iceberg
spec:
  image: flink:1.20.0-java17
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.backend: rocksdb
    state.checkpoints.dir: s3://your-bucket/checkpoints
    state.savepoints.dir: s3://your-bucket/savepoints
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: s3://your-bucket/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar
    entryClass: com.amazonaws.java.flink.MySQLCDCToIcebergDynamic
    args:
      - --mysql.hostname
      - your-mysql-host
      - --mysql.database
      - testdb
      - --iceberg.catalog.type
      - glue
      - --iceberg.warehouse
      - s3://your-bucket/warehouse
    parallelism: 2
    upgradeMode: savepoint
    state: running
```

应用配置:

```bash
kubectl apply -f flink-deployment.yaml
```

### 方案 4: 使用 Amazon EC2 自托管

在 EC2 上运行 Flink Standalone 或 Session 集群。

#### 步骤 1: 启动 EC2 实例

```bash
# 使用 Amazon Linux 2023 AMI
aws ec2 run-instances \
  --image-id ami-0c55b159cbfafe1f0 \
  --instance-type m5.xlarge \
  --key-name your-key \
  --security-group-ids sg-xxxxx \
  --subnet-id subnet-xxxxx \
  --iam-instance-profile Name=Flink-EC2-Role
```

#### 步骤 2: 安装 Java 17 和 Flink

```bash
# SSH 到实例
ssh -i your-key.pem ec2-user@<instance-ip>

# 安装 Java 17
sudo yum install -y java-17-amazon-corretto

# 下载 Flink 1.20
wget https://dlcdn.apache.org/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz
tar -xzf flink-1.20.0-bin-scala_2.12.tgz
cd flink-1.20.0

# 启动 Flink 集群
./bin/start-cluster.sh
```

#### 步骤 3: 提交作业

```bash
./bin/flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  /path/to/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --config /path/to/config.properties
```

## KDA 自定义镜像的 IAM 权限

KDA 使用自定义镜像需要以下权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetAuthorizationToken"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

## 验证 Java 版本

在应用启动后，检查日志确认 Java 版本：

```bash
# 查看 KDA 应用日志
aws logs tail /aws/kinesisanalytics/MySQLCDCToIceberg --follow

# 应该看到类似输出：
# OpenJDK Runtime Environment (build 17.0.x+xx)
```

## 成本考虑

| 方案 | 成本 | 复杂度 | 灵活性 |
|------|------|--------|--------|
| KDA 自定义镜像 | 中 | 低 | 中 |
| EMR on EKS | 高 | 中 | 高 |
| 自托管 EKS | 高 | 高 | 最高 |
| EC2 自托管 | 低-中 | 中 | 高 |

## 推荐方案

1. **生产环境**: KDA 自定义镜像（方案 1）- 完全托管，运维简单
2. **需要更多控制**: EMR on EKS（方案 2）- 平衡托管和灵活性
3. **完全自定义**: 自托管 EKS（方案 3）- 最大灵活性
4. **开发测试**: EC2 自托管（方案 4）- 成本最低

## 参考资料

- [Amazon Managed Apache Flink Custom Images](https://docs.aws.amazon.com/managed-flink/latest/java/how-custom-images.html)
- [Flink on Kubernetes](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/native_kubernetes/)
- [EMR on EKS](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html)

---

**最后更新**: 2026-02-02  
**版本**: v1.0
