# Java 版本要求和故障排查

## 版本要求

本项目**必须使用 Java 17 或更高版本**。

### 为什么需要 Java 17？

Iceberg 1.11.0-SNAPSHOT 使用 Java 17 编译（class file version 61.0），无法在 Java 11 环境中运行。

### 版本对照表

| Java 版本 | Class File Version | 状态 |
|-----------|-------------------|------|
| Java 8    | 52.0              | ❌ 不支持 |
| Java 11   | 55.0              | ❌ 不支持 |
| Java 17   | 61.0              | ✅ 支持 |
| Java 21   | 65.0              | ✅ 支持 |

## 常见错误

### 错误 1: UnsupportedClassVersionError

**完整错误信息**:
```
java.lang.UnsupportedClassVersionError: org/apache/iceberg/types/Type 
has been compiled by a more recent version of the Java Runtime 
(class file version 61.0), this version of the Java Runtime only 
recognizes class file versions up to 55.0
    at java.base/java.lang.ClassLoader.defineClass1(Native Method)
    at java.base/java.lang.ClassLoader.defineClass(ClassLoader.java:1012)
    ...
```

**原因**: 使用 Java 11 运行了用 Java 17 编译的代码。

**解决方案**: 升级到 Java 17（见下文）。

### 错误 2: 编译时 Java 版本不匹配

**错误信息**:
```
[ERROR] Source option 17 is not supported. Use 11 or later.
```

**原因**: Maven 使用的 Java 版本低于 17。

**解决方案**: 
```bash
# 检查 Maven 使用的 Java 版本
mvn -version

# 设置 JAVA_HOME
export JAVA_HOME=/path/to/java17
```

## 检查当前 Java 版本

### 1. 检查运行时 Java 版本

```bash
java -version
```

**期望输出**:
```
openjdk version "17.0.x" 2024-xx-xx
OpenJDK Runtime Environment (build 17.0.x+xx)
OpenJDK 64-Bit Server VM (build 17.0.x+xx, mixed mode, sharing)
```

### 2. 检查编译时 Java 版本

```bash
javac -version
```

**期望输出**:
```
javac 17.0.x
```

### 3. 检查 Maven 使用的 Java 版本

```bash
mvn -version
```

**期望输出**:
```
Apache Maven 3.x.x
Maven home: /usr/local/maven
Java version: 17.0.x, vendor: Oracle Corporation
Java home: /usr/lib/jvm/java-17-openjdk
...
```

## 安装 Java 17

### macOS

#### 使用 Homebrew (推荐)
```bash
# 安装 OpenJDK 17
brew install openjdk@17

# 设置为默认 Java
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# 设置环境变量
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> ~/.zshrc
source ~/.zshrc
```

#### 使用 SDKMAN
```bash
# 安装 SDKMAN
curl -s "https://get.sdkman.io" | bash

# 安装 Java 17
sdk install java 17.0.10-tem

# 设置为默认
sdk default java 17.0.10-tem
```

### Linux (Ubuntu/Debian)

```bash
# 更新包列表
sudo apt update

# 安装 OpenJDK 17
sudo apt install openjdk-17-jdk

# 设置为默认 Java
sudo update-alternatives --config java

# 设置环境变量
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### Amazon Linux 2

```bash
# 安装 Java 17
sudo amazon-linux-extras install java-openjdk17

# 验证安装
java -version

# 设置环境变量
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto' >> ~/.bashrc
source ~/.bashrc
```

### CentOS/RHEL

```bash
# 安装 OpenJDK 17
sudo yum install java-17-openjdk-devel

# 设置为默认 Java
sudo alternatives --config java

# 设置环境变量
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk' >> ~/.bashrc
source ~/.bashrc
```

### Windows

1. 下载 OpenJDK 17:
   - [Adoptium (推荐)](https://adoptium.net/)
   - [Oracle JDK](https://www.oracle.com/java/technologies/downloads/#java17)

2. 运行安装程序

3. 设置环境变量:
   - 系统属性 → 高级 → 环境变量
   - 新建 `JAVA_HOME`: `C:\Program Files\Java\jdk-17`
   - 编辑 `Path`: 添加 `%JAVA_HOME%\bin`

## 在不同环境中配置

### 本地开发

```bash
# 设置 JAVA_HOME
export JAVA_HOME=/path/to/java17
export PATH=$JAVA_HOME/bin:$PATH

# 验证
java -version
mvn -version
```

### Docker

```dockerfile
FROM flink:1.20.0-java17

# 复制 JAR
COPY target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar /opt/flink/usrlib/

# 运行作业
CMD ["standalone-job", "--job-classname", "com.amazonaws.java.flink.MySQLCDCToIcebergDynamic"]
```

### Amazon Managed Service for Apache Flink (KDA)

KDA 支持 Java 17，在创建应用时选择：
- Runtime: Apache Flink 1.20
- Java Version: Java 17

### Amazon EMR

```bash
# EMR 6.10+ 支持 Java 17
aws emr create-cluster \
  --release-label emr-6.10.0 \
  --applications Name=Flink \
  --configurations '[
    {
      "Classification": "flink-conf",
      "Properties": {
        "env.java.home": "/usr/lib/jvm/java-17-amazon-corretto"
      }
    }
  ]'
```

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: flink-taskmanager
spec:
  containers:
  - name: flink
    image: flink:1.20.0-java17
    env:
    - name: JAVA_HOME
      value: /usr/lib/jvm/java-17-openjdk-amd64
```

## 多版本 Java 管理

### 使用 SDKMAN (推荐)

```bash
# 列出已安装的 Java 版本
sdk list java

# 切换版本
sdk use java 17.0.10-tem

# 设置默认版本
sdk default java 17.0.10-tem
```

### 使用 update-alternatives (Linux)

```bash
# 列出已安装的 Java 版本
sudo update-alternatives --config java

# 选择 Java 17
# 输入对应的数字
```

### 使用 jenv (macOS/Linux)

```bash
# 安装 jenv
brew install jenv

# 添加 Java 版本
jenv add /path/to/java17

# 设置全局版本
jenv global 17.0

# 设置项目版本
cd /path/to/project
jenv local 17.0
```

## 验证配置

### 完整验证脚本

```bash
#!/bin/bash

echo "=== Java 版本检查 ==="
echo ""

# 检查 java
echo "1. Java 运行时版本:"
java -version 2>&1 | head -1
echo ""

# 检查 javac
echo "2. Java 编译器版本:"
javac -version 2>&1
echo ""

# 检查 JAVA_HOME
echo "3. JAVA_HOME 环境变量:"
echo $JAVA_HOME
echo ""

# 检查 Maven
echo "4. Maven 使用的 Java 版本:"
mvn -version 2>&1 | grep "Java version"
echo ""

# 检查版本是否满足要求
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -ge 17 ]; then
    echo "✅ Java 版本满足要求 (>= 17)"
else
    echo "❌ Java 版本不满足要求 (需要 >= 17，当前: $JAVA_VERSION)"
    exit 1
fi
```

保存为 `check-java.sh` 并运行：
```bash
chmod +x check-java.sh
./check-java.sh
```

## 编译和运行

### 编译项目

```bash
# 确保使用 Java 17
java -version

# 清理并编译
mvn clean package -DskipTests

# 检查生成的 JAR
ls -lh target/*.jar
```

### 运行项目

```bash
# 本地运行
flink run \
  -c com.amazonaws.java.flink.MySQLCDCToIcebergDynamic \
  target/flink-cdc-mysql-iceberg-dynamic-1.0-SNAPSHOT.jar \
  --config config.properties
```

## 常见问题

### Q1: 我的系统只有 Java 11，可以降级项目吗？

**A**: 不可以。Iceberg 1.11.0-SNAPSHOT 必须使用 Java 17。如果必须使用 Java 11，需要：
1. 等待 Iceberg 1.11.0 正式版发布（可能会有 Java 11 版本）
2. 或者使用 Iceberg 1.6.x 版本（但不支持 Dynamic Sink）

### Q2: Flink 1.20 支持 Java 17 吗？

**A**: 是的，Flink 1.20.0 完全支持 Java 17。

### Q3: 在生产环境中使用 Java 17 安全吗？

**A**: 是的，Java 17 是 LTS（长期支持）版本，支持到 2029 年 9 月。

### Q4: 如何在 CI/CD 中指定 Java 版本？

**A**: 
```yaml
# GitHub Actions
- uses: actions/setup-java@v3
  with:
    java-version: '17'
    distribution: 'temurin'

# GitLab CI
image: maven:3.9-eclipse-temurin-17

# Jenkins
tools {
    jdk 'JDK17'
}
```

## 参考资料

- [Java SE 17 Documentation](https://docs.oracle.com/en/java/javase/17/)
- [Flink Java Version Requirements](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/java_compatibility/)
- [Iceberg Java Requirements](https://iceberg.apache.org/docs/latest/)
- [SDKMAN Documentation](https://sdkman.io/)

---

**最后更新**: 2026-02-02  
**版本**: v2.3
