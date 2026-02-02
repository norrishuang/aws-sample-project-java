#!/bin/bash

echo "=========================================="
echo "环境诊断脚本 - Java 版本检查"
echo "=========================================="
echo ""

# 检查 Java 运行时版本
echo "1. Java 运行时版本 (java -version):"
java -version 2>&1 | head -3
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
echo ""

# 检查 Java 编译器版本
echo "2. Java 编译器版本 (javac -version):"
javac -version 2>&1
echo ""

# 检查 JAVA_HOME
echo "3. JAVA_HOME 环境变量:"
if [ -z "$JAVA_HOME" ]; then
    echo "❌ JAVA_HOME 未设置"
else
    echo "✅ JAVA_HOME = $JAVA_HOME"
    echo "   实际版本: $($JAVA_HOME/bin/java -version 2>&1 | head -1)"
fi
echo ""

# 检查 Maven 使用的 Java
echo "4. Maven 使用的 Java 版本:"
mvn -version 2>&1 | grep "Java version"
echo ""

# 检查 Flink 使用的 Java
echo "5. Flink 使用的 Java 版本:"
if command -v flink &> /dev/null; then
    FLINK_JAVA=$(flink --version 2>&1 | grep -i java || echo "无法检测")
    echo "$FLINK_JAVA"
else
    echo "❌ Flink 命令未找到"
fi
echo ""

# 检查已安装的 Java 版本
echo "6. 系统中已安装的 Java 版本:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    /usr/libexec/java_home -V 2>&1 | grep -E "Java|jdk"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    if command -v update-alternatives &> /dev/null; then
        update-alternatives --list java 2>&1
    else
        ls -la /usr/lib/jvm/ 2>&1 | grep java
    fi
fi
echo ""

# 版本判断
echo "=========================================="
echo "诊断结果:"
echo "=========================================="

if [ "$JAVA_VERSION" -ge 17 ]; then
    echo "✅ Java 运行时版本满足要求 (>= 17)"
else
    echo "❌ Java 运行时版本不满足要求"
    echo "   当前版本: $JAVA_VERSION"
    echo "   需要版本: >= 17"
    echo ""
    echo "解决方案:"
    echo "1. 安装 Java 17 (见下方命令)"
    echo "2. 设置 JAVA_HOME 指向 Java 17"
    echo "3. 更新 PATH 环境变量"
fi
echo ""

# 提供安装建议
echo "=========================================="
echo "Java 17 安装命令:"
echo "=========================================="
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS (Homebrew):"
    echo "  brew install openjdk@17"
    echo "  sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \\"
    echo "    /Library/Java/JavaVirtualMachines/openjdk-17.jdk"
    echo "  export JAVA_HOME=/opt/homebrew/opt/openjdk@17"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        case $ID in
            ubuntu|debian)
                echo "Ubuntu/Debian:"
                echo "  sudo apt update"
                echo "  sudo apt install openjdk-17-jdk"
                echo "  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
                ;;
            amzn)
                echo "Amazon Linux 2:"
                echo "  sudo amazon-linux-extras install java-openjdk17"
                echo "  export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto"
                ;;
            centos|rhel)
                echo "CentOS/RHEL:"
                echo "  sudo yum install java-17-openjdk-devel"
                echo "  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk"
                ;;
        esac
    fi
fi
echo ""

echo "=========================================="
echo "设置环境变量 (添加到 ~/.bashrc 或 ~/.zshrc):"
echo "=========================================="
echo "export JAVA_HOME=/path/to/java17"
echo "export PATH=\$JAVA_HOME/bin:\$PATH"
echo ""
