#!/bin/bash
# 下载 Kafka 连接 JAR 到 Spark 容器（绕过 ivy 权限问题）
set -e

JARS_DIR=/opt/spark/jars

echo "下载 Kafka JARs 到 spark-master..."
docker exec -u root spark-master bash -c "
cd $JARS_DIR
wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar
wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar
wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar
wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
chmod 644 spark-sql-kafka*.jar kafka-clients*.jar commons-pool2*.jar spark-token-provider-kafka*.jar
echo 'spark-master OK:'
ls -la spark-sql-kafka*.jar kafka-clients*.jar
"

echo ""
echo "复制 JAR 到 spark-worker..."
docker exec -u root spark-master bash -c "cd $JARS_DIR && tar cf - spark-sql-kafka-0-10*.jar kafka-clients-3.3.2.jar spark-token-provider-kafka*.jar commons-pool2-2.11.1.jar" | docker exec -i -u root spark-worker bash -c "cd $JARS_DIR && tar xf - && echo 'spark-worker OK:' && ls -la spark-sql-kafka*.jar"
echo ""
echo "完成!"
