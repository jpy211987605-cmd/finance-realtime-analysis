#!/bin/bash

echo "=== 启动大数据集群 ==="

cd "$(dirname "$0")/../docker"

echo "1. 启动所有服务..."
docker compose up -d

echo "2. 等待服务启动..."
sleep 60

echo "3. 检查服务状态..."
docker compose ps

echo "4. 验证关键服务..."

echo "检查HDFS..."
if curl -s http://localhost:9870 > /dev/null; then
    echo "✓ HDFS 正常"
else
    echo "✗ HDFS 异常"
fi

echo "检查Kafka..."
if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "✓ Kafka 正常"
else
    echo "✗ Kafka 异常"
fi

echo "检查Elasticsearch..."
if curl -s http://localhost:9200/_cat/health | grep -q green; then
    echo "✓ Elasticsearch 正常"
else
    echo "✗ Elasticsearch 异常"
fi

echo "检查Spark..."
if curl -s http://localhost:8080 > /dev/null; then
    echo "✓ Spark 正常"
else
    echo "✗ Spark 异常"
fi

echo "=== 集群启动完成 ==="