#!/bin/bash

echo "=== 端口诊断脚本 ==="
echo ""

# 检查Docker服务状态
echo "[1] 检查Docker服务状态"
systemctl is-active docker
echo ""

# 检查容器运行状态
echo "[2] 检查容器运行状态"
docker compose ps
echo ""

# 检查容器内端口监听
echo "[3] 检查容器内端口监听"
echo "--- Hadoop NameNode (9870) ---"
docker exec -it hadoop-namenode bash -c "ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null || cat /proc/net/tcp" | grep -E "9870|:98|LISTEN"
echo ""

echo "--- Spark Master (8080) ---"
docker exec -it spark-master bash -c "ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null" | grep -E "8080|:80|LISTEN"
echo ""

echo "--- Kibana (5601) ---"
docker exec -it kibana bash -c "ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null" | grep -E "5601|:56|LISTEN"
echo ""

# 检查宿主机端口监听
echo "[4] 检查宿主机端口监听"
ss -tlnp | grep -E "9870|8080|5601"
echo ""

# 测试本地连接
echo "[5] 测试本地连接"
echo "--- 测试 9870 (HDFS) ---"
curl -s http://localhost:9870 > /dev/null 2>&1 && echo "✓ 9870 端口可访问" || echo "✗ 9870 端口不可访问"
echo ""

echo "--- 测试 8080 (Spark) ---"
curl -s http://localhost:8080 > /dev/null 2>&1 && echo "✓ 8080 端口可访问" || echo "✗ 8080 端口不可访问"
echo ""

echo "--- 测试 5601 (Kibana) ---"
curl -s http://localhost:5601 > /dev/null 2>&1 && echo "✓ 5601 端口可访问" || echo "✗ 5601 端口不可访问"
echo ""

# 检查Docker网络
echo "[6] 检查Docker网络"
docker network inspect bigdata
echo ""

echo "=== 诊断完成 ==="
