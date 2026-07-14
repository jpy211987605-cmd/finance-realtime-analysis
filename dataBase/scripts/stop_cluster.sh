#!/bin/bash

echo "=== 停止大数据集群 ==="

cd "$(dirname "$0")/../docker"

echo "1. 停止所有服务..."
docker compose down

echo "2. 清理无用容器..."
docker container prune -f

echo "3. 清理无用镜像..."
docker image prune -f

echo "=== 集群停止完成 ==="