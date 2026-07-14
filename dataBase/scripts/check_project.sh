#!/bin/bash

echo "=== 项目完整性检查 ==="

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
echo "项目目录: $PROJECT_DIR"

errors=0

echo ""
echo "1. 检查目录结构..."

REQUIRED_DIRS=(
    "config/flume"
    "config/hive"
    "config/logstash"
    "docker"
    "docs"
    "output"
    "scripts"
    "src/python"
    "models"
    "data/logs"
)

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$PROJECT_DIR/$dir" ]; then
        echo "✓ $dir"
    else
        echo "✗ $dir - 不存在，创建中..."
        mkdir -p "$PROJECT_DIR/$dir"
        errors=$((errors + 1))
    fi
done

echo ""
echo "2. 检查必要文件..."

REQUIRED_FILES=(
    "docker/docker-compose.yml"
    "config/flume/user_behavior.conf"
    "config/hive/hive_ddl.sql"
    "config/logstash/kafka_to_es.conf"
    "src/python/generate_logs.py"
    "src/python/spark_analysis.py"
    "src/python/train_sklearn_model.py"
    "src/python/main.py"
    "scripts/etl_daily.sh"
    "scripts/start_cluster.sh"
    "scripts/stop_cluster.sh"
    "README.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$PROJECT_DIR/$file" ]; then
        echo "✓ $file"
    else
        echo "✗ $file - 不存在"
        errors=$((errors + 1))
    fi
done

echo ""
echo "3. 检查模型文件..."

MODEL_FILES=(
    "models/lr_model.pkl"
    "models/sales_model.pkl"
)

model_missing=0
for model in "${MODEL_FILES[@]}"; do
    if [ -f "$PROJECT_DIR/$model" ]; then
        echo "✓ $model"
    else
        echo "⚠ $model - 不存在 (将使用模拟模型)"
        model_missing=$((model_missing + 1))
    fi
done

if [ $model_missing -gt 0 ]; then
    echo ""
    echo "提示: FastAPI服务将使用内置的模拟模型"
    echo "如需使用真实模型，请运行:"
    echo "cd $PROJECT_DIR"
    echo "python3 src/python/train_sklearn_model.py"
fi

echo ""
echo "4. 检查脚本可执行权限..."

SCRIPTS=(
    "scripts/etl_daily.sh"
    "scripts/start_cluster.sh"
    "scripts/stop_cluster.sh"
    "scripts/check_project.sh"
)

for script in "${SCRIPTS[@]}"; do
    if [ -x "$PROJECT_DIR/$script" ]; then
        echo "✓ $script (可执行)"
    else
        echo "⚠ $script (添加可执行权限)"
        chmod +x "$PROJECT_DIR/$script"
    fi
done

echo ""
echo "=== 检查完成 ==="

if [ $errors -eq 0 ] && [ $model_missing -eq 0 ]; then
    echo "项目完整性检查通过！"
    exit 0
else
    echo "发现 $errors 个缺失文件和 $model_missing 个缺失模型"
    exit 1
fi