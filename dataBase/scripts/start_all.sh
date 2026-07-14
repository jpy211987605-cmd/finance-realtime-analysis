#!/bin/bash
# ============================================================================
# 一键启动脚本 — 大数据集群 + 数据管道 + AI 服务
# 用法: bash scripts/start_all.sh
# ============================================================================

set -e
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================"
echo "  金融大数据平台 — 一键启动"
echo "  项目目录: $PROJECT_DIR"
echo "============================================"

# ---- 1. 安装依赖 ----
echo ""
echo "[1/5] 安装 Python 依赖..."
pip install httpx>=0.24.0 -q 2>/dev/null
echo "  [OK] httpx 就绪"

# ---- 2. Docker 集群 ----
echo ""
echo "[2/5] 启动 Docker 集群..."
cd "$PROJECT_DIR/docker"

RUNNING=$(sudo docker compose ps 2>/dev/null | grep -c "Up" || echo 0)
echo "  当前运行容器: $RUNNING/12"

if [ "$RUNNING" -lt 8 ]; then
    echo "  启动中..."
    sudo docker compose up -d
    echo "  等待集群就绪 (90秒)..."
    sleep 90
    echo "  容器状态:"
    sudo docker compose ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null
else
    echo "  [OK] 集群已在运行"
fi

cd "$PROJECT_DIR"

# ---- 3. 初始化 ----
echo ""
echo "[3/5] 初始化业务组件..."

TOPICS=$(sudo docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | wc -l)
if [ "$TOPICS" -lt 5 ]; then
    echo "  初始化 Kafka/HDFS/Hive/ES..."
    bash scripts/init_all.sh
else
    echo "  [OK] Kafka Topics: $TOPICS (已初始化)"
fi

# ---- 4. 数据管道 ----
echo ""
echo "[4/5] 启动数据管道..."

# 停止旧进程
pkill -f finance_kafka_producer 2>/dev/null || true
sleep 1

# Producer
nohup python src/python/finance_kafka_producer.py --rate 5 --loop > /tmp/producer.log 2>&1 &
echo "  Producer: PID $!"

# Spark Streaming (非阻塞提交)
sudo docker exec spark-master bash -c '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name FinanceStreaming \
    --total-executor-cores 2 \
    --executor-memory 512m \
    /home/spark/finance_spark_streaming.py \
    > /tmp/spark_submit.log 2>&1 &
' 2>/dev/null
echo "  Spark Streaming: 已提交"

# ---- 5. 前端大屏 ----
echo ""
echo "[5/6] 启动前端大屏 (port 3000)..."

pkill -f "http.server 3000" 2>/dev/null || true
sleep 1

cd "$PROJECT_DIR/output"
nohup python3 -m http.server 3000 > /tmp/dashboard_server.log 2>&1 &
echo "  大屏: PID $!"

cd "$PROJECT_DIR"

# ---- 6. API 服务 ----
echo ""
echo "[5/5] 启动 API 服务 (含 AI 对话)..."

pkill -f finance_api_server 2>/dev/null || true
sleep 1

nohup python src/python/finance_api_server.py > /tmp/api_server.log 2>&1 &
API_PID=$!
echo "  FastAPI: PID $API_PID"

sleep 4

# ---- 验证 ----
echo ""
echo "============================================"
echo "  验证服务"
echo "============================================"

if curl -s http://localhost:8000/health | grep -q "healthy\|status"; then
    echo "  [OK] FastAPI :8000"
else
    echo "  [FAIL] FastAPI — tail /tmp/api_server.log"
fi

curl -s http://localhost:8000/api/config/apikey | grep -q "configured" && \
    echo "  [OK] AI 聊天端点" || echo "  [INFO] AI 端点就绪，需配置 API Key"

curl -s http://localhost:3000/ | grep -q "finance_dashboard" 2>/dev/null && \
    echo "  [OK] 大屏 :3000" || echo "  [INFO] 大屏 HTTP 服务运行中"

echo ""
echo "============================================"
echo "  全部启动完成！"
echo ""
echo "  Windows 浏览器访问:"
echo "    http://192.168.128.130:3000/finance_dashboard.html"
echo ""
echo "  API:     http://192.168.128.130:8000/docs"
echo "  HDFS:    http://192.168.128.130:9870"
echo "  Spark:   http://192.168.128.130:8080"
echo "============================================"
