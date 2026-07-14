#!/bin/bash
# ============================================================================
# AI 智能对话功能部署脚本
# 传输修改文件到 Ubuntu 并重启服务
# ============================================================================

set -e

UBUNTU_IP="192.168.128.130"
UBUNTU_USER="root"
PROJECT_DIR="/root/dataBase_03/dataBase"
LOCAL_DIR="D:\桌面\dataBase_03\dataBase"

echo "============================================"
echo "  金融大数据平台 - AI 功能部署"
echo "  目标: ${UBUNTU_USER}@${UBUNTU_IP}"
echo "============================================"

# Step 1: SCP 传输修改文件
echo ""
echo "[1/4] 传输修改文件到 Ubuntu..."
echo "------------------------------------------"

FILES_TO_UPLOAD=(
    "src/python/finance_api_server.py"
    "output/finance_dashboard.html"
    "requirements.txt"
)

for FILE in "${FILES_TO_UPLOAD[@]}"; do
    LOCAL_PATH="${LOCAL_DIR}/${FILE}"
    REMOTE_PATH="${PROJECT_DIR}/${FILE}"

    if [ -f "$LOCAL_PATH" ]; then
        echo "  上传: $FILE"
        scp -o StrictHostKeyChecking=no "$LOCAL_PATH" "${UBUNTU_USER}@${UBUNTU_IP}:${REMOTE_PATH}"
        echo "    ✓ 完成"
    else
        echo "    ✗ 文件不存在: $LOCAL_PATH"
    fi
done

# Step 2: SSH 到 Ubuntu 执行部署
echo ""
echo "[2/4] SSH 到 Ubuntu 安装依赖..."
echo "------------------------------------------"

ssh -o StrictHostKeyChecking=no ${UBUNTU_USER}@${UBUNTU_IP} bash << 'REMOTE_SCRIPT'
set -e

echo "  当前目录: $(pwd)"
echo "  主机名: $(hostname)"

# 切换到项目目录
cd /root/dataBase_03/dataBase
echo "  项目目录: $(pwd)"

# 安装 Python 依赖 (httpx for AI chat)
echo ""
echo "  [2.1] 安装 Python 依赖..."
pip install httpx>=0.24.0 2>&1 | tail -3
echo "    ✓ httpx 已安装"

# Step 3: 启动大数据集群
echo ""
echo "[3/4] 启动大数据集群..."
echo "------------------------------------------"

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "  启动 Docker 服务..."
    systemctl start docker
    sleep 3
fi
echo "  Docker 状态: $(docker info --format '{{.ServerVersion}}' 2>/dev/null || echo '未运行')"

# 启动 Docker Compose 集群
cd /root/dataBase_03/dataBase/docker

echo "  检查容器状态..."
RUNNING_COUNT=$(docker compose ps --format json 2>/dev/null | grep -c '"State":"running"' 2>/dev/null || echo 0)
echo "  当前运行容器: $RUNNING_COUNT/12"

if [ "$RUNNING_COUNT" -lt "8" ]; then
    echo "  启动 Docker Compose 集群 (12 容器)..."
    docker compose up -d 2>&1 | tail -5
    echo ""
    echo "  等待集群就绪 (约 90 秒)..."
    sleep 90
    echo "  容器状态:"
    docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null
else
    echo "  ✓ 集群已在运行"
fi

# 初始化 (Kafka Topic + HDFS + Hive + ES)
echo ""
echo "  [3.1] 检查初始化状态..."
cd /root/dataBase_03/dataBase

# 检查 Kafka Topics
KAFKA_TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | wc -l)
if [ "$KAFKA_TOPICS" -lt "5" ]; then
    echo "  初始化 Kafka/HDFS/Hive/ES..."
    bash scripts/init_all.sh 2>&1 | tail -10
else
    echo "  ✓ Kafka Topics: $KAFKA_TOPICS 个 (已初始化)"
fi

# Step 4: 启动数据管道 + API 服务
echo ""
echo "[4/4] 启动数据管道和 AI 服务..."
echo "------------------------------------------"

# 停止旧的 API 服务
echo "  停止旧服务..."
pkill -f "uvicorn.*finance_api_server" 2>/dev/null || true
pkill -f "finance_api_server" 2>/dev/null || true
pkill -f "finance_kafka_producer" 2>/dev/null || true
sleep 2

# 启动 Kafka Producer (后台)
echo "  启动 Kafka Producer..."
cd /root/dataBase_03/dataBase
nohup python src/python/finance_kafka_producer.py --rate 5 --loop > /tmp/producer.log 2>&1 &
echo "    Producer PID: $!"

# 启动 Spark Streaming
echo "  提交 Spark Streaming 作业..."
docker exec spark-master bash -c '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name FinanceStreaming \
    --total-executor-cores 2 \
    --executor-memory 512m \
    /home/spark/finance_spark_streaming.py
' > /tmp/spark_streaming.log 2>&1 &
echo "    Spark PID: $!"

# 启动 FastAPI (含 AI 对话)
echo "  启动 FastAPI (含 AI 对话)..."
cd /root/dataBase_03/dataBase
nohup python src/python/finance_api_server.py > /tmp/api_server.log 2>&1 &
API_PID=$!
echo "    API PID: $API_PID"

sleep 3

# 验证服务
echo ""
echo "============================================"
echo "  验证服务状态"
echo "============================================"

# 检查 API 是否启动
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "  ✓ FastAPI :8000 — 运行中"
    HEALTH=$(curl -s http://localhost:8000/health)
    echo "    Health: $HEALTH"
else
    echo "  ✗ FastAPI :8000 — 启动失败，检查日志:"
    echo "    tail -30 /tmp/api_server.log"
fi

# 检查 AI 端点
sleep 1
if curl -s http://localhost:8000/api/config/apikey > /dev/null 2>&1; then
    echo "  ✓ AI 聊天端点 — 就绪"
    echo "    /api/chat"
    echo "    /api/config/apikey"
else
    echo "  ✗ AI 端点 — 未就绪"
fi

# 检查其他组件
echo ""
echo "  容器状态:"
docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null

echo ""
echo "============================================"
echo "  ✓ 部署完成！"
echo ""
echo "  访问地址:"
echo "    API 文档:     http://192.168.128.130:8000/docs"
echo "    可视化大屏:   打开 output/finance_dashboard.html"
echo "    HDFS UI:      http://192.168.128.130:9870"
echo "    Spark UI:     http://192.168.128.130:8080"
echo "    ES:           http://192.168.128.130:9200"
echo "    Kibana:       http://192.168.128.130:5601"
echo ""
echo "  AI 对话: 打开大屏 → 点击右下角按钮 → 设置 Key"
echo "============================================"

REMOTE_SCRIPT

echo ""
echo "全部完成！"
