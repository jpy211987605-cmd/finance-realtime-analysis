#!/bin/bash
# ============================================================================
# Kafka Topic 初始化脚本
# 创建金融实时数据所需的 5 个 Topic
#
# 使用方式:
#   bash scripts/init_kafka_topics.sh           # 创建/重建所有 Topic
#   bash scripts/init_kafka_topics.sh --list    # 仅列出已有 Topic
# ============================================================================

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

# Topic 配置
TOPICS=("copper_tick" "gas_tick" "gold_tick" "oil_tick" "silver_tick")
PARTITIONS=3
REPLICATION=1

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "========================================"
echo "  Kafka Topic 初始化"
echo "========================================"
echo ""

# ---- 检查 Kafka 容器 ----
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo -e "${RED}[ERROR]${NC} Kafka 容器未运行！"
    echo "  请先启动: cd docker && docker compose up -d"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Kafka 容器运行中"
echo ""

# ---- 仅列出模式 ----
if [ "$1" = "--list" ]; then
    echo "当前 Topic 列表:"
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null
    exit 0
fi

# ---- 创建 Topic ----
CREATED=0
SKIPPED=0

for topic in "${TOPICS[@]}"; do
    echo -n "  Topic: $topic ... "

    # 检查是否已存在
    if docker exec "$KAFKA_CONTAINER" \
        kafka-topics --describe --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
        echo -e "${YELLOW}已存在，跳过${NC}"
        ((SKIPPED++))
        continue
    fi

    # 创建
    if docker exec "$KAFKA_CONTAINER" \
        kafka-topics --create --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" >/dev/null 2>&1; then
        echo -e "${GREEN}创建成功${NC}"
        ((CREATED++))
    else
        echo -e "${RED}创建失败${NC}"
    fi
done

echo ""
echo "----------------------------------------"
echo "  新建: $CREATED  已存在: $SKIPPED"
echo "----------------------------------------"
echo ""
echo "当前 Topic 列表:"
docker exec "$KAFKA_CONTAINER" \
    kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null

echo ""
echo "========================================"
echo "  Kafka Topic 初始化完成"
echo "========================================"
