#!/bin/bash
# ============================================================================
# 金融实时大数据分析系统 - 一键启动脚本
#
# 功能：
#   按顺序启动所有组件，确保数据链路完整
#
# 启动流程：
#   1) Docker Compose 启动集群
#   2) 等待服务就绪
#   3) 初始化 Kafka Topic
#   4) 初始化 HDFS 目录
#   5) 初始化 Hive 表
#   6) 初始化 ES 索引
#   7) 启动 Spark Streaming 实时计算
#   8) 启动 Kafka Producer 数据生产
#   9) 启动 FastAPI 数据服务
#  10) 提供前端访问地址
#
# 使用方式：
#   bash scripts/start_finance_system.sh
#   bash scripts/start_finance_system.sh --rate 10 --loop
#   bash scripts/start_finance_system.sh --skip-spark
# ============================================================================

set -e

# ---- 颜色定义 ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ---- 默认参数 ----
KAFKA_RATE=5
LOOP_MODE=""
SKIP_SPARK=false
SKIP_FRONTEND=false

# ---- 解析参数 ----
while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)
            KAFKA_RATE="$2"
            shift 2
            ;;
        --loop)
            LOOP_MODE="--loop"
            shift
            ;;
        --skip-spark)
            SKIP_SPARK=true
            shift
            ;;
        --skip-frontend)
            SKIP_FRONTEND=true
            shift
            ;;
        *)
            echo "未知参数: $1"
            echo "用法: $0 [--rate N] [--loop] [--skip-spark] [--skip-frontend]"
            exit 1
            ;;
    esac
done

# ---- 项目路径 ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"
PYTHON_DIR="$PROJECT_ROOT/src/python"

cd "$PROJECT_ROOT"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  🏦 金融实时大数据分析系统${NC}"
echo -e "${BLUE}  一键启动脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  项目目录:   ${CYAN}$PROJECT_ROOT${NC}"
echo -e "  数据速率:   ${CYAN}$KAFKA_RATE 条/秒${NC}"
echo -e "  循环模式:   ${CYAN}${LOOP_MODE:-否}${NC}"
echo -e "  Spark:      ${CYAN}$([ "$SKIP_SPARK" = true ] && echo '跳过' || echo '启动')${NC}"
echo ""

# ========================================================================
# Step 1: Docker Compose
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 1/8: 启动 Docker 大数据集群${NC}"
echo -e "${BLUE}========================================${NC}"

cd "$DOCKER_DIR"

# 检查 docker compose 命令
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif docker-compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo -e "${RED}❌ 未找到 docker compose！${NC}"
    exit 1
fi

echo "启动所有容器..."
$DOCKER_COMPOSE up -d

echo ""
echo -e "${YELLOW}等待服务就绪（约 90 秒）...${NC}"

# 等待关键服务
SERVICES=("hadoop-namenode:9870" "kafka:9092" "elasticsearch:9200" "spark-master:8080")
for svc in "${SERVICES[@]}"; do
    name="${svc%%:*}"
    port="${svc##*:}"
    echo -n "  等待 $name :$port ..."
    for i in $(seq 1 30); do
        if curl -s "http://localhost:$port" >/dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo -e " ${RED}✗${NC}"
            echo -e "${YELLOW}  ⚠ $name 可能未完全启动，继续...${NC}"
        fi
        sleep 3
    done
done

# 额外等待 Hive 就绪
echo -n "  等待 Hive Server :10000 ..."
for i in $(seq 1 40); do
    if docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT 1" >/dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        break
    fi
    if [ "$i" -eq 40 ]; then
        echo -e " ${YELLOW}⚠ (将继续)${NC}"
    fi
    sleep 5
done

echo ""
echo -e "${GREEN}✓ Docker 集群启动完成${NC}"

cd "$PROJECT_ROOT"

# ========================================================================
# Step 2: Kafka Topics
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 2/8: 初始化 Kafka Topic${NC}"
echo -e "${BLUE}========================================${NC}"
bash scripts/init_kafka_topics.sh

# ========================================================================
# Step 3: HDFS Dirs
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 3/8: 初始化 HDFS 目录${NC}"
echo -e "${BLUE}========================================${NC}"
bash scripts/init_hdfs_dirs.sh

# ========================================================================
# Step 4: Hive Tables
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 4/8: 初始化 Hive 数仓表${NC}"
echo -e "${BLUE}========================================${NC}"
bash scripts/init_hive_tables.sh

# ========================================================================
# Step 5: ES Index
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 5/8: 初始化 ES 索引${NC}"
echo -e "${BLUE}========================================${NC}"

# 删除旧索引（可重复运行）
echo "清理旧索引..."
curl -s -X DELETE "http://localhost:9200/finance_realtime_*" >/dev/null 2>&1 || true
sleep 1

# 创建 finance_realtime_metrics 索引
echo "创建索引: finance_realtime_metrics"
curl -s -X PUT "http://localhost:9200/finance_realtime_metrics" \
    -H "Content-Type: application/json" \
    -d '{
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s"
      },
      "mappings": {
        "properties": {
          "symbol":           {"type": "keyword"},
          "timestamp":        {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'"'T'"'HH:mm:ss"},
          "price":            {"type": "double"},
          "volume":           {"type": "double"},
          "bid":              {"type": "double"},
          "ask":              {"type": "double"},
          "spread":           {"type": "double"},
          "spread_pct":       {"type": "double"},
          "direction":        {"type": "keyword"},
          "volatility":       {"type": "double"},
          "price_change":     {"type": "double"},
          "price_change_pct": {"type": "double"},
          "sma_5":            {"type": "double"},
          "sma_10":           {"type": "double"},
          "sma_20":           {"type": "double"},
          "mid_price":        {"type": "double"},
          "event_time":       {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'"'T'"'HH:mm:ss"}
        }
      }
    }' >/dev/null 2>&1
echo -e "${GREEN}✓ finance_realtime_metrics 创建完成${NC}"

# 创建 finance_realtime_agg 索引
echo "创建索引: finance_realtime_agg"
curl -s -X PUT "http://localhost:9200/finance_realtime_agg" \
    -H "Content-Type: application/json" \
    -d '{
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "10s"
      },
      "mappings": {
        "properties": {
          "symbol":           {"type": "keyword"},
          "minute":           {"type": "keyword"},
          "dt":               {"type": "keyword"},
          "open_price":       {"type": "double"},
          "close_price":      {"type": "double"},
          "high_price":       {"type": "double"},
          "low_price":        {"type": "double"},
          "avg_price":        {"type": "double"},
          "total_volume":     {"type": "double"},
          "avg_spread":       {"type": "double"},
          "avg_spread_pct":   {"type": "double"},
          "avg_volatility":   {"type": "double"},
          "max_volatility":   {"type": "double"},
          "up_count":         {"type": "integer"},
          "down_count":       {"type": "integer"},
          "tick_count":       {"type": "integer"},
          "price_change":     {"type": "double"},
          "price_change_pct": {"type": "double"},
          "event_time":       {"type": "date"}
        }
      }
    }' >/dev/null 2>&1
echo -e "${GREEN}✓ finance_realtime_agg 创建完成${NC}"

# ========================================================================
# Step 6: Spark Streaming
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 6/8: 启动 Spark Streaming${NC}"
echo -e "${BLUE}========================================${NC}"

if [ "$SKIP_SPARK" = true ]; then
    echo -e "${YELLOW}⚠ 跳过 Spark Streaming (--skip-spark)${NC}"
else
    # 检查 Python 依赖
    echo "检查 Python 依赖..."
    if ! python3 -c "import elasticsearch" 2>/dev/null; then
        echo -e "${YELLOW}⚠ elasticsearch-py 未安装，Spark ES 写入可能需要此依赖${NC}"
        echo -e "${YELLOW}  安装: pip install elasticsearch${NC}"
    fi

    echo "提交 Spark Streaming 任务..."
    SPARK_LOG="$PROJECT_ROOT/logs/spark_streaming.log"
    mkdir -p "$PROJECT_ROOT/logs"

    nohup docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --name "FinanceStreaming" \
        --conf "spark.executor.memory=1g" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.cores.max=2" \
        --conf "spark.sql.shuffle.partitions=2" \
        --conf "spark.sql.streaming.checkpointLocation=/tmp/spark_checkpoint_finance" \
        --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
        /opt/spark/work-dir/src/python/finance_spark_streaming.py \
        --kafka-brokers kafka:9093 \
        --es-host elasticsearch \
        --es-port 9200 \
        --trigger-interval "10 seconds" \
        > "$SPARK_LOG" 2>&1 &

    SPARK_PID=$!
    echo -e "${GREEN}✓ Spark Streaming 已提交 (PID: $SPARK_PID)${NC}"
    echo "  日志: $SPARK_LOG"
    sleep 5
fi

# ========================================================================
# Step 7: Kafka Producer
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 7/8: 启动 Kafka 数据生产者${NC}"
echo -e "${BLUE}========================================${NC}"

PRODUCER_LOG="$PROJECT_ROOT/logs/kafka_producer.log"
echo "启动 Kafka Producer (速率: $KAFKA_RATE 条/秒)..."

nohup python3 "$PYTHON_DIR/finance_kafka_producer.py" \
    --rate "$KAFKA_RATE" \
    $LOOP_MODE \
    --broker localhost:9092 \
    > "$PRODUCER_LOG" 2>&1 &

PRODUCER_PID=$!
echo -e "${GREEN}✓ Kafka Producer 已启动 (PID: $PRODUCER_PID)${NC}"
echo "  日志: $PRODUCER_LOG"

# ========================================================================
# Step 8: FastAPI Server
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 8/8: 启动 FastAPI 数据服务${NC}"
echo -e "${BLUE}========================================${NC}"

API_LOG="$PROJECT_ROOT/logs/fastapi.log"

# 杀掉已有 FastAPI 进程
pkill -f "finance_api_server" 2>/dev/null || true
sleep 1

nohup python3 "$PYTHON_DIR/finance_api_server.py" \
    > "$API_LOG" 2>&1 &

API_PID=$!
echo -e "${GREEN}✓ FastAPI 已启动 (PID: $API_PID)${NC}"
echo "  日志: $API_LOG"
sleep 3

# ========================================================================
# 汇总
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  ✅ 金融实时大数据分析系统启动完成！${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}  📡 服务访问地址:${NC}"
echo ""
echo -e "  ${CYAN}大数据集群:${NC}"
echo -e "    HDFS Web UI:        http://localhost:9870"
echo -e "    Spark Master:       http://localhost:8080"
echo -e "    Kibana:             http://localhost:5601"
echo -e "    Elasticsearch:      http://localhost:9200"
echo ""
echo -e "  ${CYAN}应用服务:${NC}"
echo -e "    FastAPI 服务:        http://localhost:8000"
echo -e "    API 文档:            http://localhost:8000/docs"
echo -e "    行情快照 API:        http://localhost:8000/api/realtime/latest"
echo -e "    趋势数据 API:        http://localhost:8000/api/realtime/trend?symbol=GOLD"
echo ""
echo -e "  ${CYAN}监控命令:${NC}"
echo -e "    Kafka 消息:          docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gold_tick --max-messages 5"
echo -e "    HDFS 文件:           docker exec hadoop-namenode hdfs dfs -ls /user/finance/"
echo -e "    ES 数据:             curl http://localhost:9200/finance_realtime_metrics/_count"
echo -e "    Producer 日志:       tail -f $PRODUCER_LOG"
echo -e "    Spark 日志:          tail -f $SPARK_LOG"
echo ""
echo -e "  ${GREEN}前端大屏:${NC}"
echo -e "    在浏览器中打开:      file://$PROJECT_ROOT/output/finance_dashboard.html"
echo -e "    或启动开发服务器:    cd frontend && npm run dev"
echo ""
echo -e "${BLUE}========================================${NC}"
