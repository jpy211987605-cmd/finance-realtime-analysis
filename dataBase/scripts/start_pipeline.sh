#!/bin/bash
# ============================================================================
# 金融实时大数据分析系统 - 启动数据管道
#
# 按顺序启动:
#   1) Spark Streaming 实时计算 (提交到 Docker Spark 集群)
#   2) Kafka Producer 数据生产 (Python 后台进程)
#   3) FastAPI 数据服务 (Python 后台进程)
#
# 前置条件:
#   已执行 init_all.sh 完成初始化
#
# 使用方式:
#   bash scripts/start_pipeline.sh                    # 默认 5条/秒, 单轮
#   bash scripts/start_pipeline.sh --rate 10 --loop   # 10条/秒, 循环
#   bash scripts/start_pipeline.sh --no-spark         # 跳过 Spark
#   bash scripts/start_pipeline.sh --status           # 查看运行状态
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ---- 参数默认值 ----
KAFKA_RATE=5
LOOP_MODE=""
SKIP_SPARK=false
SKIP_API=false
STATUS_ONLY=false

# ---- 解析参数 ----
while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)
            KAFKA_RATE="$2"; shift 2 ;;
        --loop)
            LOOP_MODE="--loop"; shift ;;
        --no-spark)
            SKIP_SPARK=true; shift ;;
        --no-api)
            SKIP_API=true; shift ;;
        --status)
            STATUS_ONLY=true; shift ;;
        *)
            echo "用法: $0 [--rate N] [--loop] [--no-spark] [--no-api] [--status]"
            exit 1 ;;
    esac
done

# ---- 路径 ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOGS_DIR"

cd "$PROJECT_ROOT"

# ========================================================================
# Status 模式
# ========================================================================
if [ "$STATUS_ONLY" = true ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  数据管道运行状态${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    # Spark
    echo -n "  Spark Streaming:  "
    if curl -s http://localhost:8080/json/ 2>/dev/null | grep -q "FinanceStreaming"; then
        echo -e "${GREEN}运行中${NC}"
    else
        echo -e "${YELLOW}未运行${NC}"
    fi

    # Producer
    echo -n "  Kafka Producer:  "
    PRODUCER_PID=$(pgrep -f "finance_kafka_producer" 2>/dev/null || true)
    if [ -n "$PRODUCER_PID" ]; then
        echo -e "${GREEN}运行中 (PID: $PRODUCER_PID)${NC}"
    else
        echo -e "${YELLOW}未运行${NC}"
    fi

    # API
    echo -n "  FastAPI:         "
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
        echo -e "${GREEN}运行中 (http://localhost:8000)${NC}"
    else
        echo -e "${YELLOW}未运行${NC}"
    fi

    echo ""
    echo "  日志目录: $LOGS_DIR"
    exit 0
fi

# ========================================================================
# 启动
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  启动数据管道${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  Producer 速率:  ${CYAN}$KAFKA_RATE 条/秒${NC}"
echo -e "  循环模式:       ${CYAN}${LOOP_MODE:-否}${NC}"
echo -e "  Spark:          ${CYAN}$([ "$SKIP_SPARK" = true ] && echo '跳过' || echo '启动')${NC}"
echo -e "  FastAPI:        ${CYAN}$([ "$SKIP_API" = true ] && echo '跳过' || echo '启动')${NC}"
echo ""

# ---- 前置检查 ----
echo -e "${YELLOW}[前置检查]${NC}"
if ! docker ps --format '{{.Names}}' | grep -q "^kafka$"; then
    echo -e "${RED}[ERROR]${NC} Docker 集群未运行"
    echo "  请先: cd docker && docker compose up -d"
    exit 1
fi
echo -e "  ${GREEN}[OK]${NC} Docker 集群运行中"

# 检查 Python 依赖
check_python_dep() {
    python3 -c "import $1" 2>/dev/null && return 0 || return 1
}
echo -n "  kafka-python: "
check_python_dep "kafka" && echo -e "${GREEN}[OK]${NC}" || echo -e "${RED}[MISSING]${NC} (pip install kafka-python)"
echo -n "  elasticsearch: "
check_python_dep "elasticsearch" && echo -e "${GREEN}[OK]${NC}" || echo -e "${YELLOW}[WARN]${NC} (pip install elasticsearch)"
echo -n "  fastapi: "
check_python_dep "fastapi" && echo -e "${GREEN}[OK]${NC}" || echo -e "${RED}[MISSING]${NC} (pip install fastapi uvicorn)"

echo ""

# ========================================================================
# Step 1: Spark Streaming
# ========================================================================
if [ "$SKIP_SPARK" = false ]; then
    echo -e "${BLUE}----------------------------------------${NC}"
    echo -e "${BLUE}  [1/3] 启动 Spark Streaming${NC}"
    echo -e "${BLUE}----------------------------------------${NC}"

    SPARK_LOG="$LOGS_DIR/spark_streaming.log"

    # 清空旧日志
    :> "$SPARK_LOG"

    echo "提交 Spark 作业到 spark-master..."
    echo "  日志: $SPARK_LOG"

    nohup docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --name "FinanceStreaming" \
        --conf "spark.executor.memory=1g" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.cores.max=2" \
        --conf "spark.sql.shuffle.partitions=4" \
        --conf "spark.sql.streaming.checkpointLocation=file:///tmp/spark_checkpoint_finance" \
        --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
        /opt/spark/work-dir/src/python/finance_spark_streaming.py \
        --kafka-brokers kafka:9093 \
        --trigger "10 seconds" \
        > "$SPARK_LOG" 2>&1 &

    SPARK_BG_PID=$!
    echo -e "  后台 PID: $SPARK_BG_PID"
    echo "  等待 Spark 作业提交... (约 15 秒)"
    sleep 8

    # 检查是否提交成功
    if grep -q "所有流已启动" "$SPARK_LOG" 2>/dev/null; then
        echo -e "  ${GREEN}[OK]${NC} Spark Streaming 已启动"
    elif grep -q "Error\|Exception" "$SPARK_LOG" 2>/dev/null; then
        echo -e "  ${RED}[FAIL]${NC} Spark 启动异常，查看日志:"
        grep -E "Error|Exception|Caused by" "$SPARK_LOG" | tail -5
    else
        echo -e "  ${YELLOW}[WAIT]${NC} Spark 正在初始化... (查看: tail -f $SPARK_LOG)"
    fi
else
    echo -e "${YELLOW}----------------------------------------${NC}"
    echo -e "${YELLOW}  [1/3] Spark Streaming (跳过)${NC}"
    echo -e "${YELLOW}----------------------------------------${NC}"
fi

# ========================================================================
# Step 2: Kafka Producer
# ========================================================================
echo ""
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  [2/3] 启动 Kafka Producer${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

PRODUCER_LOG="$LOGS_DIR/kafka_producer.log"
:> "$PRODUCER_LOG"

# 先杀掉旧进程
OLD_PID=$(pgrep -f "finance_kafka_producer" 2>/dev/null || true)
if [ -n "$OLD_PID" ]; then
    echo "  停止旧 Producer (PID: $OLD_PID)..."
    kill "$OLD_PID" 2>/dev/null || true
    sleep 1
fi

echo "  启动 Producer (速率: $KAFKA_RATE 条/秒)..."
echo "  日志: $PRODUCER_LOG"

nohup python3 "$PROJECT_ROOT/src/python/finance_kafka_producer.py" \
    --rate "$KAFKA_RATE" \
    $LOOP_MODE \
    --broker localhost:9092 \
    > "$PRODUCER_LOG" 2>&1 &

PRODUCER_PID=$!
echo -e "  ${GREEN}[OK]${NC} Producer 已启动 (PID: $PRODUCER_PID)"

# 等待第一批数据
sleep 3
if grep -q "开始发送数据" "$PRODUCER_LOG" 2>/dev/null; then
    echo -e "  ${GREEN}[OK]${NC} 数据正在发送中..."
elif grep -q "Error\|NoBrokersAvailable" "$PRODUCER_LOG" 2>/dev/null; then
    echo -e "  ${RED}[FAIL]${NC} Producer 启动异常"
    tail -3 "$PRODUCER_LOG"
fi

# ========================================================================
# Step 3: FastAPI Server
# ========================================================================
echo ""
echo -e "${BLUE}----------------------------------------${NC}"
echo -e "${BLUE}  [3/3] 启动 FastAPI 数据服务${NC}"
echo -e "${BLUE}----------------------------------------${NC}"

if [ "$SKIP_API" = false ]; then
    API_LOG="$LOGS_DIR/fastapi.log"
    :> "$API_LOG"

    # 先杀掉旧进程
    OLD_API=$(pgrep -f "finance_api_server" 2>/dev/null || true)
    if [ -n "$OLD_API" ]; then
        echo "  停止旧 FastAPI (PID: $OLD_API)..."
        kill "$OLD_API" 2>/dev/null || true
        sleep 1
    fi

    echo "  启动 FastAPI (http://0.0.0.0:8000)..."
    echo "  日志: $API_LOG"

    nohup python3 "$PROJECT_ROOT/src/python/finance_api_server.py" \
        > "$API_LOG" 2>&1 &

    API_PID=$!
    echo -e "  ${GREEN}[OK]${NC} FastAPI 已启动 (PID: $API_PID)"

    # 等待就绪
    sleep 3
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
        echo -e "  ${GREEN}[OK]${NC} API 服务就绪: http://localhost:8000"
    else
        echo -e "  ${YELLOW}[WAIT]${NC} API 尚未就绪 (查看: tail -f $API_LOG)"
    fi
else
    echo -e "${YELLOW}  [3/3] FastAPI (跳过)${NC}"
fi

# ========================================================================
# 汇总
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  数据管道启动完成!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  ${CYAN}运行状态:${NC}"
echo -e "    Kafka Producer:   tail -f $LOGS_DIR/kafka_producer.log"
echo -e "    Spark Streaming:  tail -f $LOGS_DIR/spark_streaming.log"
echo -e "    FastAPI:          tail -f $LOGS_DIR/fastapi.log"
echo ""
echo -e "  ${CYAN}验证命令:${NC}"
echo -e "    Kafka 消息:       docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gold_tick --max-messages 3"
echo -e "    ES 数据量:        curl http://localhost:9200/finance_realtime_metrics/_count"
echo -e "    API 行情:         curl http://localhost:8000/api/realtime/latest"
echo ""
echo -e "  ${CYAN}Web 界面:${NC}"
echo -e "    API 文档:         ${GREEN}http://localhost:8000/docs${NC}"
echo -e "    前端大屏:         ${GREEN}浏览器打开 output/finance_dashboard.html${NC}"
echo -e "    Spark UI:         ${GREEN}http://localhost:8080${NC}"
echo ""
echo -e "  ${CYAN}停止管道:${NC}"
echo -e "    bash scripts/stop_pipeline.sh"
echo ""
echo -e "${BLUE}========================================${NC}"
