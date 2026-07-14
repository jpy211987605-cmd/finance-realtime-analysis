#!/bin/bash
# ============================================================================
# 金融实时大数据分析系统 - 停止数据管道
#
# 停止顺序 (优雅退出):
#   1) Kafka Producer (先停写入)
#   2) Spark Streaming (等待消费完缓冲区)
#   3) FastAPI (最后停 API)
#
# 使用方式:
#   bash scripts/stop_pipeline.sh                  # 停止所有业务进程
#   bash scripts/stop_pipeline.sh --force          # 强制停止
#   bash scripts/stop_pipeline.sh --all            # 停止管道 + Docker 集群
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

FORCE=false
STOP_DOCKER=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --force) FORCE=true; shift ;;
        --all)   STOP_DOCKER=true; shift ;;
        *)       echo "用法: $0 [--force] [--all]"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  停止数据管道${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

STOPPED=0

# ---- 1. 停止 Kafka Producer ----
echo -e "${YELLOW}[1/3] 停止 Kafka Producer...${NC}"
PRODUCER_PIDS=$(pgrep -f "finance_kafka_producer" 2>/dev/null || true)

if [ -n "$PRODUCER_PIDS" ]; then
    for pid in $PRODUCER_PIDS; do
        echo -n "  停止 PID $pid ... "
        if [ "$FORCE" = true ]; then
            kill -9 "$pid" 2>/dev/null && echo -e "${GREEN}强制终止${NC}" || echo -e "${YELLOW}已退出${NC}"
        else
            kill "$pid" 2>/dev/null && echo -e "${GREEN}已发送 SIGTERM${NC}" || echo -e "${YELLOW}已退出${NC}"
        fi
        ((STOPPED++))
    done
else
    echo "  未运行"
fi

# 等待优雅退出
if [ "$FORCE" != true ]; then
    sleep 1
fi

# ---- 2. 停止 Spark Streaming ----
echo ""
echo -e "${YELLOW}[2/3] 停止 Spark Streaming...${NC}"

# 查找并停止 Spark Driver
SPARK_DRIVER=$(docker ps --format '{{.ID}} {{.Names}}' | grep "spark" || true)

# 通过 Spark Master API 停止应用
SPARK_APPS=$(curl -s http://localhost:8080/json/ 2>/dev/null | \
    python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    apps = d.get('activeapps', [])
    for a in apps:
        print(a.get('id', ''))
except: pass
" 2>/dev/null)

if [ -n "$SPARK_APPS" ]; then
    for app_id in $SPARK_APPS; do
        echo -n "  停止 Spark 应用 $app_id ... "
        curl -s -X POST "http://localhost:8080/app/kill/?id=$app_id" >/dev/null 2>&1 && \
            echo -e "${GREEN}[OK]${NC}" || \
            echo -e "${YELLOW}请手动停止${NC}"
        ((STOPPED++))
    done
else
    echo "  无活跃 Spark 应用"
fi

# 备选：直接杀容器内进程
if [ "$FORCE" = true ]; then
    echo "  强制清理 Spark Driver..."
    docker exec spark-master pkill -f "FinanceStreaming" 2>/dev/null || true
fi

# ---- 3. 停止 FastAPI ----
echo ""
echo -e "${YELLOW}[3/3] 停止 FastAPI Server...${NC}"
API_PIDS=$(pgrep -f "finance_api_server" 2>/dev/null || true)

if [ -n "$API_PIDS" ]; then
    for pid in $API_PIDS; do
        echo -n "  停止 PID $pid ... "
        if [ "$FORCE" = true ]; then
            kill -9 "$pid" 2>/dev/null && echo -e "${GREEN}强制终止${NC}" || echo -e "${YELLOW}已退出${NC}"
        else
            kill "$pid" 2>/dev/null && echo -e "${GREEN}已发送 SIGTERM${NC}" || echo -e "${YELLOW}已退出${NC}"
        fi
        ((STOPPED++))
    done
else
    echo "  未运行"
fi

# ---- 可选：停止 Docker 集群 ----
if [ "$STOP_DOCKER" = true ]; then
    echo ""
    echo -e "${YELLOW}停止 Docker 集群...${NC}"
    cd "$PROJECT_ROOT/docker"
    docker compose down
    cd "$PROJECT_ROOT"
    echo -e "${GREEN}[OK]${NC} Docker 集群已停止"
fi

# ---- 汇总 ----
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  管道已停止 (终止了 $STOPPED 个进程)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  ${CYAN}重新启动:${NC}"
echo -e "    bash scripts/start_pipeline.sh"
echo ""
echo -e "  ${CYAN}完全清理:${NC}"
echo -e "    bash scripts/stop_pipeline.sh --all"
echo ""
