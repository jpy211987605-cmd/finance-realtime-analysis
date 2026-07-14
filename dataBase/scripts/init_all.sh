#!/bin/bash
# ============================================================================
# 金融实时大数据分析系统 - 一键初始化脚本
#
# 功能：
#   1) 初始化 Kafka Topic (5个品种)
#   2) 初始化 HDFS 目录结构
#   3) 初始化 Hive 数仓表 (ODS→DWD→DWS→ADS)
#   4) 初始化 Elasticsearch 索引 (finance_realtime_metrics + finance_realtime_agg)
#
# 前置条件:
#   Docker 集群已启动: cd docker && docker compose up -d
#   等待约 90 秒让所有容器就绪
#
# 使用方式:
#   bash scripts/init_all.sh
#   bash scripts/init_all.sh --reset    # 删除旧资源后重建 (ES 索引会被清空)
#
# 可重复执行: 所有操作均为幂等 (CREATE IF NOT EXISTS / mkdir -p)
# ============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

RESET_MODE=false
if [ "$1" = "--reset" ]; then
    RESET_MODE=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  金融实时大数据分析系统${NC}"
echo -e "${BLUE}  业务组件初始化${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  项目目录:   ${CYAN}$PROJECT_ROOT${NC}"
echo -e "  模式:       ${CYAN}$([ "$RESET_MODE" = true ] && echo '重置 (删除旧数据)' || echo '初始化 (保留已有)')${NC}"
echo ""

# ---- 前置检查 ----
echo -e "${YELLOW}[前置检查]${NC} 验证 Docker 容器状态..."
REQUIRED=("kafka" "hadoop-namenode" "hive-server" "elasticsearch")
ALL_OK=true
for c in "${REQUIRED[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
        echo -e "  ${GREEN}[OK]${NC} $c"
    else
        echo -e "  ${RED}[MISSING]${NC} $c"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    echo ""
    echo -e "${RED}[ERROR]${NC} 部分容器未运行，请先启动 Docker 集群:"
    echo "  cd docker && docker compose up -d"
    exit 1
fi
echo ""

# ========================================================================
# Step 1: HDFS 目录 (必须先于 Hive)
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  [1/4] HDFS 目录初始化${NC}"
echo -e "${BLUE}========================================${NC}"
bash "$SCRIPT_DIR/init_hdfs_dirs.sh"
echo ""

# ========================================================================
# Step 2: Kafka Topic
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  [2/4] Kafka Topic 初始化${NC}"
echo -e "${BLUE}========================================${NC}"
bash "$SCRIPT_DIR/init_kafka_topics.sh"
echo ""

# ========================================================================
# Step 3: Hive 数仓表 (依赖 HDFS 已就绪)
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  [3/4] Hive 数仓建表${NC}"
echo -e "${BLUE}========================================${NC}"
bash "$SCRIPT_DIR/init_hive_tables.sh"
echo ""

# ========================================================================
# Step 4: Elasticsearch 索引
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  [4/4] Elasticsearch 索引初始化${NC}"
echo -e "${BLUE}========================================${NC}"

ES_URL="http://localhost:9200"

# 检查 ES 是否可达
echo -n "检查 ES 连接... "
if curl -s "$ES_URL" >/dev/null 2>&1; then
    echo -e "${GREEN}[OK]${NC}"
else
    echo -e "${RED}[FAIL]${NC}"
    echo -e "${YELLOW}[WARN]${NC} Elasticsearch 不可达，跳过索引初始化"
    echo "  后续可手动执行: bash $SCRIPT_DIR/init_all.sh"
fi

if curl -s "$ES_URL" >/dev/null 2>&1; then
    # 重置模式：删除旧索引
    if [ "$RESET_MODE" = true ]; then
        echo "重置模式: 删除旧索引..."
        curl -s -X DELETE "$ES_URL/finance_realtime_metrics" >/dev/null 2>&1 || true
        curl -s -X DELETE "$ES_URL/finance_realtime_agg" >/dev/null 2>&1 || true
        sleep 2
        echo -e "${GREEN}[OK]${NC} 旧索引已删除"
    fi

    # ---- 创建 finance_realtime_metrics ----
    echo -n "创建索引: finance_realtime_metrics ... "
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$ES_URL/finance_realtime_metrics" \
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
              "timestamp":        {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'\''T'\''HH:mm:ss"},
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
              "event_time":       {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'\''T'\''HH:mm:ss"}
            }
          }
        }')
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}创建成功${NC}"
    else
        echo -e "${YELLOW}已存在 (HTTP $HTTP_CODE)${NC}"
    fi

    # ---- 创建 finance_realtime_agg ----
    echo -n "创建索引: finance_realtime_agg ... "
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$ES_URL/finance_realtime_agg" \
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
              "flat_count":       {"type": "integer"},
              "tick_count":       {"type": "integer"},
              "price_change":     {"type": "double"},
              "price_change_pct": {"type": "double"},
              "event_time":       {"type": "date"}
            }
          }
        }')
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}创建成功${NC}"
    else
        echo -e "${YELLOW}已存在 (HTTP $HTTP_CODE)${NC}"
    fi

    echo ""
    echo "当前 ES 索引:"
    curl -s "$ES_URL/_cat/indices?v" 2>/dev/null | grep -E "index|finance"
fi

# ========================================================================
# 汇总
# ========================================================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  初始化完成!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "  ${CYAN}已初始化的组件:${NC}"
echo -e "    [OK] Kafka:      5 个 Topic (copper/gas/gold/oil/silver_tick)"
echo -e "    [OK] HDFS:       /user/finance/raw/{5品种} + /agg/{1min,5min}"
echo -e "    [OK] Hive:       4 层数仓 (ods/dwd/dws/ads_finance)"
echo -e "    [OK] ES:         2 个索引 (finance_realtime_metrics/agg)"
echo ""
echo -e "  ${CYAN}下一步:${NC}"
echo -e "    启动数据管道:   ${GREEN}bash scripts/start_pipeline.sh${NC}"
echo ""
echo -e "${BLUE}========================================${NC}"
