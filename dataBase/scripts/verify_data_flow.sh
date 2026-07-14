#!/bin/bash
# ============================================================================
# 金融实时数据链路 - 全链路验证脚本
# 逐层验证数据是否正常流动
#
# 使用方式:
#   bash scripts/verify_data_flow.sh
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

check() {
    local label="$1"
    shift
    echo -n "  [$label] ... "
    if "$@" >/dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
        ((PASS++))
    else
        echo -e "${RED}FAIL${NC}"
        ((FAIL++))
    fi
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  金融实时数据链路 - 全链路验证${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ========================================================================
# 1. Docker 容器
# ========================================================================
echo -e "${YELLOW}[1] Docker 容器状态${NC}"
CONTAINERS=("zookeeper" "kafka" "hadoop-namenode" "hadoop-datanode"
            "hive-metastore-postgresql" "hive-metastore" "hive-server"
            "elasticsearch" "kibana" "spark-master" "spark-worker")
for c in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
        echo -e "  ${GREEN}UP${NC}   $c"
        ((PASS++))
    else
        echo -e "  ${RED}DOWN${NC} $c"
        ((FAIL++))
    fi
done
echo ""

# ========================================================================
# 2. Kafka
# ========================================================================
echo -e "${YELLOW}[2] Kafka${NC}"
check "Topic列表" docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 检查消息（5秒超时）
echo -n "  [消息检查] gold_tick ... "
if docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic gold_tick \
    --max-messages 1 --timeout-ms 5000 2>/dev/null | grep -q "{"; then
    echo -e "${GREEN}有数据${NC}"
    ((PASS++))
else
    echo -e "${YELLOW}暂无数据 (Producer 运行中?)${NC}"
    ((WARN++))
fi
echo ""

# ========================================================================
# 3. HDFS
# ========================================================================
echo -e "${YELLOW}[3] HDFS${NC}"
HDFS="docker exec hadoop-namenode hdfs dfs"
check "根目录" $HDFS -ls /user/finance 2>/dev/null

# 检查各品种目录
for sym in gold oil silver copper gas; do
    if $HDFS -ls /user/finance/raw/$sym >/dev/null 2>&1; then
        FILE_COUNT=$($HDFS -ls -R /user/finance/raw/$sym 2>/dev/null | grep -c ".json" || echo 0)
        if [ "$FILE_COUNT" -gt 0 ]; then
            echo -e "  ${GREEN}OK${NC}   /user/finance/raw/$sym ($FILE_COUNT 个文件)"
            ((PASS++))
        else
            echo -e "  ${YELLOW}EMPTY${NC} /user/finance/raw/$sym (等待 Spark 写入)"
            ((WARN++))
        fi
    else
        echo -e "  ${RED}MISSING${NC} /user/finance/raw/$sym"
        ((FAIL++))
    fi
done
echo ""

# ========================================================================
# 4. Hive
# ========================================================================
echo -e "${YELLOW}[4] Hive 数仓${NC}"
for db in "ods_finance" "dwd_finance" "dws_finance" "ads_finance"; do
    TABLE_COUNT=$(docker exec hive-server hive -e "USE $db; SHOW TABLES;" 2>/dev/null | grep -c "_" || echo 0)
    if [ "$TABLE_COUNT" -gt 0 ]; then
        echo -e "  ${GREEN}OK${NC}   $db ($TABLE_COUNT 张表)"
        ((PASS++))
    else
        echo -e "  ${RED}FAIL${NC} $db (无表)"
        ((FAIL++))
    fi
done
echo ""

# ========================================================================
# 5. Spark Streaming
# ========================================================================
echo -e "${YELLOW}[5] Spark Streaming${NC}"
SPARK_APPS=$(curl -s http://localhost:8080/json/ 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    apps = d.get('activeapps', [])
    for a in apps:
        print(a.get('name','') + '|' + (a.get('id','')))
except: pass
" 2>/dev/null)

if [ -n "$SPARK_APPS" ]; then
    while IFS='|' read -r name app_id; do
        echo -e "  ${GREEN}ACTIVE${NC} $name (id=$app_id)"
        ((PASS++))
    done <<< "$SPARK_APPS"
else
    echo -e "  ${YELLOW}无活跃应用${NC} (Spark Streaming 未启动?)"
    ((WARN++))
fi
echo ""

# ========================================================================
# 6. Elasticsearch
# ========================================================================
echo -e "${YELLOW}[6] Elasticsearch${NC}"
ES_COUNT=$(curl -s "http://localhost:9200/finance_realtime_metrics/_count" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
ES_AGG=$(curl -s "http://localhost:9200/finance_realtime_agg/_count" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")

if [ "$ES_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}OK${NC}   finance_realtime_metrics: $ES_COUNT 条"
    ((PASS++))
else
    echo -e "  ${YELLOW}EMPTY${NC} finance_realtime_metrics (等待 Spark 写入)"
    ((WARN++))
fi

if [ "$ES_AGG" -gt 0 ]; then
    echo -e "  ${GREEN}OK${NC}   finance_realtime_agg: $ES_AGG 条"
    ((PASS++))
else
    echo -e "  ${YELLOW}EMPTY${NC} finance_realtime_agg (等待窗口聚合)"
    ((WARN++))
fi
echo ""

# ========================================================================
# 7. FastAPI
# ========================================================================
echo -e "${YELLOW}[7] FastAPI${NC}"
check "Health" curl -s http://localhost:8000/health
echo -n "  [行情API] ... "
LATEST=$(curl -s http://localhost:8000/api/realtime/latest 2>/dev/null)
if echo "$LATEST" | grep -q '"data"'; then
    COUNT=$(echo "$LATEST" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))" 2>/dev/null || echo "0")
    echo -e "${GREEN}OK${NC} ($COUNT 品种)"
    ((PASS++))
else
    echo -e "${RED}FAIL${NC}"
    ((FAIL++))
fi
echo ""

# ========================================================================
# 8. Logstash（辅助管道）
# ========================================================================
echo -e "${YELLOW}[8] Logstash (辅助)${NC}"
check "运行状态" docker ps --format '{{.Names}}' | grep -q logstash
echo ""

# ========================================================================
# 汇总
# ========================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "  验证结果: ${GREEN}$PASS 通过${NC}  ${YELLOW}$WARN 警告${NC}  ${RED}$FAIL 失败${NC}"
echo -e "${BLUE}========================================${NC}"

TOTAL=$((PASS + FAIL + WARN))
if [ "$FAIL" -eq 0 ] && [ "$WARN" -eq 0 ]; then
    echo -e "${GREEN}  全链路验证通过！数据正常流动。${NC}"
    exit 0
elif [ "$FAIL" -eq 0 ]; then
    echo -e "${YELLOW}  链路基本正常，部分组件数据尚未生成。${NC}"
    echo -e "${YELLOW}  提示: 等待 1-2 分钟后再次验证${NC}"
    exit 0
else
    echo -e "${RED}  存在失败项，请检查对应组件日志。${NC}"
    echo -e "  Docker 日志: cd docker && docker compose logs"
    exit 1
fi
