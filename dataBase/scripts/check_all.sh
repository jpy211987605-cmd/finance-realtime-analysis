#!/bin/bash
# ============================================================================
# 全组件状态验证脚本
# 用法: bash scripts/check_all.sh
# ============================================================================

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; FAIL=0

check() { echo -n "  $1 ... "; shift; if "$@" >/dev/null 2>&1; then echo -e "${GREEN}OK${NC}"; ((PASS++)); else echo -e "${RED}FAIL${NC}"; ((FAIL++)); fi; }

echo "============================================"
echo "  金融实时分析系统 - 全组件验证"
echo "============================================"
echo ""

# 1. Docker
echo "[1] Docker 容器"
for c in zookeeper kafka hadoop-namenode hadoop-datanode hive-metastore-postgresql hive-metastore hive-server elasticsearch logstash kibana spark-master spark-worker; do
  if docker ps --format '{{.Names}}' | grep -q "^$c$"; then echo -e "  ${GREEN}UP${NC}   $c"; ((PASS++)); else echo -e "  ${RED}DOWN${NC} $c"; ((FAIL++)); fi
done
echo ""

# 2. Kafka
echo "[2] Kafka"
check "Topic列表" docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
echo -n "  消息数据 ... "; docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gold_tick --max-messages 1 --timeout-ms 5000 2>/dev/null | grep -q "price" && echo -e "${GREEN}OK${NC}" && ((PASS++)) || { echo -e "${YELLOW}暂无${NC}"; }
echo ""

# 3. HDFS
echo "[3] HDFS"
check "NameNode Web" curl -s http://localhost:9870
echo -n "  raw目录 ... "; docker exec hadoop-namenode hdfs dfs -ls /user/finance/raw/ 2>/dev/null | grep -q "gold" && echo -e "${GREEN}OK${NC}" && ((PASS++)) || { echo -e "${YELLOW}暂无数据${NC}"; }
echo ""

# 4. Hive
echo "[4] Hive"
echo -n "  HiveServer2 ... "; docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SELECT 1" 2>/dev/null | grep -q "1" && echo -e "${GREEN}OK${NC}" && ((PASS++)) || { echo -e "${RED}FAIL${NC}"; ((FAIL++)); }
echo -n "  ODS表 ... "; docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE ods_finance; SHOW TABLES;" 2>/dev/null | grep -q "gold_tick" && echo -e "${GREEN}5张表${NC}" && ((PASS++)) || { echo -e "${RED}FAIL${NC}"; ((FAIL++)); }
echo ""

# 5. ES
echo "[5] Elasticsearch"
ES_COUNT=$(curl -s http://localhost:9200/finance_realtime_metrics/_count | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0")
if [ "$ES_COUNT" -gt 0 ]; then echo -e "  ${GREEN}OK${NC}   finance_realtime_metrics: ${ES_COUNT} 条"; ((PASS++)); else echo -e "  ${YELLOW}暂无${NC}"; fi
echo ""

# 6. API
echo "[6] FastAPI"
API_STATUS=$(curl -s http://localhost:8000/api/realtime/latest | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "down")
if [ "$API_STATUS" = "ok" ]; then
  echo -e "  ${GREEN}OK${NC}   API 在线 (非mock)"
  curl -s http://localhost:8000/api/realtime/latest | python3 -c "
import sys,json
d=json.load(sys.stdin)
for x in d['data']:
  print(f\"    {x['symbol']:6s}: {x['price']:.2f}\")" 2>/dev/null
  ((PASS++))
elif [ "$API_STATUS" = "mock" ]; then
  echo -e "  ${YELLOW}MOCK${NC}  API 回退模拟数据"
else
  echo -e "  ${RED}DOWN${NC}  API 不可达"
  ((FAIL++))
fi
echo ""

# 7. Spark
echo "[7] Spark"
APPS=$(curl -s http://localhost:8080/json/ | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")
if [ "$APPS" -gt 0 ]; then
  echo -e "  ${GREEN}OK${NC}   活跃应用: ${APPS} 个"; ((PASS++))
  echo -n "  HDFS写入 ... "; grep -q "HDFS" ~/dataBase_03/dataBase/logs/spark_streaming.log 2>/dev/null && echo -e "${GREEN}OK${NC}" && ((PASS++)) || echo -e "${YELLOW}待验证${NC}"
else
  echo -e "  ${YELLOW}NONE${NC}  无活跃应用"
  tail -3 ~/dataBase_03/dataBase/logs/spark_streaming.log 2>/dev/null | grep -q "Exception" && echo -e "  ${RED}Spark 启动失败，查看日志${NC}" || echo -e "  ${YELLOW}Spark 未启动${NC}"
fi
echo ""

# 汇总
echo "============================================"
echo -e "  通过: ${GREEN}${PASS}${NC}  失败: ${RED}${FAIL}${NC}"
echo "============================================"
