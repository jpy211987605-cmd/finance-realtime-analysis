#!/bin/bash
# 一键修复 ES mapping + Spark JAR + 重启 Spark
set -e

echo "============================================"
echo "  修复 ES + Spark 一键脚本"
echo "============================================"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS="$PROJECT_ROOT/logs"

# ---- 1. 修复 ES ----
echo ""
echo "[1/3] 重建 ES 索引..."
docker stop logstash 2>/dev/null || true
sleep 1
curl -s -X DELETE http://localhost:9200/finance_realtime_metrics >/dev/null 2>&1 || true
sleep 1
curl -s -X PUT http://localhost:9200/finance_realtime_metrics \
  -H 'Content-Type: application/json' \
  -d @"$PROJECT_ROOT/config/es_mapping.json" >/dev/null 2>&1
docker start logstash 2>/dev/null || true
echo "  ES 索引已重建"

# ---- 2. 修复 Spark JAR ----
echo ""
echo "[2/3] 下载 Kafka JAR 到 Spark Worker..."

docker exec -u root spark-worker bash -c "
cd /opt/spark/jars
wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar 2>/dev/null || true
wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar 2>/dev/null || true
wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar 2>/dev/null || true
echo 'Worker JARs:'
ls spark-sql-kafka*.jar kafka-clients*.jar commons-pool2*.jar spark-token-provider*.jar 2>/dev/null
"

echo "  Spark Worker JAR 就绪"

# ---- 3. 重启 Spark ----
echo ""
echo "[3/3] 重启 Spark Streaming..."

# 杀掉旧任务
pkill -f "finance_spark_streaming" 2>/dev/null || true
docker exec spark-master rm -rf /tmp/spark_cp_finance /tmp/spark_checkpoint_finance 2>/dev/null || true

mkdir -p "$LOGS"
:> "$LOGS/spark_streaming.log"

nohup docker exec spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --name "FinanceStreaming" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.cores.max=2" \
    --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
    /opt/spark/work-dir/src/python/finance_spark_streaming.py \
    --kafka-brokers kafka:9093 \
    --trigger "10 seconds" \
    > "$LOGS/spark_streaming.log" 2>&1 &

echo "  Spark 已提交"

sleep 8

echo ""
echo "============================================"
echo "  修复完成，等待 30 秒数据流入后验证:"
echo "    bash scripts/check_all.sh"
echo "============================================"
