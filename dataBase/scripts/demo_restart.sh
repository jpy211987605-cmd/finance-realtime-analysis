#!/bin/bash
# ============================================================================
# 演示专用：一键重启整个系统（从零开始，100% 可靠）
#
# 用法: bash scripts/demo_restart.sh
#
# 流程:
#   1. 停止所有容器 + 删除数据卷 (干净状态)
#   2. 启动 Docker 集群 (12 节点)
#   3. 等待所有服务就绪
#   4. 初始化 Kafka + HDFS + Hive + ES
#   5. 启动数据管道 (Spark + Producer + API)
# ============================================================================

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

cd "$PROJECT_ROOT"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  金融实时分析系统 - 演示模式重启${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# ====================================================================
# Step 1: 彻底清理
# ====================================================================
echo -e "${YELLOW}[1/5] 清理旧环境...${NC}"
cd "$DOCKER_DIR"
docker compose down -v 2>/dev/null || true
echo -e "${GREEN}  已清理${NC}"

# ====================================================================
# Step 2: 启动集群
# ====================================================================
echo ""
echo -e "${YELLOW}[2/5] 启动 Docker 集群...${NC}"
docker compose up -d
echo -e "${GREEN}  容器已启动，等待服务就绪...${NC}"

# 等待 namenode + kafka + es 就绪
echo "  等待 NameNode..."
for i in $(seq 1 30); do
  curl -s http://localhost:9870 >/dev/null 2>&1 && break
  sleep 3
done

echo "  等待 Kafka..."
for i in $(seq 1 20); do
  docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 3
done

echo "  等待 ES..."
for i in $(seq 1 20); do
  curl -s http://localhost:9200 >/dev/null 2>&1 && break
  sleep 3
done

# 额外等待 datanode + hive 初始化
echo "  等待 DataNode + Hive 初始化 (约 90s)..."
sleep 90

echo -e "${GREEN}  集群就绪${NC}"

# ====================================================================
# Step 3: 初始化业务组件
# ====================================================================
echo ""
echo -e "${YELLOW}[3/5] 初始化业务组件...${NC}"
cd "$PROJECT_ROOT"

# ES 索引 (先停 Logstash 防止自动创建错误映射)
echo "  创建 ES 索引..."
docker stop logstash 2>/dev/null || true
sleep 2
curl -s -X DELETE http://localhost:9200/finance_realtime_metrics >/dev/null 2>&1 || true
sleep 1
curl -s -X PUT http://localhost:9200/finance_realtime_metrics -H 'Content-Type: application/json' -d @"$PROJECT_ROOT/config/es_mapping.json" >/dev/null 2>&1
# Spark 统计索引 + 小时OHLC索引
curl -s -X PUT http://localhost:9200/finance_spark_stats -H 'Content-Type: application/json' -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' >/dev/null 2>&1
curl -s -X PUT http://localhost:9200/finance_hourly_ohlc -H 'Content-Type: application/json' -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' >/dev/null 2>&1
echo "  ES 索引创建完成"
docker start logstash 2>/dev/null || true

# Kafka Topics
echo "  创建 Kafka Topics..."
for t in copper_tick gas_tick gold_tick oil_tick silver_tick; do
  docker exec kafka kafka-topics --create --topic "$t" --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
done

# HDFS
echo "  创建 HDFS 目录..."
for s in gold oil silver copper gas; do
  docker exec hadoop-namenode hdfs dfs -mkdir -p "/user/finance/raw/$s" 2>/dev/null || true
done
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/finance/agg/1min /user/finance/agg/5min 2>/dev/null || true
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /user/finance 2>/dev/null || true

# Hive (等 HiveServer2 就绪)
echo "  等待 HiveServer2..."
for i in $(seq 1 30); do
  docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 5
done

echo "  创建 Hive 表..."
docker cp "$PROJECT_ROOT/config/hive/hive_ddl.sql" hive-server:/tmp/ddl.sql 2>/dev/null
docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -f /tmp/ddl.sql 2>/dev/null || \
  docker exec hive-server hive -f /tmp/ddl.sql 2>/dev/null || true

echo -e "${GREEN}  业务组件初始化完成${NC}"

# ====================================================================
# Step 4: 启动数据管道
# ====================================================================
echo ""
echo -e "${YELLOW}[4/5] 启动数据管道...${NC}"

LOGS_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOGS_DIR"
:> "$LOGS_DIR/spark_streaming.log"
:> "$LOGS_DIR/kafka_producer.log"
:> "$LOGS_DIR/fastapi.log"

# 预下载 Kafka JAR (绕过 ivy 权限)
echo "  准备 Spark Kafka JAR..."
JARS_DIR=/opt/spark/jars
docker exec -u root spark-master bash -c "
cd $JARS_DIR
for url in \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; do
  [ -f \$(basename \$url) ] || wget -q \$url
done
" 2>/dev/null
docker exec -u root spark-worker bash -c "
cd $JARS_DIR
for url in \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; do
  [ -f \$(basename \$url) ] || wget -q \$url
done
" 2>/dev/null
echo "  准备 Spark Kafka JAR... OK"

# Spark Streaming
echo "  启动 Spark Streaming..."
docker exec spark-master rm -rf /tmp/spark_checkpoint_finance /tmp/spark_cp_finance 2>/dev/null || true
nohup docker exec spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --name "FinanceStreaming" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.cores.max=2" \
    --conf "spark.sql.streaming.checkpointLocation=file:///tmp/spark_cp_finance" \
    --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
    /opt/spark/work-dir/src/python/finance_spark_streaming.py \
    --kafka-brokers kafka:9093 \
    --trigger "10 seconds" \
    > "$LOGS_DIR/spark_streaming.log" 2>&1 &

sleep 8

# Producer
echo "  启动 Kafka Producer..."
nohup python3 "$PROJECT_ROOT/src/python/finance_kafka_producer.py" \
    --rate 5 --loop --broker localhost:9092 \
    > "$LOGS_DIR/kafka_producer.log" 2>&1 &

# FastAPI
echo "  启动 FastAPI..."
pkill -f finance_api_server 2>/dev/null || true
sleep 1
nohup python3 "$PROJECT_ROOT/src/python/finance_api_server.py" \
    > "$LOGS_DIR/fastapi.log" 2>&1 &

sleep 3
echo -e "${GREEN}  数据管道已启动${NC}"

# ====================================================================
# Step 5: 验证
# ====================================================================
echo ""
echo -e "${YELLOW}[5/5] 等待数据流入 (30s)...${NC}"
sleep 30

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}  系统启动完成！${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo "  API 行情:  curl http://localhost:8000/api/realtime/latest"
echo "  ES 数据:   curl http://localhost:9200/finance_realtime_metrics/_count"
echo "  API 文档:  http://localhost:8000/docs"
echo "  前端大屏:  http://localhost:3000/finance_dashboard.html"
echo ""

# 快速验证
ES_COUNT=$(curl -s http://localhost:9200/finance_realtime_metrics/_count 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0")
echo "  ES 数据量: ${ES_COUNT}"
echo ""
