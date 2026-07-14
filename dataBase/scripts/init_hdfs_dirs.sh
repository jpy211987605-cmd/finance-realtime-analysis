#!/bin/bash
# ============================================================================
# HDFS 目录初始化脚本
# 创建金融数据湖目录结构，与 Hive ODS 外部表 LOCATION 对齐
#
# 目录结构:
#   /user/finance/
#     raw/
#       gold/     ← 对应 ods_finance.gold_tick
#       oil/      ← 对应 ods_finance.oil_tick
#       silver/   ← 对应 ods_finance.silver_tick
#       copper/   ← 对应 ods_finance.copper_tick
#       gas/      ← 对应 ods_finance.gas_tick
#     agg/
#       1min/     ← 对应 dws_finance.tick_1min_agg
#       5min/     ← 对应 dws_finance.tick_5min_agg
#     warehouse/  ← Hive 托管表存储
#
# 使用方式:
#   bash scripts/init_hdfs_dirs.sh
# ============================================================================

HDFS_CONTAINER="hadoop-namenode"
HDFS="docker exec $HDFS_CONTAINER hdfs dfs"
ROOT_DIR="/user/finance"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "  HDFS 目录初始化"
echo "========================================"
echo ""

# ---- 检查 HDFS 容器 ----
if ! docker ps --format '{{.Names}}' | grep -q "^${HDFS_CONTAINER}$"; then
    echo -e "${RED}[ERROR]${NC} Hadoop NameNode 容器未运行！"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} HDFS NameNode 运行中"
echo ""

# ---- 创建目录 ----
SYMBOLS=("gold" "oil" "silver" "copper" "gas")

echo "创建目录结构..."
echo ""

# 根目录
$HDFS -mkdir -p "$ROOT_DIR" 2>/dev/null

# 品种原始数据目录 (与 Hive ODS LOCATION 对齐)
for sym in "${SYMBOLS[@]}"; do
    $HDFS -mkdir -p "$ROOT_DIR/raw/$sym" 2>/dev/null
    echo "  [OK] $ROOT_DIR/raw/$sym"
done

# 聚合结果目录
$HDFS -mkdir -p "$ROOT_DIR/agg/1min" 2>/dev/null
echo "  [OK] $ROOT_DIR/agg/1min"
$HDFS -mkdir -p "$ROOT_DIR/agg/5min" 2>/dev/null
echo "  [OK] $ROOT_DIR/agg/5min"

# Hive 仓库目录
$HDFS -mkdir -p "$ROOT_DIR/warehouse" 2>/dev/null
echo "  [OK] $ROOT_DIR/warehouse"

# 设置权限
$HDFS -chmod -R 777 "$ROOT_DIR" 2>/dev/null

echo ""
echo "----------------------------------------"
echo "当前 HDFS 目录结构:"
$HDFS -ls -R "$ROOT_DIR" 2>/dev/null | head -30
echo "----------------------------------------"

echo ""
echo "========================================"
echo "  HDFS 目录初始化完成"
echo "========================================"
