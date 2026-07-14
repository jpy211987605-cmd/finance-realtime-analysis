#!/bin/bash
# ============================================================================
# Hive 数仓建表脚本
# 执行金融数仓 DDL，创建 ODS → DWD → DWS → ADS 四层表
#
# 使用方式:
#   bash scripts/init_hive_tables.sh
# ============================================================================

set -e

HIVE_CONTAINER="hive-server"
DDL_SOURCE="config/hive/hive_ddl.sql"
DDL_CONTAINER_PATH="/tmp/finance_hive_ddl.sql"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Hive 数仓建表${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ---- 检查 Hive 容器 ----
if ! docker ps --format '{{.Names}}' | grep -q "^${HIVE_CONTAINER}$"; then
    echo -e "${RED}[ERROR]${NC} Hive Server 容器未运行！"
    exit 1
fi

# ---- 检查 DDL 文件 ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DDL_FILE="$PROJECT_ROOT/$DDL_SOURCE"

if [ ! -f "$DDL_FILE" ]; then
    echo -e "${RED}[ERROR]${NC} DDL 文件不存在: $DDL_FILE"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} DDL 文件: $DDL_SOURCE"
echo ""

# ---- 等待 HiveServer2 就绪 ----
echo -n "等待 HiveServer2 就绪..."
MAX_WAIT=60
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if docker exec "$HIVE_CONTAINER" beeline -u "jdbc:hive2://localhost:10000" \
        -e "SELECT 1" >/dev/null 2>&1; then
        echo -e " ${GREEN}就绪${NC} (${WAITED}s)"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo -e " ${RED}超时${NC}"
    echo -e "${YELLOW}[WARN]${NC} HiveServer2 未就绪，尝试使用 hive CLI..."
fi

# ---- 复制 DDL 到容器 ----
echo "复制 DDL 文件到容器..."
docker cp "$DDL_FILE" "${HIVE_CONTAINER}:${DDL_CONTAINER_PATH}"
echo -e "${GREEN}[OK]${NC} 已复制"

# ---- 执行 DDL ----
echo ""
echo "执行 DDL 建表语句..."
echo ""

if docker exec "$HIVE_CONTAINER" hive -f "$DDL_CONTAINER_PATH" 2>&1; then
    echo ""
    echo -e "${GREEN}[OK]${NC} DDL 执行成功 (hive CLI)"
else
    echo -e "${YELLOW}[WARN]${NC} hive CLI 失败，尝试 beeline..."
    docker exec "$HIVE_CONTAINER" \
        beeline -u "jdbc:hive2://localhost:10000" -n root \
        -f "$DDL_CONTAINER_PATH" 2>&1 || {
        echo -e "${RED}[ERROR]${NC} DDL 执行失败，请检查日志"
        exit 1
    }
fi

# ---- 清理 ----
docker exec "$HIVE_CONTAINER" rm -f "$DDL_CONTAINER_PATH" 2>/dev/null || true

# ---- 验证 ----
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  验证 Hive 表${NC}"
echo -e "${BLUE}========================================${NC}"

verify_db() {
    local db=$1
    echo ""
    echo "--- $db ---"
    docker exec "$HIVE_CONTAINER" hive -e "USE $db; SHOW TABLES;" 2>/dev/null || \
        echo -e "  ${YELLOW}(空或数据库不存在)${NC}"
}

verify_db "ods_finance"
verify_db "dwd_finance"
verify_db "dws_finance"
verify_db "ads_finance"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Hive 数仓建表完成${NC}"
echo -e "${GREEN}========================================${NC}"
