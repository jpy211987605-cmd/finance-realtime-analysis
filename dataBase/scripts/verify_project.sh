#!/bin/bash

###############################################################################
# 电商大数据分析平台 - 快速验证脚本
# 功能：验证所有服务是否正常运行，并测试完整的数据处理流程
###############################################################################

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  电商大数据分析平台 - 快速验证脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 获取本机IP地址
LOCAL_IP=$(hostname -I | awk '{print $1}')

# 总分
TOTAL_SCORE=0
MAX_SCORE=10

# 函数：检查服务
check_service() {
    local service_name=$1
    local url=$2
    local description=$3
    
    echo -n "检查 $service_name... "
    
    if curl -s --connect-timeout 5 "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((TOTAL_SCORE++))
        return 0
    else
        echo -e "${RED}✗${NC}"
        echo -e "  ${YELLOW}无法访问: $url${NC}"
        return 1
    fi
}

echo -e "${BLUE}【1】检查Docker容器状态${NC}"
echo "----------------------------------------"

cd "$PROJECT_ROOT/docker"

# 检查容器是否运行
RUNNING_CONTAINERS=$(docker-compose ps --services --filter "status=running" | wc -l)
TOTAL_CONTAINERS=$(docker-compose ps --services | wc -l)

echo "运行中的容器: $RUNNING_CONTAINERS / $TOTAL_CONTAINERS"

if [ "$RUNNING_CONTAINERS" -eq "$TOTAL_CONTAINERS" ]; then
    echo -e "${GREEN}✓ 所有容器都在运行${NC}"
    ((TOTAL_SCORE++))
else
    echo -e "${YELLOW}⚠ 部分容器未运行${NC}"
    docker-compose ps
fi

echo ""
echo -e "${BLUE}【2】检查大数据服务${NC}"
echo "----------------------------------------"

# 检查各个服务
check_service "HDFS NameNode" "http://localhost:9870" "HDFS Web界面"
check_service "Spark Master" "http://localhost:8080" "Spark Master Web界面"
check_service "Kibana" "http://localhost:5601" "Kibana可视化界面"
check_service "Elasticsearch" "http://localhost:9200" "Elasticsearch API"

echo ""
echo -e "${BLUE}【3】检查FastAPI服务${NC}"
echo "----------------------------------------"

# 检查FastAPI服务
if curl -s --connect-timeout 5 "http://localhost:8000/health" > /dev/null 2>&1; then
    echo -e "FastAPI服务... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    
    # 获取健康检查详情
    HEALTH_DATA=$(curl -s "http://localhost:8000/health")
    echo "  健康状态: $HEALTH_DATA"
else
    echo -e "FastAPI服务... ${RED}✗${NC}"
    echo -e "  ${YELLOW}FastAPI服务未运行，请先启动项目${NC}"
fi

echo ""
echo -e "${BLUE}【4】测试数据生成功能${NC}"
echo "----------------------------------------"

cd "$PROJECT_ROOT/src/python"

# 测试生成少量数据
echo "测试生成100条数据..."
if python3 generate_logs.py --count 100 --output "$PROJECT_ROOT/data/test_logs" > /dev/null 2>&1; then
    echo -e "数据生成... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    
    # 检查生成的文件
    if ls "$PROJECT_ROOT/data/test_logs"/user_behavior_*.log 1> /dev/null 2>&1; then
        FILE_COUNT=$(wc -l < "$PROJECT_ROOT/data/test_logs"/user_behavior_*.log)
        echo "  生成了 $FILE_COUNT 条记录"
    fi
else
    echo -e "数据生成... ${RED}✗${NC}"
fi

echo ""
echo -e "${BLUE}【5】测试模型训练功能${NC}"
echo "----------------------------------------"

# 测试模型训练
echo "测试模型训练..."
if timeout 60 python3 train_sklearn_model.py > /tmp/model_train.log 2>&1; then
    echo -e "模型训练... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    
    # 检查模型文件
    if [ -f "$PROJECT_ROOT/src/python/models/lr_model.pkl" ] && [ -f "$PROJECT_ROOT/src/python/models/sales_model.pkl" ]; then
        echo "  模型文件已生成"
    fi
else
    echo -e "模型训练... ${YELLOW}⚠ 超时或失败${NC}"
    echo "  查看日志: cat /tmp/model_train.log"
fi

echo ""
echo -e "${BLUE}【6】测试FastAPI工作流${NC}"
echo "----------------------------------------"

# 测试工作流启动
echo "测试工作流启动..."
WORKFLOW_RESPONSE=$(curl -s -X POST "http://localhost:8000/workflow/run" \
    -H "Content-Type: application/json" \
    -d '{"count": 1000, "output_dir": "data/test_workflow"}' 2>/dev/null || echo '{"error": "API不可用"}')

if echo "$WORKFLOW_RESPONSE" | grep -q "started\|success"; then
    echo -e "工作流启动... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    echo "  响应: $WORKFLOW_RESPONSE"
else
    echo -e "工作流启动... ${RED}✗${NC}"
    echo "  响应: $WORKFLOW_RESPONSE"
fi

echo ""
echo -e "${BLUE}【7】检查分析结果API${NC}"
echo "----------------------------------------"

# 测试获取分析结果
echo "测试获取分析结果..."
ANALYSIS_RESPONSE=$(curl -s "http://localhost:8000/analysis/results" 2>/dev/null || echo '{"error": "API不可用"}')

if echo "$ANALYSIS_RESPONSE" | grep -q "user_profile\|category_stats"; then
    echo -e "分析结果API... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    
    # 提取一些关键信息
    DATA_COUNT=$(echo "$ANALYSIS_RESPONSE" | grep -o '"data_count":[0-9]*' | cut -d':' -f2)
    GENERATION_TIME=$(echo "$ANALYSIS_RESPONSE" | grep -o '"generation_time":"[^"]*"' | cut -d'"' -f4)
    echo "  数据量: ${DATA_COUNT:-N/A}"
    echo "  生成时间: ${GENERATION_TIME:-N/A}"
else
    echo -e "分析结果API... ${RED}✗${NC}"
fi

echo ""
echo -e "${BLUE}【8】检查可视化文件${NC}"
echo "----------------------------------------"

# 检查可视化文件
if [ -f "$PROJECT_ROOT/output/dashboard.html" ]; then
    echo -e "可视化大屏... ${GREEN}✓${NC}"
    ((TOTAL_SCORE++))
    echo "  文件路径: $PROJECT_ROOT/output/dashboard.html"
else
    echo -e "可视化大屏... ${RED}✗${NC}"
fi

echo ""
echo -e "${BLUE}【9】网络连接测试${NC}"
echo "----------------------------------------"

# 测试端口连接
echo "测试关键端口..."
PORTS_OK=0

for port in 8000 8080 9200 5601 9870; do
    if timeout 2 bash -c "cat < /dev/null > /dev/tcp/localhost/$port" 2>/dev/null; then
        echo -e "  端口 $port: ${GREEN}✓${NC}"
        ((PORTS_OK++))
    else
        echo -e "  端口 $port: ${RED}✗${NC}"
    fi
done

if [ "$PORTS_OK" -ge 4 ]; then
    ((TOTAL_SCORE++))
fi

echo ""
echo -e "${BLUE}【10】项目文件完整性检查${NC}"
echo "----------------------------------------"

# 检查关键文件是否存在
REQUIRED_FILES=(
    "src/python/main.py"
    "src/python/generate_logs.py"
    "src/python/train_sklearn_model.py"
    "output/dashboard.html"
    "docker/docker-compose.yml"
)

FILES_OK=0
for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$PROJECT_ROOT/$file" ]; then
        ((FILES_OK++))
    else
        echo -e "  缺少文件: $file"
    fi
done

if [ "$FILES_OK" -eq "${#REQUIRED_FILES[@]}" ]; then
    echo -e "项目文件... ${GREEN}✓${NC} ($FILES_OK/${#REQUIRED_FILES[@]})"
    ((TOTAL_SCORE++))
else
    echo -e "项目文件... ${YELLOW}⚠ ($FILES_OK/${#REQUIRED_FILES[@]})${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  验证结果汇总${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "总分: ${GREEN}$TOTAL_SCORE${NC} / $MAX_SCORE"
echo ""

if [ "$TOTAL_SCORE" -eq "$MAX_SCORE" ]; then
    echo -e "${GREEN}🎉 恭喜！所有验证项目都通过了！${NC}"
    echo ""
    echo -e "${GREEN}项目状态: 优秀${NC}"
    echo -e "${GREEN}系统已就绪，可以正常使用！${NC}"
elif [ "$TOTAL_SCORE" -ge 8 ]; then
    echo -e "${YELLOW}✓ 项目基本正常，有少量问题${NC}"
    echo -e "${YELLOW}建议检查上述失败的项目${NC}"
else
    echo -e "${RED}✗ 项目存在较多问题${NC}"
    echo -e "${RED}建议重新启动项目或检查配置${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  访问地址${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}应用服务：${NC}"
echo -e "  FastAPI服务:        ${YELLOW}http://$LOCAL_IP:8000${NC}"
echo -e "  API文档:            ${YELLOW}http://$LOCAL_IP:8000/docs${NC}"
echo -e "  可视化大屏:         ${YELLOW}file://$PROJECT_ROOT/output/dashboard.html${NC}"
echo ""
echo -e "${GREEN}大数据集群：${NC}"
echo -e "  HDFS Web UI:        ${YELLOW}http://$LOCAL_IP:9870${NC}"
echo -e "  Spark Master:       ${YELLOW}http://$LOCAL_IP:8080${NC}"
echo -e "  Kibana Dashboard:   ${YELLOW}http://$LOCAL_IP:5601${NC}"
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  验证完成！${NC}"
echo -e "${BLUE}========================================${NC}"

exit 0