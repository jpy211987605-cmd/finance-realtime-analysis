#!/bin/bash

###############################################################################
# 电商大数据分析平台 - 一键启动脚本
# 功能：启动Docker集群、FastAPI服务，并提供访问地址
###############################################################################

set -e  # 遇到错误立即退出

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
echo -e "${BLUE}  电商大数据分析平台 - 一键启动脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker未安装，请先安装Docker${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker已安装${NC}"

# 检查Docker Compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose未安装，请先安装Docker Compose${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker Compose已安装${NC}"
echo ""

# 检查Docker权限，确定是否需要sudo
DOCKER_CMD="docker"
DOCKER_COMPOSE_CMD="docker-compose"

if ! docker info > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠ 当前用户没有Docker权限，将使用sudo${NC}"
    DOCKER_CMD="sudo docker"
    DOCKER_COMPOSE_CMD="sudo docker-compose"
    
    # 检查sudo是否可用
    if ! sudo -n true 2>/dev/null; then
        echo -e "${YELLOW}⚠ 需要输入sudo密码${NC}"
    fi
fi

echo ""
# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo -e "${YELLOW}⚠ Python3未安装，FastAPI服务将无法启动${NC}"
else
    echo -e "${GREEN}✓ Python3已安装${NC}"
fi

# 检查pip
if command -v pip3 &> /dev/null; then
    echo -e "${GREEN}✓ pip3已安装${NC}"
else
    echo -e "${YELLOW}⚠ pip3未安装，可能无法安装Python依赖${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤1：启动Docker大数据集群${NC}"
echo -e "${BLUE}========================================${NC}"

cd "$PROJECT_ROOT/docker"

# 停止旧容器
echo -e "${YELLOW}停止旧容器...${NC}"
${DOCKER_COMPOSE_CMD} down 2>/dev/null || true

# 启动新容器
echo -e "${YELLOW}启动大数据集群...${NC}"
${DOCKER_COMPOSE_CMD} up -d

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤2：等待服务启动${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "${YELLOW}等待服务启动（约60秒）...${NC}"
sleep 60

# 检查容器状态
echo -e "${YELLOW}检查容器状态...${NC}"
${DOCKER_COMPOSE_CMD} ps

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤3：检查Python依赖${NC}"
echo -e "${BLUE}========================================${NC}"

cd "$PROJECT_ROOT"

# 检查必要的Python依赖是否已安装
REQUIRED_PACKAGES=("fastapi" "uvicorn" "pandas" "numpy" "sklearn" "joblib")
MISSING_PACKAGES=()

if command -v python3 &> /dev/null; then
    echo -e "${YELLOW}检查Python依赖包...${NC}"
    
    for pkg in "${REQUIRED_PACKAGES[@]}"; do
        if ! python3 -c "import $pkg" 2>/dev/null; then
            MISSING_PACKAGES+=("$pkg")
        fi
    done
    
    if [ ${#MISSING_PACKAGES[@]} -eq 0 ]; then
        echo -e "${GREEN}✓ 所有Python依赖已安装${NC}"
    else
        echo -e "${YELLOW}⚠ 缺少以下Python依赖包：${NC}"
        for pkg in "${MISSING_PACKAGES[@]}"; do
            echo -e "  - $pkg"
        done
        echo -e "${YELLOW}  请手动安装：pip3 install ${MISSING_PACKAGES[*]}${NC}"
    fi
else
    echo -e "${YELLOW}⚠ 无法检查Python依赖（python3不可用）${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤4：启动FastAPI服务${NC}"
echo -e "${BLUE}========================================${NC}"

# 创建必要的目录
mkdir -p data/logs models output logs

# 启动FastAPI服务（后台运行）
if command -v python3 &> /dev/null; then
    echo -e "${YELLOW}启动FastAPI服务...${NC}"
    cd "$PROJECT_ROOT/src/python"
    
    # 检查是否已有FastAPI进程在运行
    if pgrep -f "uvicorn.*main:app" > /dev/null; then
        echo -e "${YELLOW}FastAPI服务已在运行，先停止旧进程...${NC}"
        pkill -f "uvicorn.*main:app"
        sleep 2
    fi
    
    # 启动FastAPI服务
    nohup python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 > "$PROJECT_ROOT/logs/fastapi.log" 2>&1 &
    FASTAPI_PID=$!
    
    # 等待FastAPI启动
    echo -e "${YELLOW}等待FastAPI服务启动...${NC}"
    sleep 5
    
    # 检查FastAPI是否启动成功
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ FastAPI服务启动成功 (PID: $FASTAPI_PID)${NC}"
    else
        echo -e "${YELLOW}⚠ FastAPI服务可能未正常启动，请检查日志: $PROJECT_ROOT/logs/fastapi.log${NC}"
    fi
else
    echo -e "${YELLOW}⚠ 跳过FastAPI服务启动（python3不可用）${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤5：生成数据并加载到Hive${NC}"
echo -e "${BLUE}========================================${NC}"

if command -v python3 &> /dev/null; then
    echo -e "${YELLOW}生成用户行为日志...${NC}"
    cd "$PROJECT_ROOT"
    python3 src/python/generate_logs.py --count 100000 --output data/logs
    
    echo -e "${YELLOW}加载数据到Hive ODS层...${NC}"
    bash scripts/load_data_to_hive.sh
    
    echo -e "${YELLOW}执行ETL...${NC}"
    bash scripts/etl_daily.sh
    
    echo -e "${YELLOW}发送数据到Kafka（供Logstash→ES实时流）...${NC}"
    python3 src/python/kafka_log_producer.py --dir data/logs --topic user_behavior --broker localhost:9092
else
    echo -e "${YELLOW}⚠ 跳过数据加载（python3不可用）${NC}"
    echo -e "${YELLOW}  手动执行: python3 src/python/generate_logs.py && bash scripts/load_data_to_hive.sh && bash scripts/etl_daily.sh${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  步骤6：验证服务状态${NC}"
echo -e "${BLUE}========================================${NC}"

# 获取本机IP地址
LOCAL_IP=$(hostname -I | awk '{print $1}')

echo -e "${GREEN}✓ 服务启动完成！${NC}"
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  服务访问地址${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}大数据集群服务：${NC}"
echo -e "  HDFS Web UI:        ${YELLOW}http://$LOCAL_IP:9870${NC}"
echo -e "  Spark Master:       ${YELLOW}http://$LOCAL_IP:8080${NC}"
echo -e "  Kibana Dashboard:   ${YELLOW}http://$LOCAL_IP:5601${NC}"
echo -e "  Elasticsearch:      ${YELLOW}http://$LOCAL_IP:9200${NC}"
echo -e "  Kafka:              ${YELLOW}$LOCAL_IP:9092${NC}"
echo -e "  ZooKeeper:          ${YELLOW}$LOCAL_IP:2181${NC}"
echo ""
echo -e "${GREEN}应用服务：${NC}"
echo -e "  FastAPI服务:        ${YELLOW}http://$LOCAL_IP:8000${NC}"
echo -e "  API文档:            ${YELLOW}http://$LOCAL_IP:8000/docs${NC}"
echo -e "  可视化大屏:         ${YELLOW}file://$PROJECT_ROOT/output/dashboard.html${NC}"
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  使用说明${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "1. 访问可视化大屏：在浏览器中打开上述可视化大屏地址"
echo -e "2. 点击\"刷新数据\"按钮：生成10万条新数据并执行完整分析流程"
echo -e "3. 查看实时结果：所有图表将自动更新为最新分析结果"
echo ""
echo -e "${YELLOW}手动数据加载：${NC}"
echo -e "  生成日志:   python3 src/python/generate_logs.py"
echo -e "  加载到Hive: bash scripts/load_data_to_hive.sh"
echo -e "  执行ETL:    bash scripts/etl_daily.sh"
echo ""
echo -e "${YELLOW}停止服务：${NC}"
echo -e "  停止Docker集群: cd $PROJECT_ROOT/docker && ${DOCKER_COMPOSE_CMD} down"
echo -e "  停止FastAPI:    pkill -f 'uvicorn.*main:app'"
echo ""
echo -e "${YELLOW}查看日志：${NC}"
echo -e "  FastAPI日志:    tail -f $PROJECT_ROOT/logs/fastapi.log"
echo -e "  Docker日志:     cd $PROJECT_ROOT/docker && ${DOCKER_COMPOSE_CMD} logs -f [service_name]"
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  启动完成！${NC}"
echo -e "${GREEN}========================================${NC}"