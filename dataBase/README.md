# 🏦 金融实时大数据分析平台

基于 Docker 12 节点集群构建的金融实时行情分析平台，覆盖黄金、原油、白银、铜、天然气 5 个期货品种，实现从数据采集到可视化大屏的全链路自动化处理。

---

## 📋 项目概述

| 项目 | 说明 |
|------|------|
| **数据资产** | 5 个期货品种，25 万条 tick 级行情数据 |
| **数据字段** | timestamp, price, volume, bid, ask, spread, direction, volatility, high, low |
| **技术栈** | Python · Kafka · Spark · Logstash · HDFS · Hive · Elasticsearch · FastAPI · ECharts · sklearn · DeepSeek |
| **大屏功能** | 14 个可视化区域，5 秒实时刷新 |

---

## 🏗️ 系统架构

```
CSV(5×50k) → Producer(5条/s) → Kafka(5 Topics)
                                      │
                ┌─────────────────────┼─────────────────────┐
                ▼                                           ▼
        Spark Streaming                                Logstash
     (10s 微批, OHLC聚合)                           (数据清洗转换)
                │                                           │
                ▼                                           ▼
             HDFS                                      Elasticsearch
      /user/finance/raw/                          finance_realtime_metrics
      /user/finance/agg/1min/                            │
                │                                           │
                ▼                                           ▼
        Hive 四层数仓                                    FastAPI
     (ODS→DWD→DWS→ADS)                             (15个REST端点)
                │                                           │
                └──────────────┬────────────────────────────┘
                               ▼
                          ECharts 大屏
                       (5秒轮询, 14个区域)
                               │
                               └── AI 对话 (DeepSeek V4)
                               └── ML 预测 (sklearn VotingClassifier)
```

### 核心设计：双消费者分流

| 链路 | 技术 | 写入目标 | 用途 |
|------|------|----------|------|
| **上路** | Spark Streaming | HDFS | 数据湖持久化 + OHLC 聚合 + 离线分析 |
| **下路** | Logstash | Elasticsearch | 近实时查询 + 大屏数据源 |

两条链路独立消费 Kafka，互不影响。一条挂了另一条照常运行。

---

## 🐳 环境要求

| 软件 | 最低版本 | 用途 |
|------|----------|------|
| Docker | 20.10+ | 容器运行时 |
| Docker Compose | 2.0+ | 集群编排 |
| Python | 3.8+ | Producer / API / 模型训练 |
| pip | 21.0+ | Python 包管理 |

**操作系统：** Ubuntu 20.04+ 或 Windows 10+（模型训练可在 Windows 本地）

---

## 🚀 快速启动

### 第一步：安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 第二步：启动 Docker 大数据集群

```bash
cd docker
sudo docker compose up -d
```

启动后等待约 90 秒，让 12 个容器全部就绪：

```bash
sudo docker compose ps
```

### 第三步：初始化业务组件

```bash
cd ..
bash scripts/demo_restart.sh
```

这会自动完成：Kafka Topic 创建 → HDFS 目录初始化 → Hive 建表 → ES 索引创建 → 启动数据管道 → 启动 API 服务。

### 第四步：启动前端大屏

```bash
cd output && python3 -m http.server 3000 &
```

### 第五步：打开浏览器

访问：**http://localhost:3000/finance_dashboard.html**

---

## 📡 服务端口

| 服务 | 端口 | 访问地址 |
|------|------|----------|
| 前端大屏 | 3000 | http://localhost:3000/finance_dashboard.html |
| FastAPI | 8000 | http://localhost:8000/docs |
| HDFS NameNode | 9870 | http://localhost:9870 |
| Spark Master | 8080 | http://localhost:8080 |
| Elasticsearch | 9200 | http://localhost:9200 |
| Kibana | 5601 | http://localhost:5601 |
| Kafka | 9092 | localhost:9092 |
| ZooKeeper | 2181 | localhost:2181 |
| Hive Server2 | 10000 | jdbc:hive2://localhost:10000 |

---

## 📂 项目结构

```
dataBase/
├── .claude/                            # Claude AI 辅助开发配置与项目上下文管理文件
├── .gitignore                          # Git 版本控制忽略规则配置，过滤临时文件、日志及敏感数据
├── dataBase/                           # 大数据环境部署配置、数据库初始化脚本及数据存储方案
├── src/python/                         # Python 源代码
│   ├── finance_kafka_producer.py       # Kafka 生产者 (CSV→Kafka)
│   ├── finance_spark_streaming.py      # Spark Streaming (Kafka→HDFS + OHLC)
│   ├── finance_api_server.py           # FastAPI 服务 (15个端点)
│   └── train_model.py                  # ML 模型训练脚本 (Windows)
├── config/                             # 配置文件
│   ├── hive/hive_ddl.sql               # Hive 四层数仓 DDL
│   ├── logstash/kafka_to_es.conf       # Logstash 管道配置
│   └── es_mapping.json                 # ES 索引映射
├── scripts/                            # Shell 运维脚本
│   ├── demo_restart.sh                 # 一键重启 (从零构建)
│   ├── init_all.sh                     # 初始化所有组件
│   ├── start_pipeline.sh               # 启动数据管道
│   └── stop_pipeline.sh                # 停止数据管道
├── docker/                             # Docker 容器编排
│   └── docker-compose.yml              # 12 节点集群定义
├── output/                             # 前端大屏
│   └── finance_dashboard.html          # 可视化大屏 (纯HTML+ECharts)
├── requirements.txt                    # Python 依赖清单
└── README.md                           # 项目说明文档
```

---

## 🔌 API 接口

### 实时行情

| 端点 | 说明 |
|------|------|
| `GET /health` | 健康检查 |
| `GET /api/symbols` | 品种列表 |
| `GET /api/realtime/latest` | 5 品种最新行情快照 |
| `GET /api/realtime/trend?symbol=GOLD&minutes=60` | 价格趋势时间序列 |
| `GET /api/realtime/volume` | 成交量聚合 |
| `GET /api/realtime/risk` | 风险指标 (波动率/回撤/价差) |
| `GET /api/realtime/ohlc?symbol=GOLD&minutes=60` | OHLC K线数据 |

### Spark 分析

| 端点 | 说明 |
|------|------|
| `GET /api/spark/ohlc?symbol=GOLD&minutes=120` | Spark 1分钟 OHLC 聚合 |
| `GET /api/spark/status` | Spark 集群作业状态 |

### 数据查询

| 端点 | 说明 |
|------|------|
| `GET /api/data/stats` | CSV + HDFS 数据资产统计 |
| `GET /api/data/sample?symbol=GOLD` | 数据预览 |
| `GET /api/data/search?keyword=4100-4120` | 跨品种价格搜索 |
| `GET /api/analysis/correlation` | 5×5 Pearson 相关性矩阵 |

### AI & ML

| 端点 | 说明 |
|------|------|
| `GET /api/ml/predict?symbol=GOLD` | ML 价格方向预测 |
| `POST /api/chat` | AI 智能对话 (DeepSeek V4) |
| `GET/POST /api/config/apikey` | API Key 管理 |

---

## 🤖 AI 智能对话

大屏右下角浮动窗口集成 DeepSeek V4 Pro 大语言模型。

**使用：** 打开大屏 → 右下角圆形按钮 → ⚙ 设置 API Key → 开始对话。

AI 可以回答：行情分析、技术指标解读 (OHLC/SMA/RSI)、风险评估、大数据技术架构 (Kafka/Spark/Hive/ES) 等问题。

---

## 🧠 机器学习预测

### 模型

GradientBoosting + RandomForest VotingClassifier 集成分类器，预测 tick 价格方向 (UP/DOWN)。

### 特征（15 个技术指标）

价格滞后项、SMA 均线交叉、RSI 动量、布林带位置、成交量比率、价差比例、日内振幅等。

### 训练

```bash
# Windows 本地运行（使用 sklearn）
python src/python/train_model.py
```

训练后生成 `model_direction.joblib`，上传到 Ubuntu 服务器即可使用。

### 推理

FastAPI 每 5 秒从 ES 取最新 60 条实时数据 → 计算 15 个特征 → 模型推理 → 返回预测方向和概率 → 前端展示进度条。

---

## 📊 大屏功能区域

| # | 区域 | 图表类型 | 数据来源 |
|---|------|----------|----------|
| 1 | 顶栏状态 | 指示灯 | FastAPI 聚合 ES + Spark |
| 2 | 5 张行情卡片 | HTML 卡片 | ES → FastAPI |
| 3 | 价格趋势图 | 折线 + SMA + 缩放 | ES → FastAPI |
| 4 | 成交量对比 | 渐变色柱状图 | ES → FastAPI |
| 5 | Spark K线图 | Candlestick + SMA | Spark → HDFS → FastAPI |
| 6 | Spark 作业状态 | 数值卡片 | Spark Master API |
| 7 | 风险雷达图 | ECharts Radar | ES → FastAPI (计算最大回撤) |
| 8 | 风险指标面板 | HTML 卡片 | 同上 |
| 9 | 数据资产统计 | 数值卡片 | CSV + HDFS |
| 10 | 数据预览表 | HTML 表格 | CSV |
| 11 | 价格区间搜索 | 输入框+列表 | CSV |
| 12 | 相关性热力图 | ECharts Heatmap | Hive ODS |
| 13 | ML 价格预测 | 概率进度条 | ES → sklearn 模型 |
| 14 | AI 智能对话 | 浮动聊天窗口 | DeepSeek V4 Pro |

---

## 🛠️ 常见问题

| 问题 | 解决 |
|------|------|
| Spark 报 `Failed to find data source: kafka` | 手动下载 `spark-sql-kafka-0-10_2.12-3.3.3.jar` 到 `/opt/spark/jars/` |
| ES 数据为空 | 检查 Producer 和 Logstash 是否运行：`ps aux \| grep finance` |
| 大屏加载不出 | 检查 API 健康：`curl http://localhost:8000/health` |
| Docker 容器一直重启 | `docker compose logs 容器名` 查看日志 |
| 端口冲突 | `lsof -i :端口号` 查找占用进程 |

---

## 📄 许可证

MIT License
