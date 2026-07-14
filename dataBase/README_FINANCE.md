# 🏦 金融实时大数据分析系统

基于 Docker 大数据集群构建的完整金融实时数据分析平台，覆盖**数据采集 → 消息队列 → 数据湖存储 → 数据仓库建模 → 实时计算 → 可视化**全链路。

---

## 技术栈

| 层级 | 技术 | 用途 |
|------|------|------|
| 数据采集 | Python + Kafka Producer | 读取 5 种商品 CSV，模拟实时 tick 行情流 |
| 消息队列 | Kafka (5 Topics) | 实时数据缓冲与解耦 |
| 数据湖 | HDFS | 原始金融数据持久存储 |
| 数据仓库 | Hive 4层建模 | ODS → DWD → DWS → ADS |
| 实时计算 | Spark Structured Streaming | 滑动窗口聚合、技术指标计算、SMA |
| 搜索引擎 | Elasticsearch | 实时指标存储与查询 |
| 日志管道 | Logstash | Kafka → ES 辅助数据管道 |
| 数据服务 | FastAPI | REST API 供前端调用 |
| 可视化 | HTML + ECharts / Vue3 | 金融实时分析大屏 |
| 容器编排 | Docker Compose | 12 节点集群编排 |

---

## 数据资产

| 品种 | 文件 | 条数 | 价格范围 | Kafka Topic |
|------|------|------|----------|-------------|
| 🥇 黄金期货 | `gold_tick_data.csv` | 50,000 | ~$4,111 | `gold_tick` |
| 🛢️ 原油期货 | `oil_tick_data.csv` | 50,000 | ~$72.5 | `oil_tick` |
| 🥈 白银期货 | `silver_tick_data.csv` | 50,000 | ~$58.4 | `silver_tick` |
| 🔶 铜期货 | `copper_tick_data.csv` | 50,000 | ~$4.25 | `copper_tick` |
| 🔥 天然气期货 | `gas_tick_data.csv` | 50,000 | ~$3.20 | `gas_tick` |

**字段**: `timestamp, price, volume, bid, ask, spread, direction, volatility, high, low`

---

## 系统架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Docker Compose 集群                           │
│                                                                       │
│  ┌──────────┐     ┌──────────┐     ┌───────────────────────────────┐ │
│  │ZooKeeper │────▶│  Kafka   │     │  HDFS (NameNode + DataNode)   │ │
│  │  :2181   │     │ :9092/93 │     │  :9870 :8020 :9864            │ │
│  └──────────┘     └────┬─────┘     └──────────┬────────────────────┘ │
│                        │                       │                      │
│         ┌──────────────┼───────────────────────┼──────────────┐       │
│         │              │                       │              │       │
│         ▼              ▼                       ▼              ▼       │
│   ┌──────────┐  ┌─────────────┐  ┌──────────────────┐ ┌──────────┐  │
│   │Logstash  │  │Spark Master │  │  Hive Metastore  │ │ Hive     │  │
│   │:5044/5000│  │+ Worker     │  │  PG:5432 :9083   │ │Server2   │  │
│   └────┬─────┘  │:8080 :7077  │  └────────┬─────────┘ │:10000    │  │
│        │        └──────┬──────┘           │            └──────────┘  │
│        │               │                  │                          │
│        ▼               ▼                  │                          │
│   ┌─────────────────────────────────┐     │                          │
│   │      Elasticsearch :9200        │◀────┘                          │
│   └──────────────┬──────────────────┘                                │
│                  │                                                    │
│   ┌──────────────┼──────────────────┐                                │
│   │   Kibana     │    FastAPI:8000  │                                │
│   │   :5601      │    (宿主机)       │                                │
│   └──────────────┘    ┌──────┴─────┐                                 │
│                       │ 前端大屏    │                                 │
│                       │ :3000/HTML │                                 │
│                       └────────────┘                                 │
└──────────────────────────────────────────────────────────────────────┘
```

### 数据链路

```
[5×CSV 文件] ──▶ Kafka Producer (宿主机) ──▶ Kafka (5 Topics)
                                                  │
                    ┌─────────────────────────────┼──────────────────┐
                    ▼                             ▼                  ▼
              Spark Streaming             Logstash (辅助)     直接消费
              (容器内运行)                    │
                    │                         ▼
        ┌───────────┼───────────┐     ES (finance_realtime_raw)
        ▼           ▼           ▼
      HDFS         ES           Hive
  /user/finance/   metrics      ODS→DWD→DWS→ADS
  raw/{symbol}/     + agg
                    │
                    ▼
              FastAPI ◀── ES ◀── 前端大屏
```

### 关键连接串

| 来源 | 目标 | 地址 | 说明 |
|------|------|------|------|
| 宿主机 Producer | Kafka | `localhost:9092` | Kafka 对外端口 |
| Spark 容器 | Kafka | `kafka:9093` | Docker 网络内 |
| Spark 容器 | HDFS | `hdfs://hadoop-namenode:8020` | 容器名解析 |
| Spark 容器 | ES | `elasticsearch:9200` | Docker 网络内 |
| FastAPI (宿主机) | ES | `localhost:9200` | ES 对外端口 |
| HiveServer2 | Metastore | `thrift://hive-metastore:9083` | Docker 网络内 |

---

## 快速启动

### 前置条件

- Docker 20.10+ & Docker Compose 2.0+
- Python 3.8+ (宿主机)
- Node.js 18+ (仅 Vue3 前端需要)

### 安装 Python 依赖

```bash
cd dataBase
pip install -r requirements.txt
```

### 第一步：启动集群 + 初始化（手动执行）

```bash
# 1. 启动 Docker 大数据集群
cd docker
docker compose up -d

# 等待约 90-120 秒让所有容器就绪...
# 可用以下命令检查:
#   docker compose ps

# 2. 初始化所有业务组件
cd ..
bash scripts/init_all.sh
```

`init_all.sh` 会自动完成:
- ✅ Kafka Topic 创建 (5 个品种)
- ✅ HDFS 目录结构初始化
- ✅ Hive 数仓 4 层建表 (ODS→DWD→DWS→ADS)
- ✅ Elasticsearch 索引创建 (finance_realtime_metrics + agg)

### 第二步：启动数据管道（手动执行）

```bash
bash scripts/start_pipeline.sh
```

或自定义参数:
```bash
bash scripts/start_pipeline.sh --rate 10 --loop    # 10条/秒, 循环发送
bash scripts/start_pipeline.sh --no-spark           # 跳过 Spark (仅 Producer + API)
```

`start_pipeline.sh` 按顺序启动:
1. **Spark Streaming** — 提交到 Spark 集群，消费 Kafka → 计算指标 → 写 HDFS + ES
2. **Kafka Producer** — 读取 CSV → 发到 Kafka (后台进程)
3. **FastAPI** — 启动数据服务 (后台进程)

### 第三步：打开前端

```bash
# 方式A: 纯 HTML 大屏 (无需 npm，推荐)
# 浏览器直接打开: output/finance_dashboard.html

# 方式B: Vue3 开发服务器
cd frontend
npm install && npm run dev
# 浏览器打开: http://localhost:3000
```

### 停止系统

```bash
# 停止业务管道
bash scripts/stop_pipeline.sh

# 停止 Docker 集群 (保留数据卷)
cd docker && docker compose down

# 完全清理 (删除数据卷)
cd docker && docker compose down -v
```

---

## 服务端口

| 服务 | 端口 | 访问地址 |
|------|------|----------|
| HDFS NameNode Web UI | 9870 | http://localhost:9870 |
| Kafka | 9092 | localhost:9092 |
| ZooKeeper | 2181 | localhost:2181 |
| Hive Server2 | 10000 | jdbc:hive2://localhost:10000 |
| Hive Metastore | 9083 | thrift://localhost:9083 |
| Elasticsearch | 9200 | http://localhost:9200 |
| Kibana | 5601 | http://localhost:5601 |
| Spark Master UI | 8080 | http://localhost:8080 |
| Spark Master RPC | 7077 | spark://localhost:7077 |
| FastAPI | 8000 | http://localhost:8000 |
| API 文档 (Swagger) | 8000 | http://localhost:8000/docs |
| Vue3 Dev Server | 3000 | http://localhost:3000 |

---

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/api/symbols` | 品种列表 |
| GET | `/api/realtime/latest` | 5品种最新行情快照 |
| GET | `/api/realtime/trend?symbol=GOLD&minutes=60` | 价格趋势时间序列 |
| GET | `/api/realtime/volume` | 成交量汇总 |
| GET | `/api/realtime/risk` | 风险指标 |
| GET | `/api/realtime/aggregates?window=1min` | OHLC 聚合数据 |
| GET | `/api/realtime/summary` | 汇总概览（精简版） |

### 示例

```bash
# 健康检查
curl http://localhost:8000/health

# 最新行情
curl http://localhost:8000/api/realtime/latest | python -m json.tool

# 黄金趋势 (最近60分钟)
curl "http://localhost:8000/api/realtime/trend?symbol=GOLD&minutes=60" | python -m json.tool

# 汇总概览
curl http://localhost:8000/api/realtime/summary | python -m json.tool
```

---

## 验证方法

### 全链路一键验证

```bash
bash scripts/verify_data_flow.sh
```

### 分层手动验证

```bash
# 1. Docker 容器
cd docker && docker compose ps
# 预期: 所有容器 Up + healthy

# 2. Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic gold_tick --max-messages 3

# 3. HDFS
docker exec hadoop-namenode hdfs dfs -ls -R /user/finance/
# 预期: raw/{gold,oil,silver,copper,gas}/dt=... 目录中有 JSON 文件

# 4. Hive
docker exec hive-server hive -e "SHOW DATABASES;"
docker exec hive-server hive -e "MSCK REPAIR TABLE ods_finance.gold_tick; SELECT COUNT(*) FROM ods_finance.gold_tick;"

# 5. Elasticsearch
curl http://localhost:9200/_cat/indices?v
curl http://localhost:9200/finance_realtime_metrics/_count

# 6. Spark
curl -s http://localhost:8080/json/ | python -m json.tool

# 7. FastAPI
curl http://localhost:8000/health
curl http://localhost:8000/api/realtime/latest | python -m json.tool
```

---

## 金融分析指标

### 实时指标（Spark → ES）

| 类别 | 指标 | 说明 |
|------|------|------|
| 价格 | 最新价、涨跌额、涨跌幅 | 逐笔更新 |
| 技术分析 | SMA(5/10/20) | 简单移动平均 |
| 成交量 | 逐笔成交量 | 实时累计 |
| 波动率 | 瞬时波动率 | 风险评估 |
| 价差 | 买卖价差、价差比例 | 流动性评估 |
| OHLC | 开高低收 | 1min/5min 窗口聚合 |

### 风险指标

| 指标 | 说明 |
|------|------|
| 最大回撤 | 从峰值到谷底的最大跌幅 |
| 平均波动率 | 价格波动程度 |
| 价差 | 买卖价差（流动性风险） |

---

## 可重复运行

本系统设计为**可多轮重复运行**：

| 组件 | 策略 |
|------|------|
| Kafka | `init_all.sh` 检测已存在的 Topic 并跳过，避免重复创建 |
| HDFS | 数据按 dt 分区存储，Spark 写入追加模式 |
| Hive | 建表使用 `IF NOT EXISTS`，分区按日隔离 |
| ES | `init_all.sh --reset` 删除旧索引重建 |
| Spark | 使用新的 checkpoint 目录，`forceDeleteTempCheckpointLocation=true` |
| Producer | 支持 `--loop` 循环模式 |

### 重新启动流程

```bash
# 停止管道
bash scripts/stop_pipeline.sh

# (可选) 重置 ES 数据
curl -X DELETE http://localhost:9200/finance_realtime_*

# 重新初始化 + 启动
bash scripts/init_all.sh --reset
bash scripts/start_pipeline.sh --loop
```

---

## 项目结构

```
dataBase/
├── docker/
│   ├── docker-compose.yml                   # 🔒 不可修改 — 12 节点集群编排
│   └── lib/                                 # Hive/Spark JAR 依赖
│       ├── postgresql-42.7.4.jar
│       └── guava-27.0-jre.jar
├── lib/                                     # Spark 容器挂载点 (docker-compose 要求)
│   ├── postgresql-42.7.4.jar               # ← 从 docker/lib/ 复制
│   └── guava-27.0-jre.jar
├── config/
│   ├── hive/
│   │   ├── hive_ddl.sql                     # 金融数仓四层 DDL
│   │   └── hive-site.xml                    # 🔒 Hive 配置 (容器动态生成)
│   ├── logstash/
│   │   └── kafka_to_es.conf                # Logstash 管道配置
│   └── mirror/
│       └── pip.conf                         # pip 镜像配置
├── scripts/
│   ├── init_all.sh                          # ★ 一键初始化 (Kafka+HDFS+Hive+ES)
│   ├── start_pipeline.sh                    # ★ 启动数据管道
│   ├── stop_pipeline.sh                     # ★ 停止数据管道
│   ├── init_kafka_topics.sh                 # Kafka Topic 初始化
│   ├── init_hdfs_dirs.sh                    # HDFS 目录初始化
│   ├── init_hive_tables.sh                  # Hive 建表
│   ├── verify_data_flow.sh                  # 全链路验证
│   ├── start_finance_system.sh              # 原有全自动脚本 (可用但不推荐)
│   └── stop_cluster.sh                      # 集群停止
├── src/python/
│   ├── finance_kafka_producer.py            # ★ CSV → Kafka 生产者
│   ├── finance_spark_streaming.py           # ★ Spark Streaming 实时计算
│   └── finance_api_server.py               # ★ FastAPI 数据服务
├── frontend/                                # Vue3 前端工程
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── App.vue                          # 主布局
│       ├── components/                      # 图表组件
│       └── composables/                     # 数据轮询
├── output/
│   └── finance_dashboard.html               # ★ 纯 HTML 大屏 (无需 npm)
├── requirements.txt                         # ★ Python 依赖声明
├── *.csv                                    # 5 个金融数据文件
└── README_FINANCE.md                        # 本文档
```

---

## 常见问题

| 问题 | 排查 |
|------|------|
| Kafka 无法连接 | `docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092` |
| HDFS 写入失败 | `docker logs hadoop-namenode --tail 30` |
| Spark 作业失败 | 查看 http://localhost:8080 → Running Applications → 点击查看日志 |
| ES 数据为空 | `curl http://localhost:9200/finance_realtime_metrics/_count`，检查 Spark 日志 |
| Hive 查询超时 | `docker logs hive-server --tail 30` |
| Producer 找不到 CSV | 确保从项目根目录 `dataBase/` 运行脚本 |
| 端口冲突 | `netstat -ano \| findstr "8000 9092 9200"` (Windows) |
| 容器启动失败 | `cd docker && docker compose logs {容器名}` |
