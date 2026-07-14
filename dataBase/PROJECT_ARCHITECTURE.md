# 金融实时大数据分析系统 — 完整架构文档

---

## 一、项目概览

基于 **Docker 容器集群** 构建的金融实时行情分析平台，完整覆盖大数据全链路 6 层架构：

| 层级 | 技术栈 | 作用 |
|------|--------|------|
| **数据采集层** | Python + kafka-python | 读取 5 个品种 CSV，模拟实时 tick 行情流 |
| **消息队列层** | Apache Kafka + ZooKeeper | 数据缓冲、解耦、多消费者订阅 |
| **实时计算层** | Spark Structured Streaming | 消费 Kafka，写入 HDFS（数据湖） |
| **数据管道层** | Logstash | 消费 Kafka，清洗转换，写入 Elasticsearch |
| **数据存储层** | HDFS + Hive + Elasticsearch | 数据湖 + 数据仓库 + 搜索引擎 |
| **数据服务层** | FastAPI | RESTful API，从 ES 查询返回 JSON |
| **可视化层** | HTML + ECharts | 金融实时分析大屏，3 秒自动刷新 |

---

## 二、Docker 集群架构（12 个容器）

```
                    ┌─────────────────────────────────────────────┐
                    │           Docker Network: bigdata            │
                    │              (bridge, 172.18.0.0/16)         │
                    │                                              │
   宿主机            │  ┌──────────┐     ┌──────────┐              │
  localhost:9092 ◀──┼──│  Kafka   │◀────│ZooKeeper │              │
                    │  │ :9092/93 │     │  :2181   │              │
                    │  └────┬─────┘     └──────────┘              │
                    │       │                                      │
                    │  ┌────┴──────────────────────────────┐      │
                    │  │           HDFS 集群                 │      │
                    │  │  ┌──────────────┐ ┌─────────────┐  │      │
                    │  │  │  NameNode    │ │  DataNode   │  │      │
                    │  │  │  :9870 :8020 │ │  :9864      │  │      │
                    │  │  └──────────────┘ └─────────────┘  │      │
                    │  └────────────────────────────────────┘      │
                    │                                              │
                    │  ┌──────────────────────────────────────┐   │
                    │  │           Hive 生态                    │   │
                    │  │  ┌────────────┐ ┌──────────────────┐  │   │
                    │  │  │ Metastore  │ │   HiveServer2    │  │   │
                    │  │  │ :9083      │ │   :10000 :10002  │  │   │
                    │  │  └─────┬──────┘ └──────────────────┘  │   │
                    │  │        │                                │   │
                    │  │  ┌─────┴──────┐                        │   │
                    │  │  │ PostgreSQL │ (元数据)                │   │
                    │  │  │ :5432      │                        │   │
                    │  │  └────────────┘                        │   │
                    │  └──────────────────────────────────────┘   │
                    │                                              │
                    │  ┌──────────────────────────────────────┐   │
                    │  │           ELK Stack                   │   │
                    │  │  ┌──────────┐ ┌──────────┐           │   │
                    │  │  │  ES :9200│ │Logstash  │           │   │
                    │  │  └──────────┘ │:5044 :5000│          │   │
                    │  │  ┌──────────┐ └──────────┘           │   │
                    │  │  │ Kibana   │                         │   │
                    │  │  │ :5601    │                         │   │
                    │  │  └──────────┘                         │   │
                    │  └──────────────────────────────────────┘   │
                    │                                              │
                    │  ┌──────────────────────────────────────┐   │
                    │  │         Spark 计算集群                 │   │
                    │  │  ┌──────────────┐ ┌──────────────┐   │   │
                    │  │  │ Spark Master │ │ Spark Worker │   │   │
                    │  │  │ :8080 :7077  │ │              │   │   │
                    │  │  └──────────────┘ └──────────────┘   │   │
                    │  └──────────────────────────────────────┘   │
                    │                                              │
                    │  ┌──────────────────────────────────────┐   │
                    │  │       宿主机服务 (非容器)              │   │
                    │  │  ┌──────────┐ ┌──────────────────────┐│   │
                    │  │  │FastAPI   │ │ Python HTTP Server   ││   │
                    │  │  │:8000     │ │ :3000 (前端大屏)      ││   │
                    │  │  └──────────┘ └──────────────────────┘│   │
                    │  │  ┌──────────┐                         │   │
                    │  │  │ Producer │ (Python 脚本)            │   │
                    │  │  └──────────┘                         │   │
                    │  └──────────────────────────────────────┘   │
                    └─────────────────────────────────────────────┘
```

### 容器端口与服务依赖

| 容器 | 对外端口 | 容器内端口 | 依赖 | 健康检查 |
|------|----------|-----------|------|----------|
| zookeeper | 2181 | 2181 | 无 | nc -z :2181 |
| kafka | 9092 | 9092(外)/9093(内) | zookeeper healthy | broker-api-versions |
| hadoop-namenode | 9870, 8020 | 9870, 8020 | 无 | curl :9870 |
| hadoop-datanode | 9864 | 9864 | namenode healthy | 无(靠 sleep 保护) |
| hive-metastore-postgresql | 5432 | 5432 | 无 | pg_isready |
| hive-metastore | 9083 | 9083 | PG started + namenode healthy | 无 |
| hive-server | 10000, 10002 | 10000, 10002 | metastore started | 无 |
| elasticsearch | 9200, 9300 | 9200, 9300 | 无 | _cluster/health |
| logstash | 5044, 5000 | 5044, 5000 | ES + Kafka | 无 |
| kibana | 5601 | 5601 | ES | 无 |
| spark-master | 8080, 7077 | 8080, 7077 | 无 | 无 |
| spark-worker | 无 | 无 | spark-master | 无 |

### 网络通信规则

| 通信方 | 地址 | 说明 |
|--------|------|------|
| 宿主机 → Kafka | `localhost:9092` | Kafka 对外 PLAINTEXT 端口 |
| 容器 → Kafka | `kafka:9093` | Docker 内部网络 PLAINTEXT_HOST |
| 容器 → HDFS | `hdfs://hadoop-namenode:8020` | 容器名 DNS 解析 |
| 容器 → ES | `elasticsearch:9200` | 容器名 DNS 解析 |
| 宿主机 → ES | `localhost:9200` | ES 对外端口 |
| 容器 → Hive Metastore | `thrift://hive-metastore:9083` | 容器名 DNS 解析 |
| 宿主机 → HiveServer2 | `jdbc:hive2://localhost:10000` | JDBC 连接 |

---

## 三、数据资产

### CSV 数据文件（位于 dataBase/ 根目录）

| 品种 | 文件名 | 行数 | 基准价格 | Kafka Topic |
|------|--------|------|----------|-------------|
| 🥇 黄金期货 | `gold_tick_data.csv` | 50,000 | ~$4,111 | `gold_tick` |
| 🛢️ 原油期货 | `oil_tick_data.csv` | 50,000 | ~$72.5 | `oil_tick` |
| 🥈 白银期货 | `silver_tick_data.csv` | 50,000 | ~$58.4 | `silver_tick` |
| 🔶 铜期货 | `copper_tick_data.csv` | 50,000 | ~$4.25 | `copper_tick` |
| 🔥 天然气期货 | `gas_tick_data.csv` | 50,000 | ~$3.20 | `gas_tick` |

**总计：25 万条 tick 数据**

### CSV 字段结构

```csv
timestamp,price,volume,bid,ask,spread,direction,volatility,high,low
2026-07-11 13:30:53,4111.500000,5.000000,4111.439563,4111.560437,0.120873,FLAT,0.000000,4111.500000,4111.500000
```

| 字段 | 类型 | 含义 | 示例值 |
|------|------|------|--------|
| timestamp | STRING | 时间戳 | `2026-07-11 13:30:53` |
| price | DOUBLE | 最新成交价 | `4111.50` |
| volume | DOUBLE | 成交量 | `5.0` |
| bid | DOUBLE | 买一价 | `4111.44` |
| ask | DOUBLE | 卖一价 | `4111.56` |
| spread | DOUBLE | 买卖价差 | `0.12` |
| direction | STRING | 价格方向 | `UP` / `DOWN` / `FLAT` |
| volatility | DOUBLE | 瞬时波动率 | `0.000023` |
| high | DOUBLE | 当日最高价 | `4111.63` |
| low | DOUBLE | 当日最低价 | `4111.50` |

---

## 四、完整数据链路（按时间顺序）

### 第 1 步：数据生产（Producer → Kafka）

```
┌────────────────┐     kafka-python      ┌─────────────────────┐
│ finance_kafka  │ ────────────────────▶ │  Kafka Broker       │
│ _producer.py   │   5品种轮询,5条/秒     │  localhost:9092     │
│                │   JSON 序列化          │                     │
│ 读取 CSV       │   gzip 压缩            │  5 个 Topic:        │
│ 加载到内存     │   acks=all 保证送达    │  gold_tick          │
│ 25万条数据     │                       │  oil_tick           │
└────────────────┘                       │  silver_tick        │
                                          │  copper_tick        │
                                          │  gas_tick           │
                                          │                     │
                                          │ 3 partitions/topic  │
                                          │ replication=1       │
                                          └─────────────────────┘
```

关键代码调用链：
1. `FinanceDataProducer.__init__()` — 配置 Kafka 连接、速率、模式
2. `load_all_data()` — 读取 5 个 CSV 到 `self.data_cache[symbol]`
3. `send_all()` — 5 品种轮询，每品种每次发 1 条，`send(topic, value=json_row, key=symbol_timestamp)`
4. `_flush_and_report()` — 优雅关闭 + 统计报告

### 第 2 步：实时计算（Spark Streaming → HDFS）

```
┌─────────────────────┐   spark-sql-kafka    ┌─────────────────────┐
│ Spark Streaming     │ ◀─────────────────── │  Kafka (kafka:9093) │
│ (容器内)            │   Structured         │                     │
│                     │   Streaming          │ 5 Topics 订阅       │
│ spark-submit        │   maxOffsets=2000    │ startingOffsets     │
│ FinanceStreaming    │                      │ =latest             │
└────────┬────────────┘                      └─────────────────────┘
         │
         │ 1. from_json(value, TICK_SCHEMA)  → 解析 JSON
         │ 2. to_timestamp(event_time)       → 标准化时间
         │ 3. mid_price = (bid+ask)/2        → 衍生字段
         │ 4. spread_pct = spread/price*100  → 价差比例
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│                        HDFS 写入                                │
│                                                                 │
│  write_to_hdfs(df, epoch_id):                                   │
│    for symbol in [GOLD,OIL,SILVER,COPPER,GAS]:                  │
│      df.filter(symbol==sym)                                     │
│        .write.mode("append")                                    │
│        .partitionBy("dt")                                       │
│        .json(f"/user/finance/raw/{sym_lower}/")                 │
│                                                                 │
│  输出路径示例:                                                   │
│    /user/finance/raw/gold/dt=2026-07-11/part-00003-xxx.json     │
│    /user/finance/raw/oil/dt=2026-07-11/part-00003-xxx.json      │
│    /user/finance/raw/silver/dt=2026-07-11/part-00003-xxx.json   │
│    /user/finance/raw/copper/dt=2026-07-11/part-00003-xxx.json   │
│    /user/finance/raw/gas/dt=2026-07-11/part-00003-xxx.json      │
│                                                                 │
│  触发间隔: 10 秒/批                                             │
│  checkpoint: file:///tmp/spark_cp_finance                       │
└────────────────────────────────────────────────────────────────┘
```

### 第 3 步：数据管道（Logstash → ES）

```
┌─────────────────────┐                    ┌─────────────────────┐
│  Kafka (kafka:9093) │ ────────────────▶  │    Logstash         │
│  5 Topics           │   kafka input      │    (容器内)          │
│  group: logstash_   │   earliest offset  │                     │
│  finance_group      │   json codec       │  pipeline:          │
└─────────────────────┘                    │  kafka_to_es.conf   │
                                           │                     │
                                           │  filter:            │
                                           │  ├ date 解析时间     │
                                           │  ├ mutate 类型转换   │
                                           │  ├ ruby 计算:       │
                                           │  │ spread_pct       │
                                           │  │ mid_price        │
                                           │  └ mutate 清理字段   │
                                           │                     │
                                           │  output:            │
                                           │  elasticsearch {    │
                                           │   index =>           │
                                           │   "finance_realtime_ │
                                           │    metrics"          │
                                           │   document_id =>     │
                                           │   "SYMBOL-timestamp" │
                                           │  }                  │
                                           └─────────┬───────────┘
                                                     │
                                                     ▼
                                           ┌─────────────────────┐
                                           │  Elasticsearch      │
                                           │  (elasticsearch:    │
                                           │   9200)             │
                                           │                     │
                                           │  index:             │
                                           │  finance_realtime_  │
                                           │  metrics            │
                                           │  ~3500 docs         │
                                           │  1 shard, 0 replica │
                                           │  refresh: 5s        │
                                           └─────────────────────┘
```

### 第 4 步：数据服务（FastAPI → 前端）

```
┌─────────────────────┐   elasticsearch-py   ┌─────────────────────┐
│  FastAPI :8000      │ ◀────────────────── │  ES :9200           │
│  (宿主机)            │   search/aggregate  │                     │
│                     │                      │ index:              │
│  7 个端点:          │                      │ finance_realtime_   │
│  /health            │                      │ metrics             │
│  /api/symbols       │                      └─────────────────────┘
│  /api/realtime/     │
│    latest            │
│    trend?symbol=    │
│    volume            │
│    risk              │
│    aggregates        │
│    summary           │
└─────────┬───────────┘
          │ JSON (CORS enabled)
          ▼
┌─────────────────────────────────────────┐
│  前端大屏 :3000                          │
│  finance_dashboard.html                 │
│                                         │
│  ┌─────────────────────────────────┐    │
│  │ 标题栏: 在线状态 + 更新时间       │    │
│  ├─────────────────────────────────┤    │
│  │ 黄金 | 原油 | 白银 | 铜 | 天然气  │    │ ← 5 张行情卡片
│  │ $4111 | $72 | $58 | $4 | $3     │    │   点击切换趋势
│  ├──────────────────┬──────────────┤    │
│  │ 价格趋势图        │ 成交量柱状图   │    │ ← ECharts
│  │ (折线+SMA均线)    │ (5品种对比)   │    │
│  ├──────────────────┼──────────────┤    │
│  │ 风险雷达图        │ 风险指标面板   │    │ ← 回撤/波动/价差
│  └──────────────────┴──────────────┘    │
│                                         │
│  3 秒自动刷新 (setInterval poll)         │
└─────────────────────────────────────────┘
```

### 第 5 步：数据仓库（Hive 数仓分层）

```
HDFS 原始 JSON                 Hive 外部表               Hive 内部表 (ORC)
┌──────────────────┐         ┌──────────────────┐      ┌──────────────────────┐
│ /user/finance/   │  ────▶  │ ODS 层 (ods_finance)│ ──▶ │ DWD 层 (dwd_finance)  │
│ raw/gold/dt=...  │ JsonSerDe│ gold_tick          │ ETL  │ gold_tick_detail      │
│ raw/oil/dt=...   │ 外部表    │ oil_tick           │      │ oil_tick_detail       │
│ raw/silver/dt=.. │          │ silver_tick        │      │ silver_tick_detail    │
│ raw/copper/dt=.. │          │ copper_tick        │      │ copper_tick_detail    │
│ raw/gas/dt=...   │          │ gas_tick           │      │ gas_tick_detail       │
└──────────────────┘         └──────────────────┘      └──────────┬───────────┘
                                                                   │
                                              ┌────────────────────┘
                                              ▼
                                    ┌──────────────────────┐
                                    │ DWS 层 (dws_finance)  │
                                    │ tick_1min_agg         │ ← OHLC 1分钟窗口
                                    │ tick_5min_agg         │ ← OHLC 5分钟窗口
                                    └──────────┬───────────┘
                                               │
                                               ▼
                                    ┌──────────────────────┐
                                    │ ADS 层 (ads_finance)  │
                                    │ dashboard_metrics     │ ← 大屏直读指标
                                    │ latest_snapshot       │ ← 最新快照
                                    └──────────────────────┘
```

**4 层数仓详解：**

| 层 | 数据库 | 表数量 | 存储格式 | 数据来源 | 用途 |
|-----|--------|--------|----------|----------|------|
| **ODS** | `ods_finance` | 5 | HDFS 外部表 JSON | Spark Streaming 写入 | 原始数据，不丢失，可回溯 |
| **DWD** | `dwd_finance` | 5 | ORC + SNAPPY | ODS ETL 转换 | 清洗标准化，衍生字段 |
| **DWS** | `dws_finance` | 2 | ORC + SNAPPY | DWD 聚合 | 分钟级 OHLC + 统计 |
| **ADS** | `ads_finance` | 2 | ORC + SNAPPY | DWS 汇总 | 直接供大屏/API 快速查询 |

**ODS 表字段（每张表 12 列 + dt 分区）：**

`symbol, timestamp, price, volume, bid, ask, spread, direction, volatility, high, low, event_time` + `PARTITIONED BY (dt STRING)`

**DWD 表新增衍生字段（每张表 16 列 + dt 分区）：**

`symbol, event_time, price, volume, bid, ask, spread, spread_pct, direction, volatility, high, low, price_change, price_change_pct, mid_price, hour` + `PARTITIONED BY (dt STRING)`

**DWS 聚合表字段：**

`symbol, open_price, close_price, high_price, low_price, avg_price, total_volume, avg_spread, avg_spread_pct, avg_volatility, max_volatility, up_count, down_count, flat_count, tick_count, price_change, price_change_pct, minute` + `PARTITIONED BY (dt STRING)`

**ADS 大屏指标表：**

`symbol, latest_price, open_price, high_price, low_price, price_change, price_change_pct, total_volume, avg_volatility, max_drawdown, avg_spread, avg_spread_pct, sma_5, sma_10, sma_20, up_ratio, update_time` + `PARTITIONED BY (dt STRING)`

---

## 五、Elasticsearch 索引设计

### `finance_realtime_metrics`（逐笔行情指标）

| 字段 | 类型 | 格式 | 说明 |
|------|------|------|------|
| symbol | keyword | - | 品种代码，精确匹配+排序 |
| timestamp | date | `strict_date_optional_time\|\|yyyy-MM-dd HH:mm:ss\|\|epoch_millis` | 原始时间戳 |
| price | double | - | 成交价 |
| volume | double | - | 成交量 |
| bid | double | - | 买一价 |
| ask | double | - | 卖一价 |
| spread | double | - | 价差 |
| direction | keyword | - | UP/DOWN/FLAT |
| volatility | double | - | 波动率 |
| high | double | - | 最高价 |
| low | double | - | 最低价 |
| event_time | date | 同上 format | 事件时间（用于排序） |
| spread_pct | double | - | 价差比例% (Logstash 计算) |
| mid_price | double | - | 中间价 (Logstash 计算) |

**关键设计：**
- `dynamic_templates: [{strings_as_keyword}]` — 所有字符串字段自动映射为 keyword，避免 text 导致的排序报错
- `refresh_interval: 5s` — 近实时可见
- `1 shard, 0 replica` — 开发环境单节点

---

## 六、FastAPI 接口详情

| 端点 | 方法 | 参数 | 返回 | ES 查询逻辑 |
|------|------|------|------|-------------|
| `/health` | GET | - | `{status, elasticsearch, timestamp}` | `es.ping()` |
| `/api/symbols` | GET | - | 5 品种的名称/单位/颜色 | 无，从配置返回 |
| `/api/realtime/latest` | GET | - | 5 品种最新价+涨跌+委买委卖+波动 | 每品种 `size:1, sort:event_time desc, range:7d` |
| `/api/realtime/trend` | GET | `symbol=GOLD&minutes=4320` | ~800点价格时序+SMA | `sort:event_time asc, size:3000` |
| `/api/realtime/volume` | GET | - | 5 品种总成交量+笔数 | `aggs: sum(volume), value_count(symbol)` |
| `/api/realtime/risk` | GET | - | 5 品种波动率+最大回撤+价差 | `size:500, sort:event_time desc, range:7d` |
| `/api/realtime/aggregates` | GET | `window=1min&symbol=GOLD` | OHLC 聚合数据 | 从 `finance_realtime_agg` 索引查 |
| `/api/realtime/summary` | GET | - | 精简概览（价格+涨跌+回撤） | 组合 latest + risk |

**容错设计：**
- ES 不可达 → 自动切换到 `_generate_mock_*()` 系列函数，返回随机模拟数据
- ES 查询异常 → `try/except` 捕获，退回 mock
- 30 秒内连续失败 → 不再重试 ES（避免阻塞）

---

## 七、前端大屏组件详解

### 1. 标题栏 (`header`)
- **数据来源**: `GET /api/realtime/latest` 中的 `update_time`
- **状态灯**: ES 有数据 → 绿灯 "在线"；无数据 → 红灯 "离线"
- **刷新**: 每 3 秒全量刷新所有组件

### 2. 5 张行情卡片 (`priceCards`)
- **数据来源**: `GET /api/realtime/latest` → `data[]`
- **每张卡片展示**: 品种名 + 代码、最新价(大字+颜色)、涨跌幅(↑↓红绿)、Bid/Ask 挂单价、成交量
- **交互**: 点击卡片切换趋势图品种，选中卡片高亮+边框发光
- **颜色映射**: 黄金#FFD700 / 原油#FF6B35 / 白银#C0C0C0 / 铜#FF8C42 / 天然气#4FC3F7

### 3. 价格趋势图 (`mainChart`)
- **数据来源**: `GET /api/realtime/trend?symbol={sel}&minutes=4320`
- **图表类型**: ECharts 折线图
- **系列**: 价格(蓝)、SMA(5)(橙虚)、SMA(10)(紫虚)
- **交互**: 滚轮缩放 (dataZoom inside)、悬停 tooltip
- **数据量**: 最多 800 点（自动降采样）

### 4. 成交量对比图 (`volChart`)
- **数据来源**: `GET /api/realtime/volume`
- **图表类型**: ECharts 柱状图
- **每根柱子**: 品种色渐变，显示总成交量
- **Y 轴**: 自动格式化(K/M)

### 5. 风险雷达图 (`radarChart`)
- **数据来源**: `GET /api/realtime/risk`
- **图表类型**: ECharts 雷达图
- **三个维度**: 波动率×10000、最大回撤%、价差×100
- **5 条线**: 每个品种一条，对比展示

### 6. 风险监控面板 (`riskGrid`)
- **数据来源**: `GET /api/realtime/risk`
- **5 张指标卡片**: 最大回撤%(大字)、波动率、价差、tick 计数

---

## 八、验证命令速查

```bash
# 一键全链路验证
bash scripts/check_all.sh

# 分组件验证
docker compose ps                                              # 容器状态
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092  # Kafka Topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gold_tick --max-messages 3  # Kafka 消息
docker exec hadoop-namenode hdfs dfs -ls -R /user/finance/raw/gold/ | head -10  # HDFS 文件
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM ods_finance.gold_tick LIMIT 5;"  # Hive 查询
curl http://localhost:9200/finance_realtime_metrics/_count      # ES 数据量
curl http://localhost:9200/finance_realtime_metrics/_search?size=3  # ES 数据样本
curl http://localhost:8080/json/ | python3 -m json.tool         # Spark 状态
curl http://localhost:8000/api/realtime/latest | python3 -m json.tool  # API 行情
```

---

## 九、启动与停止

```bash
# 演示模式一键重启（从头来过，100% 可靠）
bash scripts/demo_restart.sh

# 手动控制
cd docker && docker compose up -d          # 启动集群
cd .. && bash scripts/init_all.sh          # 初始化
bash scripts/start_pipeline.sh --loop       # 启动管道
bash scripts/stop_pipeline.sh               # 停止管道
cd docker && docker compose down            # 停止集群

# 前端大屏
cd output && python3 -m http.server 3000 &  # 启动 HTTP 服务器
# 浏览器: http://192.168.128.130:3000/finance_dashboard.html
```

---

## 十、关键技术决策

| 决策 | 原因 |
|------|------|
| Spark 不启用 Hive Support | 避免 hive-site.xml 路径依赖，Spark 直接写 JSON 到 HDFS |
| Hive ODS 用外部表 + JsonSerDe | 数据归 HDFS 管，Hive 只映射不拥有，数据不会被 Hive 误删 |
| Logstash 负责 Kafka→ES | Spark 容器装不了 elasticsearch 包，Logstash 自带 ES 输出插件 |
| ES dynamic_templates strings→keyword | 防止 Logstash 自动创建 text 类型字段导致排序报错 |
| Spark checkpoint 放本地而非 HDFS | 避免 HDFS 单点故障影响 checkpoint，依赖更少 |
| Spark 不用 --packages，预下载 JAR | 容器 /home/spark/.ivy2 写权限不稳定 |
| demo_restart.sh 先建 ES 索引再启 Logstash | 防止 Logstash 抢先创建错误的 dynamic mapping |
