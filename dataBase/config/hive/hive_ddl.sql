-- ============================================================================
-- 金融实时大数据分析系统 - Hive 数据仓库 DDL
-- 四层建模：ODS → DWD → DWS → ADS
--
-- 数据资产：
--   COPPER  - 铜期货    (copper_tick)
--   GAS     - 天然气期货 (gas_tick)
--   GOLD    - 黄金期货   (gold_tick)
--   OIL     - 原油期货   (oil_tick)
--   SILVER  - 白银期货   (silver_tick)
--
-- 数据流向:
--   Spark Streaming → HDFS JSON 文件 → ODS 外部表 (JsonSerDe)
--   ODS → DWD (清洗/标准化/衍生字段)
--   DWD → DWS (分钟级聚合 OHLC)
--   DWS → ADS (大屏应用指标)
--
-- 可重复执行: 所有 CREATE 语句均使用 IF NOT EXISTS
-- ============================================================================

-- ==============================
-- 创建数据库
-- ==============================
CREATE DATABASE IF NOT EXISTS ods_finance;
CREATE DATABASE IF NOT EXISTS dwd_finance;
CREATE DATABASE IF NOT EXISTS dws_finance;
CREATE DATABASE IF NOT EXISTS ads_finance;

-- ============================================================================
-- ODS 层：原始数据层 (Operational Data Store)
--
-- 说明：
--   - 外部表，数据存储在 HDFS /user/finance/raw/{symbol}/ 下
--   - Spark Streaming 按 symbol 写入各自目录，按 dt 分区
--   - 使用 JsonSerDe 解析 JSON 文件
--   - 数据写入后需执行 MSCK REPAIR TABLE 或 ALTER TABLE ADD PARTITION 来识别新分区
--
-- 路径对齐:
--   Spark write_to_hdfs 写入: /user/finance/raw/{symbol}/dt={yyyy-MM-dd}/*.json
--   Hive LOCATION 指向:    /user/finance/raw/{symbol}
-- ============================================================================

USE ods_finance;

-- --------------------------------------
-- 黄金期货 ODS 表
-- --------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_finance.gold_tick (
    symbol       STRING    COMMENT '品种代码',
    `timestamp`  STRING    COMMENT '原始时间戳 yyyy-MM-dd HH:mm:ss',
    price        DOUBLE    COMMENT '最新成交价',
    volume       DOUBLE    COMMENT '成交量',
    bid          DOUBLE    COMMENT '买一价',
    ask          DOUBLE    COMMENT '卖一价',
    spread       DOUBLE    COMMENT '买卖价差',
    direction    STRING    COMMENT '价格方向 UP/DOWN/FLAT',
    volatility   DOUBLE    COMMENT '瞬时波动率',
    high         DOUBLE    COMMENT '当日最高价',
    low          DOUBLE    COMMENT '当日最低价',
    event_time   STRING    COMMENT '事件时间'
)
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/finance/raw/gold';

-- --------------------------------------
-- 原油期货 ODS 表
-- --------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_finance.oil_tick (
    symbol       STRING,
    `timestamp`  STRING,
    price        DOUBLE,
    volume       DOUBLE,
    bid          DOUBLE,
    ask          DOUBLE,
    spread       DOUBLE,
    direction    STRING,
    volatility   DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    event_time   STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/finance/raw/oil';

-- --------------------------------------
-- 白银期货 ODS 表
-- --------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_finance.silver_tick (
    symbol       STRING,
    `timestamp`  STRING,
    price        DOUBLE,
    volume       DOUBLE,
    bid          DOUBLE,
    ask          DOUBLE,
    spread       DOUBLE,
    direction    STRING,
    volatility   DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    event_time   STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/finance/raw/silver';

-- --------------------------------------
-- 铜期货 ODS 表
-- --------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_finance.copper_tick (
    symbol       STRING,
    `timestamp`  STRING,
    price        DOUBLE,
    volume       DOUBLE,
    bid          DOUBLE,
    ask          DOUBLE,
    spread       DOUBLE,
    direction    STRING,
    volatility   DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    event_time   STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/finance/raw/copper';

-- --------------------------------------
-- 天然气期货 ODS 表
-- --------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_finance.gas_tick (
    symbol       STRING,
    `timestamp`  STRING,
    price        DOUBLE,
    volume       DOUBLE,
    bid          DOUBLE,
    ask          DOUBLE,
    spread       DOUBLE,
    direction    STRING,
    volatility   DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    event_time   STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/finance/raw/gas';


-- ============================================================================
-- DWD 层：明细数据层 (Data Warehouse Detail)
--
-- 说明：
--   - 内部表，从 ODS 清洗转换后写入
--   - 字段标准化：时间戳统一为 TIMESTAMP 类型、增加衍生字段
--   - 存储格式：ORC + Snappy 压缩，查询性能最优
--   - 按 dt 分区，与 ODS 保持一致
-- ============================================================================

USE dwd_finance;

-- --------------------------------------
-- 黄金 DWD 明细表 (带完整注释)
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dwd_finance.gold_tick_detail (
    symbol          STRING    COMMENT '品种代码',
    event_time      TIMESTAMP COMMENT '标准化事件时间',
    price           DOUBLE    COMMENT '成交价',
    volume          DOUBLE    COMMENT '成交量',
    bid             DOUBLE    COMMENT '买一价',
    ask             DOUBLE    COMMENT '卖一价',
    spread          DOUBLE    COMMENT '买卖价差',
    spread_pct      DOUBLE    COMMENT '价差比例 (%)',
    direction       STRING    COMMENT '价格方向 UP/DOWN/FLAT',
    volatility      DOUBLE    COMMENT '瞬时波动率',
    high            DOUBLE    COMMENT '最高价',
    low             DOUBLE    COMMENT '最低价',
    price_change    DOUBLE    COMMENT '价格变动额 (当前-上一笔)',
    price_change_pct DOUBLE  COMMENT '价格变动百分比',
    mid_price       DOUBLE    COMMENT '中间价 (bid+ask)/2',
    hour            INT       COMMENT '小时 (0-23)'
)
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 原油 DWD 明细表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dwd_finance.oil_tick_detail (
    symbol          STRING,
    event_time      TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    spread          DOUBLE,
    spread_pct      DOUBLE,
    direction       STRING,
    volatility      DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    price_change    DOUBLE,
    price_change_pct DOUBLE,
    mid_price       DOUBLE,
    hour            INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 白银 DWD 明细表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dwd_finance.silver_tick_detail (
    symbol          STRING,
    event_time      TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    spread          DOUBLE,
    spread_pct      DOUBLE,
    direction       STRING,
    volatility      DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    price_change    DOUBLE,
    price_change_pct DOUBLE,
    mid_price       DOUBLE,
    hour            INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 铜 DWD 明细表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dwd_finance.copper_tick_detail (
    symbol          STRING,
    event_time      TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    spread          DOUBLE,
    spread_pct      DOUBLE,
    direction       STRING,
    volatility      DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    price_change    DOUBLE,
    price_change_pct DOUBLE,
    mid_price       DOUBLE,
    hour            INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 天然气 DWD 明细表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dwd_finance.gas_tick_detail (
    symbol          STRING,
    event_time      TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    spread          DOUBLE,
    spread_pct      DOUBLE,
    direction       STRING,
    volatility      DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    price_change    DOUBLE,
    price_change_pct DOUBLE,
    mid_price       DOUBLE,
    hour            INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ============================================================================
-- DWS 层：汇总统计层 (Data Warehouse Summary)
--
-- 说明：
--   - 分钟级窗口聚合：OHLC 四价、成交量、波动率统计
--   - 数据来源：Spark Streaming 窗口聚合写入 HDFS
--   - 聚合窗口：1分钟 / 5分钟
-- ============================================================================

USE dws_finance;

-- --------------------------------------
-- 1分钟聚合表 (5个品种共用结构)
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dws_finance.tick_1min_agg (
    symbol           STRING    COMMENT '品种代码',
    open_price       DOUBLE    COMMENT '开盘价 (窗口首笔)',
    close_price      DOUBLE    COMMENT '收盘价 (窗口末笔)',
    high_price       DOUBLE    COMMENT '最高价',
    low_price        DOUBLE    COMMENT '最低价',
    avg_price        DOUBLE    COMMENT '均价',
    total_volume     DOUBLE    COMMENT '总成交量',
    avg_spread       DOUBLE    COMMENT '平均价差',
    avg_spread_pct   DOUBLE    COMMENT '平均价差比例 (%)',
    avg_volatility   DOUBLE    COMMENT '平均波动率',
    max_volatility   DOUBLE    COMMENT '最大波动率',
    up_count         INT       COMMENT '上涨笔数',
    down_count       INT       COMMENT '下跌笔数',
    flat_count       INT       COMMENT '持平笔数',
    tick_count       INT       COMMENT '总笔数',
    price_change     DOUBLE    COMMENT '涨跌额 (close-open)',
    price_change_pct DOUBLE    COMMENT '涨跌幅 (%)',
    minute           STRING    COMMENT '分钟标识 HH:mm'
)
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 5分钟聚合表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dws_finance.tick_5min_agg (
    symbol           STRING,
    open_price       DOUBLE,
    close_price      DOUBLE,
    high_price       DOUBLE,
    low_price        DOUBLE,
    avg_price        DOUBLE,
    total_volume     DOUBLE,
    avg_spread       DOUBLE,
    avg_spread_pct   DOUBLE,
    avg_volatility   DOUBLE,
    max_volatility   DOUBLE,
    up_count         INT,
    down_count       INT,
    flat_count       INT,
    tick_count       INT,
    price_change     DOUBLE,
    price_change_pct DOUBLE,
    minute           STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ============================================================================
-- ADS 层：应用数据层 (Application Data Store)
--
-- 说明：
--   - 直接供前端大屏和 API 查询使用
--   - 汇总多个品种的核心指标
-- ============================================================================

USE ads_finance;

-- --------------------------------------
-- 大屏指标汇总表
-- --------------------------------------
CREATE TABLE IF NOT EXISTS ads_finance.dashboard_metrics (
    symbol           STRING    COMMENT '品种代码',
    latest_price     DOUBLE    COMMENT '最新价格',
    open_price       DOUBLE    COMMENT '当日开盘价',
    high_price       DOUBLE    COMMENT '当日最高价',
    low_price        DOUBLE    COMMENT '当日最低价',
    price_change     DOUBLE    COMMENT '当日涨跌额',
    price_change_pct DOUBLE    COMMENT '当日涨跌幅 (%)',
    total_volume     DOUBLE    COMMENT '当日累计成交量',
    avg_volatility   DOUBLE    COMMENT '平均波动率',
    max_drawdown     DOUBLE    COMMENT '最大回撤 (%)',
    avg_spread       DOUBLE    COMMENT '平均价差',
    avg_spread_pct   DOUBLE    COMMENT '平均价差比例 (%)',
    sma_5            DOUBLE    COMMENT '5周期简单移动平均',
    sma_10           DOUBLE    COMMENT '10周期简单移动平均',
    sma_20           DOUBLE    COMMENT '20周期简单移动平均',
    up_ratio         DOUBLE    COMMENT '上涨比例',
    update_time      STRING    COMMENT '数据更新时间'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- --------------------------------------
-- 各品种最新快照表 (供快速查询)
-- --------------------------------------
CREATE TABLE IF NOT EXISTS ads_finance.latest_snapshot (
    symbol           STRING    COMMENT '品种代码',
    price            DOUBLE    COMMENT '最新价',
    volume           DOUBLE    COMMENT '成交量',
    direction        STRING    COMMENT '方向',
    volatility       DOUBLE    COMMENT '波动率',
    spread           DOUBLE    COMMENT '价差',
    spread_pct       DOUBLE    COMMENT '价差比例 (%)',
    price_change_pct DOUBLE    COMMENT '涨跌幅 (%)',
    bid              DOUBLE    COMMENT '买价',
    ask              DOUBLE    COMMENT '卖价',
    event_time       STRING    COMMENT '事件时间',
    dt               STRING    COMMENT '日期'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
