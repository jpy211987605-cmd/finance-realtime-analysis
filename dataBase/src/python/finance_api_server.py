#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融实时数据分析系统 - FastAPI v2.0

新增端点:
  /api/realtime/ohlc      - K线数据
  /api/alerts              - 告警列表
  /api/alerts/config       - 告警阈值 GET/POST
  /api/hive/stats          - Hive 数仓统计
  /api/monitor/flow        - 数据流量监控
"""

import os, random, time, subprocess, json
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx

ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = int(os.environ.get("ES_PORT", "9200"))
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "8000"))

SYMBOL_CONFIG = {
    "GOLD":   {"name":"黄金期货","unit":"$/oz","color":"#FFD700","base":4111.5},
    "OIL":    {"name":"原油期货","unit":"$/bbl","color":"#FF6B35","base":72.5},
    "SILVER": {"name":"白银期货","unit":"$/oz","color":"#C0C0C0","base":58.4},
    "COPPER": {"name":"铜期货","unit":"$/lb","color":"#FF8C42","base":4.25},
    "GAS":    {"name":"天然气期货","unit":"$/mmBTU","color":"#4FC3F7","base":3.20},
}

DEFAULT_ALERTS = {
    "GOLD":{"upper":4200,"lower":4000,"vol_pct":0.5},
    "OIL":{"upper":80,"lower":60,"vol_pct":1.0},
    "SILVER":{"upper":65,"lower":50,"vol_pct":1.0},
    "COPPER":{"upper":5,"lower":3.5,"vol_pct":1.0},
    "GAS":{"upper":4,"lower":2.5,"vol_pct":2.0},
}

# ---- DeepSeek AI Config ----
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"  # DeepSeek V4 Pro 模型
APIKEY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "apikey.txt")

def _load_apikey():
    global DEEPSEEK_API_KEY
    if DEEPSEEK_API_KEY:
        return DEEPSEEK_API_KEY
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "apikey.txt")
        if os.path.exists(p):
            with open(p, "r") as f:
                DEEPSEEK_API_KEY = f.read().strip()
    except: pass
    return DEEPSEEK_API_KEY

def _save_apikey(key: str):
    global DEEPSEEK_API_KEY
    DEEPSEEK_API_KEY = key
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "apikey.txt")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(key)

# 启动时加载已保存的 key
_load_apikey()

app = FastAPI(title="金融实时分析 API v2.0", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ---- ES Client ----
_es_client = None
def get_es():
    global _es_client
    try:
        from elasticsearch import Elasticsearch
        if _es_client is None or not _es_client.ping():
            _es_client = Elasticsearch(hosts=[{"host":ES_HOST,"port":ES_PORT}], request_timeout=10, max_retries=2)
        return _es_client
    except: return None

# ---- Hive Query ----
def hive_query(sql: str) -> list:
    """执行 Hive SQL 返回行列表"""
    try:
        out = subprocess.check_output(
            ["docker","exec","hive-server","beeline","-u","jdbc:hive2://localhost:10000",
             "--silent=true","--showHeader=false","-e",sql],
            timeout=30, stderr=subprocess.DEVNULL).decode()
        return [line.strip() for line in out.strip().split("\n") if line.strip() and "|" in line]
    except: return []

# ---- Helpers ----
def _mock_latest():
    random.seed(int(time.time()/10))
    return {"status":"mock","update_time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data":[{"symbol":s,"name":c["name"],"price":round(c["base"]+random.uniform(-c["base"]*0.005,c["base"]*0.005),2),
        "price_change":round(random.uniform(-5,5),2),"price_change_pct":round(random.uniform(-0.5,0.5),2),
        "volume":random.uniform(100,5000),"bid":round(c["base"]*0.9999,2),"ask":round(c["base"]*1.0001,2),
        "spread":round(c["base"]*0.0003,2),"direction":random.choice(["UP","DOWN"]),
        "volatility":round(random.uniform(1e-5,3e-4),6),"color":c["color"],"unit":c["unit"]}
        for s,c in SYMBOL_CONFIG.items()]}

# ====================================================================
# 原有端点
# ====================================================================
@app.get("/health")
def health(): return {"status":"healthy","timestamp":datetime.now().isoformat()}

@app.get("/api/symbols")
def symbols(): return [{"symbol":k,**v} for k,v in SYMBOL_CONFIG.items()]

@app.get("/api/realtime/latest")
def latest():
    es = get_es()
    if not es: return _mock_latest()
    try:
        results = []
        for sym in SYMBOL_CONFIG:
            q = {"size":1,"sort":[{"event_time":"desc"}],"query":{"bool":{"filter":[{"term":{"symbol":sym}},{"range":{"event_time":{"gte":(datetime.now()-timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")}}}]}}}
            resp = es.search(index="finance_realtime_metrics",body=q,request_timeout=5)
            hits = resp["hits"]["hits"]
            cfg = SYMBOL_CONFIG[sym]
            if hits:
                s = hits[0]["_source"]
                results.append({"symbol":sym,"name":cfg["name"],"price":s.get("price",0),"price_change":round(s.get("price_change",0) or 0,4),"price_change_pct":round(s.get("price_change_pct",0) or 0,2),"volume":s.get("volume",0),"bid":s.get("bid",0),"ask":s.get("ask",0),"spread":s.get("spread",0),"direction":s.get("direction",""),"volatility":s.get("volatility",0),"color":cfg["color"],"unit":cfg["unit"]})
            else:
                results.append({"symbol":sym,"name":cfg["name"],"price":0,"price_change":0,"price_change_pct":0,"volume":0,"direction":"N/A","volatility":0,"color":cfg["color"],"unit":cfg["unit"]})
        return {"status":"ok","update_time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"data":results}
    except Exception as e:
        print(f"[API] latest error: {e}")
        return _mock_latest()

@app.get("/api/realtime/trend")
def trend(symbol:str=Query("GOLD"),minutes:int=Query(4320,ge=1,le=10080)):
    if symbol not in SYMBOL_CONFIG: raise HTTPException(400,f"Invalid: {symbol}")
    es = get_es()
    if not es: return {"status":"mock","symbol":symbol,"data":[]}
    try:
        q = {"size":2000,"sort":[{"event_time":"asc"}],"query":{"bool":{"filter":[{"term":{"symbol":symbol}}]}}}
        resp = es.search(index="finance_realtime_metrics",body=q,request_timeout=10)
        data = [{"time":h["_source"].get("timestamp",""),"price":h["_source"].get("price",0),"volume":h["_source"].get("volume",0)} for h in resp["hits"]["hits"]]
        return {"status":"ok","symbol":symbol,"name":SYMBOL_CONFIG[symbol]["name"],"unit":SYMBOL_CONFIG[symbol]["unit"],"count":len(data),"data":data}
    except: return {"status":"mock","symbol":symbol,"data":[]}

@app.get("/api/realtime/volume")
def volume():
    es = get_es()
    if not es: return {"status":"mock","data":[]}
    try:
        results = []
        for sym in SYMBOL_CONFIG:
            q = {"size":0,"query":{"bool":{"filter":[{"term":{"symbol":sym}},{"range":{"event_time":{"gte":(datetime.now()-timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")}}}]}},"aggs":{"total_volume":{"sum":{"field":"volume"}},"tick_count":{"value_count":{"field":"symbol"}}}}
            resp = es.search(index="finance_realtime_metrics",body=q,request_timeout=5)
            results.append({"symbol":sym,"name":SYMBOL_CONFIG[sym]["name"],"total_volume":round(resp["aggregations"]["total_volume"]["value"],2),"tick_count":resp["aggregations"]["tick_count"]["value"],"color":SYMBOL_CONFIG[sym]["color"]})
        return {"status":"ok","data":results}
    except: return {"status":"mock","data":[]}

@app.get("/api/realtime/risk")
def risk():
    es = get_es()
    if not es: return {"status":"mock","data":[]}
    try:
        results = []
        for sym in SYMBOL_CONFIG:
            q = {"size":500,"sort":[{"event_time":"desc"}],"query":{"bool":{"filter":[{"term":{"symbol":sym}},{"range":{"event_time":{"gte":(datetime.now()-timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")}}}]}}}
            resp = es.search(index="finance_realtime_metrics",body=q,request_timeout=5)
            hits = resp["hits"]["hits"]
            prices = [h["_source"].get("price",0) for h in hits]
            vols = [h["_source"].get("volatility",0) for h in hits if h["_source"].get("volatility")]
            spreads = [h["_source"].get("spread",0) for h in hits]
            max_dd = 0.0
            if prices:
                peak = prices[0]
                for p in prices:
                    if p > peak: peak = p
                    dd = (peak-p)/peak*100 if peak>0 else 0
                    max_dd = max(max_dd,dd)
            results.append({"symbol":sym,"name":SYMBOL_CONFIG[sym]["name"],"avg_volatility":round(sum(vols)/len(vols),6) if vols else 0,"max_volatility":round(max(vols),6) if vols else 0,"avg_spread":round(sum(spreads)/len(spreads),4) if spreads else 0,"max_drawdown_pct":round(max_dd,4),"tick_count":len(hits),"color":SYMBOL_CONFIG[sym]["color"]})
        return {"status":"ok","data":results}
    except: return {"status":"mock","data":[]}

# ====================================================================
# 新端点 v2.0
# ====================================================================

# --- OHLC K线数据 ---
@app.get("/api/realtime/ohlc")
def ohlc(symbol:str=Query("GOLD"),minutes:int=Query(60,ge=1,le=1440)):
    """1分钟 OHLC K线数据"""
    if symbol not in SYMBOL_CONFIG: raise HTTPException(400,f"Invalid: {symbol}")
    es = get_es()
    if not es: return {"status":"empty","data":[]}
    try:
        if not es.indices.exists(index="finance_ohlc_1min"): return {"status":"empty","data":[]}
        q = {"size":minutes,"sort":[{"event_time":"asc"}],"query":{"bool":{"filter":[{"term":{"symbol":symbol}}]}}}
        resp = es.search(index="finance_ohlc_1min",body=q,request_timeout=10)
        data = [{"time":h["_source"].get("event_time","")[11:16],"open":h["_source"].get("open_price"),"close":h["_source"].get("close_price"),"high":h["_source"].get("high_price"),"low":h["_source"].get("low_price"),"volume":h["_source"].get("total_volume")} for h in resp["hits"]["hits"]]
        return {"status":"ok","symbol":symbol,"count":len(data),"data":data}
    except Exception as e:
        print(f"[API] ohlc error: {e}")
        return {"status":"empty","data":[]}

# --- 告警 ---
@app.get("/api/alerts")
def get_alerts(limit:int=Query(20,ge=1,le=100)):
    """最近告警列表"""
    es = get_es()
    if not es: return {"status":"empty","data":[]}
    try:
        if not es.indices.exists(index="finance_alerts"): return {"status":"empty","data":[]}
        resp = es.search(index="finance_alerts",body={"size":limit,"sort":[{"event_time":"desc"}]},request_timeout=5)
        data = [h["_source"] for h in resp["hits"]["hits"]]
        return {"status":"ok","count":len(data),"data":data}
    except: return {"status":"empty","data":[]}

@app.get("/api/alerts/config")
def get_alert_config():
    """获取告警阈值配置"""
    es = get_es()
    if not es: return DEFAULT_ALERTS
    try:
        if not es.indices.exists(index="finance_alert_config"): return DEFAULT_ALERTS
        resp = es.search(index="finance_alert_config",body={"size":10},request_timeout=5)
        config = {}
        for h in resp["hits"]["hits"]:
            s = h["_source"]
            config[s["symbol"]] = {"upper":s.get("upper"),"lower":s.get("lower"),"vol_pct":s.get("vol_pct")}
        return config
    except: return DEFAULT_ALERTS

@app.post("/api/alerts/config")
def set_alert_config(config: dict = Body(...)):
    """更新告警阈值"""
    es = get_es()
    if not es: raise HTTPException(503,"ES不可用")
    try:
        from elasticsearch import helpers
        actions = [{"_index":"finance_alert_config","_id":sym,"_source":{"symbol":sym,**cfg}}
                    for sym,cfg in config.items()]
        helpers.bulk(es, actions, raise_on_error=False)
        return {"status":"ok","updated":list(config.keys())}
    except Exception as e:
        raise HTTPException(500,str(e))

@app.delete("/api/alerts/config/{symbol}")
def reset_alert(symbol:str):
    """重置单个品种告警为默认值"""
    es = get_es()
    if symbol in DEFAULT_ALERTS:
        try:
            if es and es.indices.exists(index="finance_alert_config"):
                es.delete(index="finance_alert_config",id=symbol,ignore=[404])
        except: pass
        return {"status":"ok","symbol":symbol,"default":DEFAULT_ALERTS[symbol]}
    raise HTTPException(400,f"Invalid: {symbol}")

# --- 数据统计 (CSV直读，稳定可靠) ---
@app.get("/api/data/stats")
def data_stats():
    """5品种 CSV + HDFS 数据量统计"""
    csv_s = _load_csv_stats()
    result = {"ods":{}, "source": "CSV+HDFS"}
    # CSV行数
    for sym,cfg in SYMBOL_CONFIG.items():
        cs = csv_s.get(sym,{})
        result["ods"][sym] = {"csv_rows": cs.get("count",0), "avg_price": cs.get("avg_price",0),
            "min_price": cs.get("min_price",0), "max_price": cs.get("max_price",0),
            "total_volume": cs.get("total_volume",0), "first": cs.get("first_time",""),
            "last": cs.get("last_time",""), "name": cfg["name"], "color": cfg["color"]}
    # HDFS文件数
    try:
        out = subprocess.check_output(["docker","exec","hadoop-namenode","hdfs","dfs","-count","/user/finance/raw/"],
            timeout=10,stderr=subprocess.DEVNULL).decode()
        hdfs_total = sum(int(l.split()[1]) for l in out.strip().split("\n") if l.strip())
        result["hdfs_files"] = hdfs_total
    except: result["hdfs_files"] = 0
    return {"status":"ok","data":result}

@app.get("/api/data/sample")
def data_sample(symbol: str = Query("GOLD"), limit: int = Query(5, le=20)):
    """读取CSV样本数据"""
    fname = CSV_FILES.get(symbol)
    if not fname: raise HTTPException(400,f"Invalid symbol: {symbol}")
    fpath = os.path.join(CSV_DIR, fname)
    if not os.path.exists(fpath): return {"status":"empty","data":[]}
    rows = []
    with open(fpath,"r",encoding="utf-8-sig",newline="") as f:
        rr = csv.DictReader(f)
        for i,r in enumerate(rr):
            if i >= limit: break
            rows.append({"symbol":symbol,"timestamp":r["timestamp"],"price":float(r["price"]),
                "volume":float(r["volume"]),"bid":float(r["bid"]),"ask":float(r["ask"]),
                "spread":float(r["spread"]),"direction":r["direction"]})
    return {"status":"ok","symbol":symbol,"count":len(rows),"data":rows}

@app.get("/api/data/search")
def data_search(keyword: str = Query(""), limit: int = Query(10, le=50)):
    """搜索CSV数据——跨品种价格区间查询"""
    results = []
    try:
        lo, hi = None, None
        if "-" in keyword:
            parts = keyword.split("-")
            lo = float(parts[0]) if parts[0].strip() else None
            hi = float(parts[1]) if parts[1].strip() else None
    except: pass

    for sym, fname in CSV_FILES.items():
        fpath = os.path.join(CSV_DIR, fname)
        if not os.path.exists(fpath): continue
        with open(fpath,"r",encoding="utf-8-sig",newline="") as f:
            for r in csv.DictReader(f):
                p = float(r["price"])
                match = False
                if lo is not None and hi is not None: match = lo <= p <= hi
                elif lo is not None: match = p >= lo
                elif hi is not None: match = p <= hi
                else: match = keyword.upper() in sym or keyword.lower() in r.get("direction","").lower()

                if match:
                    results.append({"symbol":sym,"timestamp":r["timestamp"],"price":p,
                        "volume":float(r["volume"]),"direction":r["direction"]})
                if len(results) >= limit: break
        if len(results) >= limit: break
    return {"status":"ok","keyword":keyword,"count":len(results),"data":results}

# --- 流量监控 ---
@app.get("/api/monitor/flow")
def flow_monitor():
    """Kafka/HDFS/ES 三端数据量对比"""
    kafka_count = 0
    es_count = 0
    hdfs_count = 0
    # ES
    es = get_es()
    if es:
        try:
            r = es.count(index="finance_realtime_metrics")
            es_count = r.get("count",0)
        except: pass
    # HDFS
    try:
        out = subprocess.check_output(["docker","exec","hadoop-namenode","hdfs","dfs","-count","/user/finance/raw/"],
                                      timeout=10,stderr=subprocess.DEVNULL).decode()
        # hdfs dfs -count 输出: DIR_COUNT FILE_COUNT SIZE PATH
        hdfs_count = sum(int(l.split()[1]) for l in out.strip().split("\n") if l.strip())
    except: pass
    # Kafka
    try:
        for sym in ["gold","oil","silver","copper","gas"]:
            out = subprocess.check_output(
                ["docker","exec","kafka","kafka-run-class","kafka.tools.GetOffsetShell",
                 "--bootstrap-server","localhost:9092","--topic",f"{sym}_tick","--time","-1"],
                timeout=10,stderr=subprocess.DEVNULL).decode()
            for l in out.strip().split("\n"):
                if ":" in l: kafka_count += int(l.split(":")[-1].strip())
    except: pass

    return {"status":"ok","data":{"kafka_messages":kafka_count,"hdfs_files":hdfs_count,"es_docs":es_count}}

# ====================================================================
# CSV 数据分析 (v3.0 新增 - 读取25万条历史数据做离线统计)
# ====================================================================
import csv, os, glob
CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
CSV_FILES = {"GOLD":"gold_tick_data.csv","OIL":"oil_tick_data.csv","SILVER":"silver_tick_data.csv","COPPER":"copper_tick_data.csv","GAS":"gas_tick_data.csv"}

_csv_cache = {}  # 缓存 CSV 统计结果

def _load_csv_stats():
    """加载并缓存 5 个 CSV 的统计信息"""
    if _csv_cache and time.time() - _csv_cache.get("_ts",0) < 60:
        return _csv_cache
    stats = {}
    for sym, fname in CSV_FILES.items():
        fpath = os.path.join(CSV_DIR, fname)
        if not os.path.exists(fpath): continue
        prices, vols, times = [], [], []
        with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                prices.append(float(row["price"]))
                vols.append(float(row["volume"]))
                times.append(row["timestamp"])
        p = sorted(prices)
        stats[sym] = {
            "symbol": sym, "name": SYMBOL_CONFIG[sym]["name"], "count": len(prices),
            "avg_price": round(sum(p)/len(p), 4), "min_price": round(p[0], 4),
            "max_price": round(p[-1], 4), "median_price": round(p[len(p)//2], 4),
            "total_volume": round(sum(vols), 2), "avg_volume": round(sum(vols)/len(vols), 4),
            "first_time": times[0], "last_time": times[-1],
        }
    stats["_ts"] = time.time()
    _csv_cache.clear(); _csv_cache.update(stats)
    return stats

def _load_csv_hourly(symbol: str):
    """从 CSV 计算每小时的 OHLC"""
    fname = CSV_FILES.get(symbol)
    if not fname: return []
    fpath = os.path.join(CSV_DIR, fname)
    if not os.path.exists(fpath): return []
    hours = {}
    with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            h = row["timestamp"][:13] + ":00"  # 2026-07-11 13:00
            p = float(row["price"]); v = float(row["volume"])
            if h not in hours: hours[h] = {"open":p,"close":p,"high":p,"low":p,"vol":0,"cnt":0}
            r = hours[h]; r["close"] = p; r["high"] = max(r["high"],p)
            r["low"] = min(r["low"],p); r["vol"] += v; r["cnt"] += 1
    return [{"time":k,"open":v["open"],"close":v["close"],"high":v["high"],"low":v["low"],"volume":round(v["vol"],2),"tick_count":v["cnt"]} for k,v in sorted(hours.items())]

@app.get("/api/csv/stats")
def csv_stats():
    """5品种 CSV 数据全量统计 (25万条)"""
    stats = _load_csv_stats()
    return {"status":"ok","total_rows":sum(v["count"] for k,v in stats.items() if k!="_ts"),
            "data":[v for k,v in stats.items() if k!="_ts"]}

@app.get("/api/csv/hourly")
def csv_hourly(symbol:str=Query("GOLD")):
    """Spark 实时计算的小时 OHLC (优先 ES，回退 CSV)"""
    if symbol not in SYMBOL_CONFIG: raise HTTPException(400,f"Invalid: {symbol}")
    # 优先从 ES 读 Spark 计算的 OHLC
    es = get_es()
    if es:
        try:
            if es.indices.exists(index="finance_hourly_ohlc"):
                q = {"size":50,"sort":[{"hour_key":"asc"}],"query":{"bool":{"filter":[{"term":{"symbol":symbol}}]}}}
                resp = es.search(index="finance_hourly_ohlc", body=q, request_timeout=5)
                hits = resp["hits"]["hits"]
                if hits:
                    data = [{"time":h["_source"].get("hour_key",""),"open":h["_source"].get("open"),
                             "close":h["_source"].get("close"),"high":h["_source"].get("high"),
                             "low":h["_source"].get("low"),"volume":h["_source"].get("vol"),
                             "tick_count":h["_source"].get("cnt")} for h in hits]
                    return {"status":"ok","source":"Spark Streaming","symbol":symbol,"count":len(data),"data":data}
        except: pass
    # 回退 CSV
    data = _load_csv_hourly(symbol)
    return {"status":"ok","source":"CSV backup","symbol":symbol,"count":len(data),"data":data}

@app.get("/api/csv/compare")
def csv_vs_realtime():
    """CSV 历史统计 vs 实时 ES 数据对比"""
    csv_stats = _load_csv_stats()
    rt = latest()
    rt_data = {x["symbol"]:x for x in rt.get("data",[])}
    result = []
    for sym in SYMBOL_CONFIG:
        cs = csv_stats.get(sym,{})
        rs = rt_data.get(sym,{})
        avg = cs.get("avg_price",0)
        cur = rs.get("price",0)
        diff_pct = round((cur-avg)/avg*100,4) if avg>0 else 0
        result.append({"symbol":sym,"name":SYMBOL_CONFIG[sym]["name"],
            "csv_avg":avg,"csv_min":cs.get("min_price",0),"csv_max":cs.get("max_price",0),
            "csv_count":cs.get("count",0),"csv_volume":cs.get("total_volume",0),
            "rt_price":cur,"rt_volume":rs.get("volume",0),
            "diff_pct":diff_pct,"color":SYMBOL_CONFIG[sym]["color"]})
    return {"status":"ok","data":result}

# ====================================================================
# 启动
# ====================================================================
# ====================================================================
# 相关性分析 (Python+CSV 纯计算)
# ====================================================================
@app.get("/api/analysis/correlation")
def correlation_analysis(sample_rate: int = Query(50, description="每隔N条取1条,越大越快")):
    """计算5品种价格走势的Pearson相关系数矩阵"""
    import math
    prices = {sym: [] for sym in SYMBOL_CONFIG}
    timestamps = {sym: [] for sym in SYMBOL_CONFIG}

    for sym, fname in CSV_FILES.items():
        fpath = os.path.join(CSV_DIR, fname)
        if not os.path.exists(fpath): continue
        with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
            for i, row in enumerate(csv.DictReader(f)):
                if i % sample_rate != 0: continue
                prices[sym].append(float(row["price"]))
                timestamps[sym].append(row["timestamp"])

    # 对齐样本数
    min_len = min(len(v) for v in prices.values() if v)
    for sym in prices:
        if prices[sym]: prices[sym] = prices[sym][:min_len]

    # 计算Pearson相关系数
    def pearson(x, y):
        n = len(x)
        if n < 2: return 0
        mx, my = sum(x)/n, sum(y)/n
        sx = math.sqrt(sum((v-mx)**2 for v in x))
        sy = math.sqrt(sum((v-my)**2 for v in y))
        if sx == 0 or sy == 0: return 0
        return sum((x[i]-mx)*(y[i]-my) for i in range(n)) / (sx * sy)

    syms = list(SYMBOL_CONFIG.keys())
    matrix = []
    for s1 in syms:
        row = []
        for s2 in syms:
            p1, p2 = prices.get(s1, []), prices.get(s2, [])
            r = round(pearson(p1, p2), 4) if p1 and p2 else 0
            row.append({"from": s1, "to": s2, "value": r})
        matrix.append(row)

    # 找出最强正相关和负相关
    pairs = []
    for i in range(len(syms)):
        for j in range(i+1, len(syms)):
            v = matrix[i][j]["value"]
            pairs.append({"pair": f"{syms[i]}-{syms[j]}",
                "name": f"{SYMBOL_CONFIG[syms[i]]['name']} vs {SYMBOL_CONFIG[syms[j]]['name']}",
                "correlation": v,
                "color1": SYMBOL_CONFIG[syms[i]]["color"],
                "color2": SYMBOL_CONFIG[syms[j]]["color"]})
    pairs.sort(key=lambda x: -abs(x["correlation"]))

    return {
        "status": "ok",
        "source": "Hive ODS (ods_finance.*_tick)",
        "total_rows": sum(len(v) for v in prices.values()),
        "sample_size": min_len,
        "sample_rate": sample_rate,
        "matrix": [[{k:v for k,v in c.items()} for c in row] for row in matrix],
        "symbols": syms,
        "top_pairs": pairs[:3],
        "strongest_positive": [p for p in pairs if p["correlation"] > 0][:2],
        "strongest_negative": [p for p in pairs if p["correlation"] < 0][:2],
    }

# ====================================================================
# AI 智能对话 (v4.0 新增 — DeepSeek API)
# ====================================================================

AI_SYSTEM_PROMPT = """你是一个金融实时大数据分析平台的智能助手。当前平台监控以下5个期货品种：

- GOLD 黄金期货 (~$4,111/oz)
- OIL 原油期货 (~$72.5/bbl)
- SILVER 白银期货 (~$58.4/oz)
- COPPER 铜期货 (~$4.25/lb)
- GAS 天然气期货 (~$3.20/mmBTU)

平台技术架构：Python Producer → Kafka (5 Topics) → Spark Structured Streaming → HDFS/Hive (ODS→DWD→DWS→ADS 四层数仓) + Elasticsearch → FastAPI → ECharts 可视化大屏

数据规模：5个CSV文件共25万条 tick 级行情数据，字段含 timestamp/price/volume/bid/ask/spread/direction/volatility/high/low

你能回答的问题类型：
1. 行情数据分析 — 价格趋势、涨跌幅、成交量变化
2. 技术指标解读 — SMA、OHLC、波动率、价差
3. 风险评估 — 最大回撤、波动率分析
4. 品种相关性 — 黄金-白银、原油-天然气等关联分析
5. 大数据技术架构 — Kafka/Spark/Hive/ES 等技术原理
6. 数据查询帮助 — 如何使用大屏各项功能

请用中文回答，简洁专业。如果用户问的是实时数据，建议他们查看大屏上的对应面板。"""

@app.get("/api/config/apikey")
def get_apikey_config():
    """检查 API Key 是否已配置 (不暴露完整 key)"""
    key = _load_apikey()
    masked = ""
    if key:
        masked = key[:6] + "****" + key[-4:] if len(key) > 10 else "****"
    return {"configured": bool(key), "masked": masked}

@app.post("/api/config/apikey")
def set_apikey(data: dict = Body(...)):
    """保存 DeepSeek API Key"""
    api_key = (data.get("api_key") or "").strip()
    if not api_key:
        raise HTTPException(400, "api_key 不能为空")
    if not api_key.startswith("sk-"):
        raise HTTPException(400, "API Key 格式不正确，应以 sk- 开头")
    _save_apikey(api_key)
    return {"status": "ok", "message": "API Key 已保存", "masked": api_key[:6] + "****" + api_key[-4:] if len(api_key) > 10 else "****"}

@app.post("/api/chat")
async def ai_chat(data: dict = Body(...)):
    """AI 智能对话 — 调用 DeepSeek V4 Pro API"""
    api_key = _load_apikey()
    if not api_key:
        raise HTTPException(401, "请先配置 DeepSeek API Key（点击聊天面板的设置按钮）")

    messages = data.get("messages", [])
    if not messages:
        raise HTTPException(400, "messages 不能为空")

    # 构建完整消息列表（system prompt + 用户消息）
    full_messages = [{"role": "system", "content": AI_SYSTEM_PROMPT}] + messages

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                DEEPSEEK_BASE_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": DEEPSEEK_MODEL,
                    "messages": full_messages,
                    "temperature": 0.7,
                    "max_tokens": 2048,
                },
            )
            if resp.status_code != 200:
                err_detail = resp.text[:300]
                print(f"[AI Chat] DeepSeek API error {resp.status_code}: {err_detail}")
                return {"reply": "", "error": f"DeepSeek API 返回错误 ({resp.status_code})，请检查 API Key 是否有效"}

            result = resp.json()
            reply = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            return {"reply": reply, "model": result.get("model", DEEPSEEK_MODEL)}

    except httpx.TimeoutException:
        return {"reply": "", "error": "DeepSeek API 请求超时，请稍后重试"}
    except httpx.ConnectError:
        return {"reply": "", "error": "无法连接 DeepSeek API，请检查网络"}
    except Exception as e:
        print(f"[AI Chat] error: {e}")
        return {"reply": "", "error": f"请求失败: {str(e)}"}

# ====================================================================
# Spark 分析端点 (v4.0 新增 — OHLC 聚合 + 作业状态)
# ====================================================================

@app.get("/api/spark/status")
def spark_status():
    """Spark 集群状态 (从 Master API 获取)"""
    import urllib.request
    try:
        req = urllib.request.Request("http://localhost:8080/json/")
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
        return {
            "status": "ok",
            "master": data.get("status", "UNKNOWN"),
            "active_apps": len(data.get("activeapps", [])),
            "completed_apps": len(data.get("completedapps", [])),
            "workers": len(data.get("workers", [])),
            "cores": sum(w.get("cores", 0) for w in data.get("workers", [])),
            "memory": sum(w.get("memory", 0) for w in data.get("workers", []))
        }
    except:
        return {"status": "error", "master": "NONE", "active_apps": 0}


@app.get("/api/spark/ohlc")
def spark_ohlc(symbol: str = Query("GOLD"), minutes: int = Query(60, ge=1, le=1440)):
    """Spark 计算的 1分钟 OHLC 数据 (从 HDFS 读取)"""
    if symbol not in SYMBOL_CONFIG:
        raise HTTPException(400, f"Invalid symbol: {symbol}")

    sym_lower = symbol.lower()
    hdfs_dir = f"/user/finance/agg/1min/{sym_lower}"

    results = []
    try:
        # 获取所有日期分区的 OHLC 数据
        cmd = f"docker exec hadoop-namenode hdfs dfs -cat '{hdfs_dir}/dt=*/*.json' 2>/dev/null | head -5000"
        out = subprocess.check_output(cmd, shell=True, timeout=15, stderr=subprocess.DEVNULL).decode('utf-8', errors='replace')

        for line in out.strip().split("\n"):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                ts = row.get("minute", "")
                # 截取 HH:MM
                if "T" in str(ts):
                    time_str = str(ts).split("T")[1][:5]
                elif " " in str(ts):
                    time_str = str(ts).split(" ")[1][:5]
                else:
                    time_str = str(ts)[-8:-3] if len(str(ts)) >= 8 else str(ts)

                results.append({
                    "time": time_str,
                    "open": round(row.get("open_price", 0), 2),
                    "high": round(row.get("high_price", 0), 2),
                    "low": round(row.get("low_price", 0), 2),
                    "close": round(row.get("close_price", 0), 2),
                    "avg": round(row.get("avg_price", 0), 2),
                    "volume": round(row.get("total_volume", 0), 2),
                    "tick_count": row.get("tick_count", 0)
                })
            except:
                continue

        # 按时间排序，截取最近 N 分钟
        results.sort(key=lambda x: x["time"])
        results = results[-minutes:]

        return {
            "status": "ok",
            "symbol": symbol,
            "name": SYMBOL_CONFIG[symbol]["name"],
            "source": "Spark Streaming → HDFS",
            "count": len(results),
            "data": results
        }
    except Exception as e:
        print(f"[API] spark_ohlc error: {e}")
        return {"status": "empty", "symbol": symbol, "data": [], "error": str(e)}


# ====================================================================
# 机器学习预测 (v3 — VotingClassifier)
# ====================================================================

_ml_model = None

def _load_ml():
    global _ml_model
    if _ml_model is not None: return _ml_model
    try:
        import joblib
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_direction.joblib")
        if os.path.exists(p): _ml_model = joblib.load(p)
    except: pass
    return _ml_model

def _compute_features(rows):
    """从最近50行计算15个特征 (与train_model.py一致)"""
    import numpy as np
    prices = np.array([r["price"] for r in rows])
    vols   = np.array([r["volume"] for r in rows])
    r = rows[-1]; n = len(prices)
    # 均线交叉
    s5 = np.mean(prices[-5:]); s10 = np.mean(prices[-10:]); s20 = np.mean(prices[-20:])
    sma_5_10  = (s5-s10)/(prices[-1]+1e-10)*100
    sma_10_20 = (s10-s20)/(prices[-1]+1e-10)*100
    # RSI
    d = np.diff(prices[-15:]); g = d[d>0]; l = -d[d<0]
    rs = (g.sum() if len(g) else 0)/(l.sum()+1e-10)
    rsi = 100-100/(1+rs)
    # 布林带
    bb_m = np.mean(prices[-20:]); bb_s = np.std(prices[-20:])
    bb_pos = (prices[-1]-bb_m)/(bb_s*2+1e-10)
    # 动量
    mom5  = (prices[-1]-prices[-6])/(prices[-6]+1e-10)*100 if n>=6 else 0
    mom10 = (prices[-1]-prices[-11])/(prices[-11]+1e-10)*100 if n>=11 else 0
    # 成交量
    vm = np.mean(vols[-5:]) if n>=5 else vols[-1]
    vol_ratio = vols[-1]/(vm+1e-10)
    vol_chg = (vols[-1]-vols[-6])/(vols[-6]+1e-10)*100 if n>=6 else 0
    # 基础
    chg1 = (prices[-1]-prices[-2])/(prices[-2]+1e-10)*100 if n>=2 else 0
    sp = r["spread"]/(prices[-1]+1e-10)*100
    hl = (r["high"]-r["low"])/(prices[-1]+1e-10)*100
    bam = (r["bid"]+r["ask"])/2-prices[-1]
    vola = r["volatility"]*10000
    return np.array([[prices[-2] if n>=2 else 0, prices[-3] if n>=3 else 0,
        chg1, sma_5_10, sma_10_20, rsi, mom5, mom10, bb_pos,
        vol_ratio, vol_chg, sp, hl, bam, vola]])

@app.get("/api/ml/predict")
def ml_predict(symbol: str = Query("GOLD")):
    if symbol not in SYMBOL_CONFIG:
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    md = _load_ml()
    if not md: return {"symbol":symbol,"name":SYMBOL_CONFIG[symbol]["name"],"prediction":"无模型","confidence":0,"accuracy":0,"source":"none"}

    rows = []
    # 优先从 ES 读取实时数据
    es = get_es()
    if es:
        try:
            q = {"size":60,"sort":[{"event_time":"desc"}],"query":{"bool":{"filter":[{"term":{"symbol":symbol}}]}}}
            resp = es.search(index="finance_realtime_metrics", body=q, request_timeout=3)
            hits = resp["hits"]["hits"]
            if hits:
                rows = [{"price":h["_source"].get("price",0),
                         "volume":h["_source"].get("volume",0),
                         "spread":h["_source"].get("spread",0),
                         "high":h["_source"].get("high",0),
                         "low":h["_source"].get("low",0),
                         "bid":h["_source"].get("bid",0),
                         "ask":h["_source"].get("ask",0),
                         "volatility":h["_source"].get("volatility",0)} for h in hits]
                rows.reverse()  # 升序（旧→新）
        except: pass

    # ES 无数据时回退 CSV
    if len(rows) < 25:
        fpath = os.path.join(CSV_DIR, CSV_FILES.get(symbol,""))
        if os.path.exists(fpath):
            import csv
            with open(fpath,"r",encoding="utf-8-sig",newline="") as f:
                for rr in csv.DictReader(f):
                    rows.append({"price":float(rr["price"]),"volume":float(rr["volume"]),"spread":float(rr["spread"]),"high":float(rr["high"]),"low":float(rr["low"]),"bid":float(rr["bid"]),"ask":float(rr["ask"]),"volatility":float(rr["volatility"])})
            rows = rows[-60:]

    if len(rows) < 25:
        return {"symbol":symbol,"name":SYMBOL_CONFIG[symbol]["name"],"prediction":"数据不足","confidence":0,"accuracy":0,"source":"none"}

    X = _compute_features(rows[-50:])
    m = md["model"]; p = m.predict_proba(X)[0]; pred = int(m.predict(X)[0])
    return {"symbol":symbol,"name":SYMBOL_CONFIG[symbol]["name"],
        "prediction":"UP" if pred==1 else "DOWN",
        "confidence":round(float(p[pred]),3),
        "accuracy":round(float(md.get("accuracy",0)),3),
        "prob_up":round(float(p[1]),3),"prob_down":round(float(p[0]),3),
        "source":"ES实时" if es else "CSV静态"}


if __name__ == "__main__":
    import uvicorn
    print(f"\n{'='*50}\n  金融实时分析 API v4.0\n  监听 {API_HOST}:{API_PORT}\n  ES: {ES_HOST}:{ES_PORT}\n  Docs: http://localhost:{API_PORT}/docs\n{'='*50}\n")
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info")
