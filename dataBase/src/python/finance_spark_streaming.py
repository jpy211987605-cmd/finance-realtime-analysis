#!/usr/bin/env python3
"""金融行情 Spark Streaming v4.0 — Kafka→HDFS + OHLC聚合"""
import argparse, json, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType

TOPICS = "copper_tick,gas_tick,gold_tick,oil_tick,silver_tick"
SYMBOLS = ["GOLD","OIL","SILVER","COPPER","GAS"]
HDFS_PATH = "/user/finance/raw"
AGG_PATH = "/user/finance/agg/1min"
TICK_SCHEMA = StructType([
    StructField("symbol",StringType()),StructField("timestamp",StringType()),
    StructField("price",DoubleType()),StructField("volume",DoubleType()),
    StructField("bid",DoubleType()),StructField("ask",DoubleType()),
    StructField("spread",DoubleType()),StructField("direction",StringType()),
    StructField("volatility",DoubleType()),StructField("high",DoubleType()),
    StructField("low",DoubleType()),StructField("event_time",StringType()),
])

spark = SparkSession.builder.appName("FinanceStreaming") \
    .config("spark.sql.shuffle.partitions","2") \
    .config("spark.hadoop.fs.defaultFS","hdfs://hadoop-namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-brokers",default="kafka:9093")
    p.add_argument("--trigger",default="10 seconds")
    p.add_argument("--checkpoint",default="file:///tmp/spark_cp_finance")
    args = p.parse_args()
    print(f"\n{'='*50}\n  Spark Streaming v3.1\n  Kafka→HDFS: {HDFS_PATH}/{{symbol}}/dt=...\n{'='*50}\n")

    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers",args.kafka_brokers) \
        .option("subscribe",TOPICS).option("startingOffsets","latest") \
        .option("failOnDataLoss","false").option("maxOffsetsPerTrigger","1000").load()
    df = raw.select(F.from_json(F.col("value").cast("string"),TICK_SCHEMA).alias("d")) \
        .select("d.*").withColumn("event_ts",F.to_timestamp("event_time")).withColumn("dt",F.to_date("event_ts"))

    def write_batch(bdf, eid):
        if bdf.rdd.isEmpty(): return

        # === 原有: 原始 tick → HDFS ===
        cols = [c for c in ["symbol","timestamp","price","volume","bid","ask","spread","direction","volatility","high","low","event_time","dt"] if c in bdf.columns]
        for sym in SYMBOLS:
            sub = bdf.filter(F.col("symbol")==sym)
            if not sub.rdd.isEmpty():
                sub.select(*cols).write.mode("append").partitionBy("dt").json(f"{HDFS_PATH}/{sym.lower()}")
                print(f"  [RAW] {sym:6s} {sub.count()} 条 e={eid}")

        # === 新增: 1分钟 OHLC 聚合 → HDFS ===
        agg = bdf.withColumn("minute", F.date_trunc("minute", F.col("event_ts"))) \
            .groupBy("symbol", "minute") \
            .agg(
                F.first("price").alias("open_price"),
                F.max("high").alias("high_price"),
                F.min("low").alias("low_price"),
                F.last("price").alias("close_price"),
                F.avg("price").alias("avg_price"),
                F.sum("volume").alias("total_volume"),
                F.count("price").alias("tick_count")
            ) \
            .withColumn("dt", F.to_date("minute"))

        ohlc_cols = ["minute","open_price","high_price","low_price","close_price","avg_price","total_volume","tick_count","dt"]
        for sym in SYMBOLS:
            sub = agg.filter(F.col("symbol")==sym)
            if not sub.rdd.isEmpty():
                sub.select("symbol",*ohlc_cols).write.mode("append").partitionBy("dt").json(f"{AGG_PATH}/{sym.lower()}")
                cnt = sub.count()
                if cnt > 0:
                    print(f"  [OHLC] {sym:6s} {cnt} 分钟 e={eid}")

    df.writeStream.outputMode("append").foreachBatch(write_batch) \
      .trigger(processingTime=args.trigger).option("checkpointLocation",f"{args.checkpoint}/all").start()
    print("  [OK] 流已启动 (RAW → HDFS, OHLC → HDFS)\n")
    spark.streams.awaitAnyTermination()

if __name__=="__main__": main()
