#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融实时行情数据 Kafka 生产者

功能：
  1. 读取 5 个品种的 CSV tick 数据文件
  2. 轮询发送到对应的 Kafka Topic，模拟实时行情流
  3. 支持可配置发送速率、循环发送、进度显示

Kafka Topics:
  copper_tick, gas_tick, gold_tick, oil_tick, silver_tick

使用示例：
  # 默认速率发送（每秒5条，单轮）
  python finance_kafka_producer.py

  # 自定义速率
  python finance_kafka_producer.py --rate 10 --loop

  # 仅发送指定品种
  python finance_kafka_producer.py --symbols GOLD,OIL

依赖：
  pip install kafka-python
"""

import argparse
import csv
import json
import os
import sys
import time
import signal
from datetime import datetime
from typing import Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ============================================================================
# 全局配置
# ============================================================================

# 品种 → CSV文件映射
SYMBOL_CONFIG: Dict[str, dict] = {
    "GOLD":   {"file": "gold_tick_data.csv",   "topic": "gold_tick",   "name": "黄金期货"},
    "OIL":    {"file": "oil_tick_data.csv",    "topic": "oil_tick",    "name": "原油期货"},
    "SILVER": {"file": "silver_tick_data.csv", "topic": "silver_tick", "name": "白银期货"},
    "COPPER": {"file": "copper_tick_data.csv", "topic": "copper_tick", "name": "铜期货"},
    "GAS":    {"file": "gas_tick_data.csv",    "topic": "gas_tick",    "name": "天然气期货"},
}

# 全局停止标志（用于优雅退出）
_stop_flag = False


def signal_handler(signum, frame):
    """处理 SIGINT/SIGTERM 信号"""
    global _stop_flag
    _stop_flag = True
    print("\n⚠ 收到停止信号，正在优雅退出...")


# 注册信号处理
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class FinanceDataProducer:
    """
    金融数据 Kafka 生产者

    读取 CSV → 解析 → 发送到 Kafka → 进度跟踪
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        rate_per_second: int = 5,
        loop: bool = False,
        symbols: Optional[List[str]] = None,
    ):
        """
        初始化生产者

        Args:
            bootstrap_servers: Kafka broker 地址
            rate_per_second:  每秒发送速率（所有品种合计）
            loop:             是否循环发送（文件结束后重新开始）
            symbols:          要发送的品种列表，None 表示全部
        """
        self.bootstrap_servers = bootstrap_servers
        self.rate_per_second = rate_per_second
        self.loop = loop
        self.symbols = symbols or list(SYMBOL_CONFIG.keys())

        # 延迟计算
        self.delay_per_message = 1.0 / rate_per_second if rate_per_second > 0 else 0.1

        # 数据存储
        self.data_cache: Dict[str, list] = {}   # symbol → [{row}, ...]
        self.sent_counts: Dict[str, int] = {}    # symbol → 已发送数
        self.total_sent = 0

        # Kafka 生产者（延迟连接）
        self.producer: Optional[KafkaProducer] = None

        # 项目根目录
        self.project_root = self._find_project_root()

    # ----- 内部方法 -----

    @staticmethod
    def _find_project_root() -> str:
        """自动查找项目根目录"""
        current = os.path.dirname(os.path.abspath(__file__))
        markers = ["docker", "gold_tick_data.csv"]
        for _ in range(6):
            if any(os.path.exists(os.path.join(current, m)) for m in markers):
                return current
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
        print(f"  [WARN] 无法自动定位项目根目录，使用: {os.getcwd()}")
        return os.getcwd()

    def _load_csv(self, symbol: str) -> list:
        """
        加载单个 CSV 文件到内存

        Args:
            symbol: 品种代码

        Returns:
            list[dict]: 解析后的数据行
        """
        config = SYMBOL_CONFIG[symbol]
        filepath = os.path.join(self.project_root, config["file"])

        if not os.path.exists(filepath):
            print(f"  ✗ 文件不存在: {filepath}")
            return []

        rows = []
        with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 类型转换 + 补充 symbol
                processed = {
                    "symbol": symbol,
                    "timestamp": row["timestamp"].strip(),
                    "price": float(row["price"]),
                    "volume": float(row["volume"]),
                    "bid": float(row["bid"]),
                    "ask": float(row["ask"]),
                    "spread": float(row["spread"]),
                    "direction": row["direction"].strip(),
                    "volatility": float(row["volatility"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "event_time": row["timestamp"].strip(),
                }
                rows.append(processed)

        print(f"  ✓ {config['name']}: 加载 {len(rows):,} 条 → Topic [{config['topic']}]")
        return rows

    def _connect_kafka(self):
        """连接 Kafka，带重试机制"""
        max_retries = 10
        retry_interval = 3

        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=3,
                    acks="all",
                    max_block_ms=10000,
                    request_timeout_ms=5000,
                    linger_ms=5,               # 小批量聚合
                    compression_type="gzip",    # 压缩传输
                )
                print(f"✓ Kafka 连接成功: {self.bootstrap_servers}")
                return
            except NoBrokersAvailable:
                print(f"  ⏳ 等待 Kafka 就绪... (尝试 {attempt}/{max_retries})")
                time.sleep(retry_interval)
            except Exception as e:
                print(f"  ⚠ Kafka 连接异常: {e}")
                time.sleep(retry_interval)

        raise RuntimeError(f"无法连接 Kafka ({self.bootstrap_servers})，已重试 {max_retries} 次")

    # ----- 公共方法 -----

    def load_all_data(self):
        """加载所有品种的 CSV 数据到内存"""
        print("\n" + "=" * 60)
        print("  加载 CSV 数据文件")
        print("=" * 60)

        for symbol in self.symbols:
            self.data_cache[symbol] = self._load_csv(symbol)
            self.sent_counts[symbol] = 0

        total_rows = sum(len(v) for v in self.data_cache.values())
        print(f"\n  总计: {total_rows:,} 条记录, {len(self.symbols)} 个品种")
        print(f"  发送速率: {self.rate_per_second} 条/秒, 间隔 {self.delay_per_message*1000:.0f}ms")
        print(f"  模式: {'循环发送' if self.loop else '单轮发送'}")

    def send_all(self):
        """
        轮询发送所有品种数据到 Kafka

        策略：5个品种轮询，每个品种每次发1条，保证公平性
        """
        if not self.producer:
            self._connect_kafka()

        print("\n" + "=" * 60)
        print("  开始发送数据到 Kafka")
        print("=" * 60)

        # 为每个品种维护独立的游标
        cursors = {s: 0 for s in self.symbols if self.data_cache.get(s)}
        symbol_list = list(cursors.keys())

        if not symbol_list:
            print("❌ 没有可发送的数据！")
            return

        start_time = time.time()
        round_count = 1
        max_rounds = 999999 if self.loop else 1

        try:
            while round_count <= max_rounds and not _stop_flag:
                for symbol in symbol_list:
                    if _stop_flag:
                        break

                    data = self.data_cache[symbol]
                    if not data:
                        continue

                    idx = cursors[symbol]

                    # 如果游标到达末尾
                    if idx >= len(data):
                        if self.loop:
                            cursors[symbol] = 0
                            idx = 0
                        else:
                            continue

                    # 发送一条消息
                    row = data[idx]
                    topic = SYMBOL_CONFIG[symbol]["topic"]
                    key = f"{symbol}_{row['timestamp']}"

                    try:
                        future = self.producer.send(topic, value=row, key=key)
                        # 非阻塞等待（允许错误积累）
                        future.get(timeout=5)
                        self.sent_counts[symbol] += 1
                        self.total_sent += 1
                    except Exception as e:
                        print(f"  ✗ 发送失败 [{symbol}]: {e}")

                    cursors[symbol] = idx + 1

                    # 进度日志
                    if self.total_sent % 500 == 0:
                        elapsed = time.time() - start_time
                        rate = self.total_sent / elapsed if elapsed > 0 else 0
                        print(f"  📊 已发送 {self.total_sent:,} 条 | "
                              f"速率 {rate:.1f} 条/秒 | "
                              f"运行 {elapsed:.0f}秒")

                    # 控制发送速率
                    time.sleep(self.delay_per_message)

                # 检查是否所有品种都发送完毕
                all_done = all(
                    cursors[s] >= len(self.data_cache[s]) for s in symbol_list
                )
                if all_done and not self.loop:
                    break
                if all_done and self.loop:
                    round_count += 1
                    for s in symbol_list:
                        cursors[s] = 0
                    print(f"\n  🔄 第 {round_count} 轮发送开始...")
                    time.sleep(1)

        finally:
            self._flush_and_report(start_time)

    def _flush_and_report(self, start_time: float):
        """刷新缓冲区并打印统计报告"""
        if self.producer:
            print("\n  ⏳ 刷新 Kafka 缓冲区...")
            self.producer.flush(timeout=30)
            self.producer.close()
            print("  ✓ Kafka 连接已关闭")

        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("  发送统计报告")
        print("=" * 60)
        print(f"  总耗时:      {elapsed:.1f} 秒")
        print(f"  总发送:      {self.total_sent:,} 条")
        print(f"  平均速率:    {self.total_sent/elapsed:.1f} 条/秒" if elapsed > 0 else "  N/A")
        print()
        for symbol in self.symbols:
            count = self.sent_counts.get(symbol, 0)
            name = SYMBOL_CONFIG[symbol]["name"]
            topic = SYMBOL_CONFIG[symbol]["topic"]
            print(f"  [{symbol:6s}] {name:8s} → {topic:16s} : {count:>8,} 条")
        print("=" * 60)


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="金融实时行情数据 Kafka 生产者",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                          # 默认：全部品种, 5条/秒, 单轮
  %(prog)s --rate 10 --loop         # 10条/秒, 循环发送
  %(prog)s --symbols GOLD,OIL       # 仅发送黄金和原油
  %(prog)s --rate 20 --loop --broker kafka:9093  # 容器内使用
        """,
    )
    parser.add_argument(
        "--rate", type=int, default=5,
        help="每秒发送速率（默认 5 条/秒）"
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="循环发送模式（CSV 文件读完后重新开始）"
    )
    parser.add_argument(
        "--symbols", type=str, default=None,
        help="要发送的品种，逗号分隔（如 GOLD,OIL,SILVER）。默认全部"
    )
    parser.add_argument(
        "--broker", type=str, default="localhost:9092",
        help="Kafka Bootstrap Server 地址（默认 localhost:9092）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅加载数据并验证，不实际发送到 Kafka"
    )

    args = parser.parse_args()

    # 解析品种过滤
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
        invalid = [s for s in symbols if s not in SYMBOL_CONFIG]
        if invalid:
            print(f"❌ 无效品种: {invalid}")
            print(f"   有效品种: {list(SYMBOL_CONFIG.keys())}")
            sys.exit(1)

    # 打印启动信息
    print("=" * 60)
    print("  🏦 金融实时行情 Kafka 生产者")
    print("=" * 60)
    print(f"  启动时间:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Kafka:     {args.broker}")
    print(f"  发送速率:  {args.rate} 条/秒")
    print(f"  发送模式:  {'循环发送' if args.loop else '单轮发送'}")
    print(f"  品种:      {symbols or '全部'}")

    # 创建生产者实例
    producer = FinanceDataProducer(
        bootstrap_servers=args.broker,
        rate_per_second=args.rate,
        loop=args.loop,
        symbols=symbols,
    )

    # 加载 CSV 数据
    producer.load_all_data()

    # Dry-run 模式：仅验证不发送
    if args.dry_run:
        print("\n✓ Dry-run 完成，数据加载正常（未发送到 Kafka）")
        return

    # 发送数据
    producer.send_all()

    print(f"\n✓ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
