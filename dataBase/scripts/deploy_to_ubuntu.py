#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键部署脚本：传输文件 + 启动大数据集群 + AI 服务
使用 paramiko 自动化 SSH/SCP
"""
import paramiko
import os
import sys
import time

# ===== 配置 =====
HOST = "192.168.128.130"
USER = "jay"
PASSWORD = "qwe123qwe"
PORT = 22

LOCAL_BASE = r"D:\桌面\dataBase_03\dataBase"
REMOTE_BASE = "/home/jay/dataBase_03/dataBase"

# Sudo wrapper - auto-inject password via stdin
def sudo(cmd):
    """Wrap command with sudo password injection"""
    return f"echo '{PASSWORD}' | sudo -S {cmd}"

# 需要上传的文件 (本地相对路径 -> 远程绝对路径)
FILES_TO_UPLOAD = {
    "src/python/finance_api_server.py": f"{REMOTE_BASE}/src/python/finance_api_server.py",
    "output/finance_dashboard.html": f"{REMOTE_BASE}/output/finance_dashboard.html",
    "requirements.txt": f"{REMOTE_BASE}/requirements.txt",
}


def ssh_connect():
    """建立 SSH 连接"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"[SSH] Connecting to {USER}@{HOST}:{PORT} ...")
    client.connect(
        HOST, port=PORT, username=USER, password=PASSWORD,
        timeout=30, allow_agent=False, look_for_keys=False
    )
    print("  [OK] Connected")
    return client


def run_ssh(client, cmd, desc="", show_output=True):
    """执行远程命令并打印输出"""
    if desc:
        print(f"\n  [{desc}]")
    print(f"  $ {cmd[:120]}{'...' if len(cmd)>120 else ''}")

    stdin, stdout, stderr = client.exec_command(cmd, timeout=300)
    out = stdout.read().decode('utf-8', errors='replace')
    err = stderr.read().decode('utf-8', errors='replace')

    if show_output and out.strip():
        for line in out.strip().split("\n")[-20:]:
            print(f"    {line}")
    if err.strip() and "WARN" not in err:
        for line in err.strip().split("\n")[-5:]:
            print(f"    [ERR] {line}")

    return out, err


def upload_file(client, local_path, remote_path):
    """SCP 上传单个文件"""
    if not os.path.exists(local_path):
        print(f"  [FAIL] File not found: {local_path}")
        return False

    size_kb = os.path.getsize(local_path) / 1024
    fname = os.path.basename(local_path)
    print(f"  [UPLOAD] {fname} ({size_kb:.1f} KB) -> {remote_path}")

    sftp = client.open_sftp()
    try:
        sftp.put(local_path, remote_path)
        print(f"    [OK] Done")
        return True
    except Exception as e:
        print(f"    [FAIL] {e}")
        return False
    finally:
        sftp.close()


def main():
    print("=" * 56)
    print("  Finance Big Data Platform - Deploy (with AI Chat)")
    print(f"  Target: {USER}@{HOST}:{PORT}")
    print("=" * 56)

    # ==========================================
    # Stage 1: Connect + Upload files
    # ==========================================
    print("\n[1/4] Upload modified files...")
    print("-" * 40)

    ssh = ssh_connect()

    for local_rel, remote_abs in FILES_TO_UPLOAD.items():
        local_abs = os.path.join(LOCAL_BASE, local_rel)
        upload_file(ssh, local_abs, remote_abs)

    # ==========================================
    # Stage 2: Install dependencies
    # ==========================================
    print("\n[2/4] Install Python dependencies...")
    print("-" * 40)

    run_ssh(ssh, "pip install httpx>=0.24.0 2>&1 | tail -5", "Install httpx")

    # ==========================================
    # Stage 3: Start big data cluster
    # ==========================================
    print("\n[3/4] Start big data cluster...")
    print("-" * 40)

    # Check Docker
    out, _ = run_ssh(ssh, "sudo docker info --format '{{.ServerVersion}}' 2>&1 || echo 'NOT_RUNNING'", "Check Docker")
    if "NOT_RUNNING" in out:
        run_ssh(ssh, "sudo systemctl start docker && sleep 3 && sudo docker info --format '{{.ServerVersion}}'", "Start Docker")
        time.sleep(3)

    # Start Docker Compose
    out, _ = run_ssh(
        ssh,
        f"cd {REMOTE_BASE}/docker && sudo docker compose ps 2>/dev/null | grep -c 'Up' || echo 0",
        "Check container status"
    )
    try:
        running = int(out.strip().split("\n")[0]) if out.strip() else 0
    except:
        running = 0
    print(f"    Running containers: {running}/12")

    if running < 8:
        print("    Starting Docker Compose cluster...")
        run_ssh(ssh, f"cd {REMOTE_BASE}/docker && sudo docker compose up -d 2>&1", "docker compose up -d")
        print("    Waiting for cluster (90 sec)...")
        time.sleep(90)

        run_ssh(ssh, f"cd {REMOTE_BASE}/docker && sudo docker compose ps --format 'table {{{{.Name}}}}\t{{{{.Status}}}}' 2>/dev/null", "Container status")
    else:
        print("    [OK] Cluster already running")

    # Init Kafka/HDFS/Hive/ES
    out, _ = run_ssh(
        ssh,
        "sudo docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | wc -l",
        "Check Kafka Topics"
    )
    try:
        topics = int(out.strip()) if out.strip() else 0
    except:
        topics = 0

    if topics < 5:
        print(f"    Kafka Topics: {topics} (need init)")
        run_ssh(ssh, f"cd {REMOTE_BASE} && bash scripts/init_all.sh 2>&1", "Init Kafka+HDFS+Hive+ES")
    else:
        print(f"    [OK] Kafka Topics: {topics} (already initialized)")

    # ==========================================
    # Stage 4: Start data pipeline + API
    # ==========================================
    print("\n[4/4] Start data pipeline + AI service...")
    print("-" * 40)

    # Clean old processes
    run_ssh(ssh,
        "pkill -f finance_api_server 2>/dev/null; pkill -f finance_kafka_producer 2>/dev/null; sleep 1; echo 'old processes cleaned'",
        "Clean old processes"
    )

    # Start Kafka Producer
    run_ssh(ssh,
        f"cd {REMOTE_BASE} && nohup python src/python/finance_kafka_producer.py --rate 5 --loop > /tmp/producer.log 2>&1 & echo Producer_PID=$!",
        "Start Kafka Producer"
    )

    time.sleep(2)

    # Submit Spark Streaming
    run_ssh(ssh,
        'sudo docker exec spark-master bash -c "'
        '/opt/spark/bin/spark-submit '
        '--master spark://spark-master:7077 '
        '--deploy-mode client '
        '--name FinanceStreaming '
        '--total-executor-cores 2 '
        '--executor-memory 512m '
        '/home/spark/finance_spark_streaming.py '
        '> /tmp/spark_submit.log 2>&1 &" && echo Spark_submitted',
        "Submit Spark Streaming"
    )

    time.sleep(3)

    # Start FastAPI
    run_ssh(ssh,
        f"cd {REMOTE_BASE} && nohup python src/python/finance_api_server.py > /tmp/api_server.log 2>&1 & echo API_PID=$!",
        "Start FastAPI (with AI chat)"
    )

    time.sleep(4)

    # ==========================================
    # Verify
    # ==========================================
    print("\n" + "=" * 56)
    print("  Verify Services")
    print("=" * 56)

    # Check API health
    out, _ = run_ssh(ssh, "curl -s http://localhost:8000/health 2>/dev/null || echo 'FAIL'", "FastAPI Health Check")
    if "healthy" in out or "status" in out:
        print("  [OK] FastAPI :8000 - Running")
    else:
        print("  [FAIL] FastAPI :8000 - Check /tmp/api_server.log")

    # Check AI endpoint
    out, _ = run_ssh(ssh, "curl -s http://localhost:8000/api/config/apikey 2>/dev/null || echo 'FAIL'", "AI Endpoint Check")
    if "configured" in out:
        print("  [OK] AI Chat endpoints ready (/api/chat, /api/config/apikey)")
    else:
        print("  [FAIL] AI endpoints - Check /tmp/api_server.log")

    # Check containers
    print("\n  [Containers]:")
    run_ssh(ssh, "sudo docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null | head -15", "")

    # Check processes
    print("\n  [Processes]:")
    run_ssh(ssh, "ps aux | grep -E 'finance_api|finance_kafka' | grep -v grep || echo '  (none)'", "")

    # ==========================================
    # Done
    # ==========================================
    print("\n" + "=" * 56)
    print("  [DONE] Deployment complete!")
    print("")
    print("  URLs:")
    print("    API Docs:     http://192.168.128.130:8000/docs")
    print("    Dashboard:    output/finance_dashboard.html")
    print("    HDFS:         http://192.168.128.130:9870")
    print("    Spark UI:     http://192.168.128.130:8080")
    print("    Kibana:       http://192.168.128.130:5601")
    print("")
    print("  AI Chat: Open dashboard -> bottom-right button -> Settings -> Set Key")
    print("=" * 56)

    ssh.close()


if __name__ == "__main__":
    main()
