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

# ===== 配置（从环境变量 / deploy.env 读取，避免硬编码凭据）=====
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_deploy_env(path):
    """从 deploy.env 读取 KEY=VALUE 到环境变量（已存在的环境变量优先）。"""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_deploy_env(os.path.join(SCRIPT_DIR, "deploy.env"))

HOST = os.environ.get("DEPLOY_HOST", "")
PORT = int(os.environ.get("DEPLOY_PORT", "22"))
USER = os.environ.get("DEPLOY_USER", "")
PASSWORD = os.environ.get("DEPLOY_PASSWORD", "")
SSH_KEY = os.environ.get("DEPLOY_SSH_KEY", "")
LOCAL_BASE = os.environ.get("DEPLOY_LOCAL_BASE", "")
REMOTE_BASE = os.environ.get("DEPLOY_REMOTE_BASE", "")

# Sudo wrapper - 优先使用密码自动注入，未配置密码时走普通 sudo（密钥/NOPASSWD）
def sudo(cmd):
    """Wrap command with optional sudo password injection."""
    if PASSWORD:
        return f"echo '{PASSWORD}' | sudo -S {cmd}"
    return f"sudo {cmd}"

# 需要上传的文件 (本地相对路径 -> 远程绝对路径)
FILES_TO_UPLOAD = {
    "src/python/finance_api_server.py": f"{REMOTE_BASE}/src/python/finance_api_server.py",
    "output/finance_dashboard.html": f"{REMOTE_BASE}/output/finance_dashboard.html",
    "requirements.txt": f"{REMOTE_BASE}/requirements.txt",
}


def ssh_connect():
    """建立 SSH 连接（优先使用私钥，其次使用密码）。"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"[SSH] Connecting to {USER}@{HOST}:{PORT} ...")

    kwargs = {"port": PORT, "username": USER, "timeout": 30}
    if SSH_KEY:
        kwargs["key_filename"] = SSH_KEY
        kwargs["allow_agent"] = False
        kwargs["look_for_keys"] = False
    elif PASSWORD:
        kwargs["password"] = PASSWORD
        kwargs["allow_agent"] = False
        kwargs["look_for_keys"] = False

    client.connect(HOST, **kwargs)
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
    missing = [k for k, v in [
        ("DEPLOY_HOST", HOST),
        ("DEPLOY_USER", USER),
        ("DEPLOY_LOCAL_BASE", LOCAL_BASE),
        ("DEPLOY_REMOTE_BASE", REMOTE_BASE),
    ] if not v]
    if missing:
        print("[ERROR] 缺少部署配置: " + ", ".join(missing))
        print("请复制 dataBase/scripts/deploy.env.example 为 deploy.env 并填写，")
        print("或通过环境变量提供上述配置后重试。")
        sys.exit(1)

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
    print(f"    API Docs:     http://{HOST}:8000/docs")
    print("    Dashboard:    output/finance_dashboard.html")
    print(f"    HDFS:         http://{HOST}:9870")
    print(f"    Spark UI:     http://{HOST}:8080")
    print(f"    Kibana:       http://{HOST}:5601")
    print("")
    print("  AI Chat: Open dashboard -> bottom-right button -> Settings -> Set Key")
    print("=" * 56)

    ssh.close()


if __name__ == "__main__":
    main()
