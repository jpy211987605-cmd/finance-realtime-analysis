#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
上传 Spark / API / 大屏文件到 Ubuntu，并做 Python 语法校验。

连接配置放在同目录 deploy.env（模板见 deploy.env.example），
支持 SSH 私钥（DEPLOY_SSH_KEY）或密码（DEPLOY_PASSWORD）。
"""
import os
import sys

import paramiko

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
PWD = os.environ.get("DEPLOY_PASSWORD", "")
SSH_KEY = os.environ.get("DEPLOY_SSH_KEY", "")
LOCAL_BASE = os.environ.get("DEPLOY_LOCAL_BASE", "")
REMOTE_BASE = os.environ.get("DEPLOY_REMOTE_BASE", "")

missing = [k for k, v in [
    ("DEPLOY_HOST", HOST),
    ("DEPLOY_USER", USER),
    ("DEPLOY_LOCAL_BASE", LOCAL_BASE),
    ("DEPLOY_REMOTE_BASE", REMOTE_BASE),
] if not v]
if missing:
    print("[ERROR] 缺少部署配置: " + ", ".join(missing))
    print("请复制 dataBase/scripts/deploy.env.example 为 deploy.env 并填写，或通过环境变量提供。")
    sys.exit(1)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

kwargs = {"port": PORT, "username": USER, "timeout": 15}
if SSH_KEY:
    kwargs["key_filename"] = SSH_KEY
    kwargs["allow_agent"] = False
    kwargs["look_for_keys"] = False
elif PWD:
    kwargs["password"] = PWD
    kwargs["allow_agent"] = False
    kwargs["look_for_keys"] = False

client.connect(HOST, **kwargs)
print("Connected")

sftp = client.open_sftp()

files = [
    (os.path.join(LOCAL_BASE, "src", "python", "finance_spark_streaming.py"),
     f"{REMOTE_BASE}/src/python/finance_spark_streaming.py"),
    (os.path.join(LOCAL_BASE, "src", "python", "finance_api_server.py"),
     f"{REMOTE_BASE}/src/python/finance_api_server.py"),
    (os.path.join(LOCAL_BASE, "output", "finance_dashboard.html"),
     f"{REMOTE_BASE}/output/finance_dashboard.html"),
]

for local, remote in files:
    sftp.put(local, remote)
    size = sftp.stat(remote).st_size
    print(f"OK: {remote.rsplit('/', 1)[-1]} ({size/1024:.1f}KB)")

sftp.close()

# 上传后做语法校验
cmd = (
    f"cd {REMOTE_BASE} && "
    'python3 -c "import ast; ast.parse(open(\"src/python/finance_spark_streaming.py\").read()); print(\"Spark syntax OK\")" && '
    'python3 -c "import ast; ast.parse(open(\"src/python/finance_api_server.py\").read()); print(\"API syntax OK\")"'
)
_, out, _ = client.exec_command(cmd)
print(out.read().decode())

client.close()
print("Done")
