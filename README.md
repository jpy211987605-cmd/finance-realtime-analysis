# 🏦 金融实时大数据分析平台

<div align="center">

[![License](https://img.shields.io/github/license/jpy211987605-cmd/finance-realtime-analysis?style=flat-square&color=blue)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/jpy211987605-cmd/finance-realtime-analysis?style=flat-square&color=green)](https://github.com/jpy211987605-cmd/finance-realtime-analysis/commits)
[![Repo Size](https://img.shields.io/github/repo-size/jpy211987605-cmd/finance-realtime-analysis?style=flat-square&color=orange)](https://github.com/jpy211987605-cmd/finance-realtime-analysis)

**基于 Docker 12 节点集群构建的金融实时行情分析平台**

覆盖黄金、原油、白银、铜、天然气 5 个期货品种，实现从数据采集、实时计算到可视化大屏的全链路自动化处理。

</div>

---

## 📋 项目简介

本项目是一个端到端的金融实时大数据分析系统，完整打通「数据采集 → 消息队列 → 数据湖存储 → 数据仓库建模 → 实时计算 → 数据服务 → 可视化」链路。

| 维度 | 说明 |
|------|------|
| 数据资产 | 5 个期货品种，25 万条 tick 级行情数据 |
| 数据字段 | timestamp、price、volume、bid、ask、spread、direction、volatility、high、low |
| 核心链路 | CSV → Kafka → Spark/HDFS + Logstash/ES → FastAPI → ECharts 大屏 |
| 可视化 | 14 个可视化区域，秒级刷新 |

---

## ✨ 核心特性

- **双消费者分流架构**：Spark Streaming 写 HDFS 数据湖，Logstash 写 Elasticsearch，两条链路独立容错。
- **四层数仓建模**：Hive `ODS → DWD → DWS → ADS`，支持离线分析与指标计算。
- **实时技术指标**：SMA、OHLC、波动率、最大回撤、买卖价差等金融指标。
- **机器学习预测**：`GradientBoosting + RandomForest` 集成模型预测价格方向。
- **AI 智能对话**：集成 DeepSeek V4 Pro，支持行情解读与技术架构问答。
- **一键部署**：Docker Compose 编排 12 节点集群，脚本化初始化、启动与验证。

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
                       (秒级轮询, 14个区域)
```

---

## 🧰 技术栈

| 层级 | 技术 |
|------|------|
| 数据采集 | Python · kafka-python |
| 消息队列 | Apache Kafka · ZooKeeper |
| 实时计算 | Spark Structured Streaming |
| 数据管道 | Logstash |
| 数据存储 | HDFS · Hive · Elasticsearch · PostgreSQL |
| 数据服务 | FastAPI · Uvicorn |
| 可视化 | ECharts · Vue 3 · Vite |
| 机器学习 | scikit-learn |
| AI 对话 | DeepSeek V4 Pro |
| 容器编排 | Docker · Docker Compose |

---

## 🐧 Ubuntu 部署与启动

本项目生产运行环境为 **Ubuntu 20.04+**，通过 Docker Compose 编排 12 个容器节点。

### 1. 环境准备

建议配置：**CPU 4 核 +、内存 16GB、可用磁盘 30GB+**（12 个容器同时运行，内存不足会导致容器反复重启）。

```bash
# 1.1 安装基础工具
sudo apt update
sudo apt install -y git curl python3 python3-pip

# 1.2 安装 Docker（含 Docker Compose v2 插件）
curl -fsSL https://get.docker.com | sudo sh
sudo systemctl enable --now docker

# 1.3 将当前用户加入 docker 组（避免每条 docker 命令都要 sudo）
sudo usermod -aG docker "$USER"
newgrp docker
# 或退出当前 SSH 会话后重新登录

# 1.4 验证
docker --version
docker compose version
```

### 2. 获取代码

```bash
git clone https://github.com/jpy211987605-cmd/finance-realtime-analysis.git
cd finance-realtime-analysis/dataBase
```

> 后续所有命令默认在 `finance-realtime-analysis/dataBase` 目录下执行。

### 3. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

若你的 Ubuntu 提示 `externally-managed-environment`，建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> 使用虚拟环境时，后面的启动命令需保持在同一个已激活的终端会话中执行。

### 4. 启动方式 A：一键启动（推荐演示 / 从零开始）

```bash
bash scripts/demo_restart.sh
```

该脚本会自动完成：**清理旧环境 → 启动 12 节点 Docker 集群 → 等待服务就绪 → 初始化 Kafka/HDFS/Hive/ES → 启动数据管道 + FastAPI**，全程约 2~3 分钟。

### 5. 启动方式 B：分步启动（推荐调试 / 生产运维）

```bash
# 5.1 启动 Docker 大数据集群
cd docker
docker compose up -d

# 等待约 90 秒（Hive 元数据初始化较慢），确认 12 个容器就绪
docker compose ps
cd ..

# 5.2 初始化业务组件（Kafka Topic / HDFS 目录 / Hive 数仓 / ES 索引，幂等可重复执行）
bash scripts/init_all.sh

# 5.3 启动数据管道（Spark Streaming + Kafka Producer + FastAPI）
bash scripts/start_pipeline.sh --rate 5 --loop

# 5.4 启动前端大屏（端口 3000）
cd output
nohup python3 -m http.server 3000 > /tmp/dashboard_server.log 2>&1 &
cd ..
```

### 6. 验证服务

```bash
# 健康检查
curl http://localhost:8000/health

# 最新行情快照
curl http://localhost:8000/api/realtime/latest | python3 -m json.tool

# ES 已写入数据量（> 0 表示链路已打通）
curl http://localhost:9200/finance_realtime_metrics/_count

# 查看各组件运行状态
bash scripts/start_pipeline.sh --status
cd docker && docker compose ps && cd ..
```

### 7. 访问地址

| 服务 | 地址 |
|------|------|
| 可视化大屏 | http://localhost:3000/finance_dashboard.html |
| FastAPI 接口文档 | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |
| HDFS NameNode UI | http://localhost:9870 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |

> 若从其他机器访问，把 `localhost` 换成 Ubuntu 服务器的内网 IP，并确保对应端口在防火墙/安全组中已放行。

### 8. 停止与清理

```bash
# 停止数据管道（Producer / Spark / FastAPI）
bash scripts/stop_pipeline.sh

# 停止 Docker 集群（保留数据卷）
cd docker && docker compose down && cd ..

# 彻底清理（删除数据卷，下次需重新初始化）
cd docker && docker compose down -v && cd ..
```

### 9. 从 Windows 自动部署（可选）

仓库提供了自动化部署脚本，适合在 Windows 本地改完代码后一键上传到 Ubuntu：

| 脚本 | 说明 |
|------|------|
| `scripts/deploy_to_ubuntu.py` | 基于 paramiko 的 SSH/SCP 自动部署（Windows 本地运行） |
| `scripts/deploy_ai.sh` | 基于 scp + ssh 的 Shell 版部署（Git Bash 运行） |

连接信息统一放在 `dataBase/scripts/deploy.env` 中（模板为 `deploy.env.example`），不再写死在脚本里：

```bash
cd dataBase/scripts
cp deploy.env.example deploy.env
# 编辑 deploy.env，填入服务器 IP、用户名、本地/远端路径
# 推荐配置 DEPLOY_SSH_KEY 使用私钥登录；不填私钥则填 DEPLOY_PASSWORD

python deploy_to_ubuntu.py
```

> ⚠️ `deploy.env` 已被 `.gitignore` 忽略，请勿将真实密码或私钥提交到仓库。运行 `deploy_to_ubuntu.py` 前请先 `pip install paramiko`。

---

## 🪟 Windows 本地模型训练（可选）

机器学习模型训练不依赖 Docker 集群，可在 Windows 本地完成：

```bash
pip install -r dataBase/requirements.txt
python dataBase/src/python/train_model.py
```

训练完成后生成 `model_direction.joblib`，上传到 Ubuntu 服务器的 `dataBase` 目录即可用于预测。

---

## 📁 项目结构

```
finance-realtime-analysis/
├── dataBase/                        # 核心实现
│   ├── docker/                      # 12 节点集群编排
│   ├── src/python/                  # Producer / Spark / FastAPI / 模型训练
│   ├── config/                      # Hive / Logstash / ES / 镜像配置
│   ├── scripts/                     # 初始化、启动、停止、验证脚本
│   ├── frontend/                    # Vue 3 前端工程
│   ├── output/                      # 纯 HTML 可视化大屏
│   ├── requirements.txt             # Python 依赖清单
│   └── README.md                    # 详细使用文档
├── .gitignore
├── .editorconfig
├── .gitattributes
├── LICENSE
├── CHANGELOG.md
├── CONTRIBUTING.md
└── README.md                        # 本文件
```

---

## 📚 文档导航

| 文档 | 说明 |
|------|------|
| [`dataBase/README.md`](dataBase/README.md) | 快速启动、服务端口、API 接口、常见问题 |
| [`dataBase/README_FINANCE.md`](dataBase/README_FINANCE.md) | 金融指标、数据资产、可重复运行说明 |
| [`dataBase/PROJECT_ARCHITECTURE.md`](dataBase/PROJECT_ARCHITECTURE.md) | 完整架构文档、关键设计决策 |
| [`dataBase/技术文档.md`](dataBase/技术文档.md) | 中文技术文档 |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | 贡献指南 |
| [`CHANGELOG.md`](CHANGELOG.md) | 版本更新日志 |

---

## 🛠️ 常见问题

| 问题 | 解决 |
|------|------|
| `docker` 提示权限不足 | 执行 `sudo usermod -aG docker $USER` 后重新登录 |
| 容器反复重启 | 检查内存是否充足（建议 16GB），`docker compose logs 容器名` 查看日志 |
| Spark 报 `Failed to find data source: kafka` | 手动下载 `spark-sql-kafka-0-10_2.12-3.3.3.jar` 到 `/opt/spark/jars/` |
| 大屏无数据 | `curl http://localhost:8000/health` 与 `curl http://localhost:9200/finance_realtime_metrics/_count` 排查 |
| 端口冲突 | `sudo lsof -i :端口号` 或 `sudo netstat -tlnp | grep 端口号` 查找占用进程 |

---

## 📄 许可证

本项目采用 [MIT License](LICENSE)。

---

## 🙋 联系方式

如有问题或建议，欢迎提交 [Issue](https://github.com/jpy211987605-cmd/finance-realtime-analysis/issues) 或 Pull Request。
