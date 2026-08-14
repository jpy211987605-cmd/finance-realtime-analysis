# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 规范，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新增
- 完善项目文档：根 `README.md`、`CONTRIBUTING.md`、`CHANGELOG.md`
- 新增 `LICENSE`（MIT）
- 新增 `.editorconfig` 与 `.gitattributes`，统一编辑器风格与换行符
- 清理并完善 `.gitignore`，忽略本地配置、依赖目录与敏感文件
- 根 `README.md` 新增「Ubuntu 部署与启动」完整指南（一键 / 分步 / 验证 / 停止）
- 新增部署配置模板 `dataBase/scripts/deploy.env.example`

### 安全
- 部署脚本凭据加固：`deploy_to_ubuntu.py`、`upload_spark.py`、`deploy_ai.sh` 改为从 `deploy.env` 读取连接配置，移除硬编码的服务器 IP、账号与密码，并支持 SSH 私钥登录

### 变更
- 将 `.claude/settings.local.json` 移出版本控制，改为本地忽略（含机器相关绝对路径）

## [0.1.0] - 2026-08-14

### 新增
- Docker 12 节点集群编排
- Kafka → Spark/HDFS + Logstash/ES 双消费者数据链路
- Hive 四层数仓（ODS → DWD → DWS → ADS）
- FastAPI 数据服务（15 个 REST 端点）
- ECharts 可视化大屏（14 个功能区域）
- 机器学习价格方向预测与 AI 智能对话
