# 贡献指南

感谢你考虑为本项目做出贡献！无论是提交 Issue、改进文档，还是修复 Bug，我们都非常欢迎。

## 开发环境

- Python 3.8+
- Docker 20.10+ / Docker Compose 2.0+
- Node.js 18+（可选，用于 Vue 前端）

```bash
# 克隆仓库
git clone https://github.com/JayChin0129/finance-realtime-analysis.git
cd finance-realtime-analysis

# 安装 Python 依赖
pip install -r dataBase/requirements.txt
```

## 分支约定

- `main`：稳定分支，请勿直接提交
- 新功能或修复请从 `main` 创建特性分支：

```bash
git checkout -b feature/your-feature-name
```

## 提交规范

建议使用清晰、语义化的提交信息，参考 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/)：

```
feat: 新增 xxx 功能
fix: 修复 xxx 问题
docs: 更新文档
chore: 工程化 / 工具链调整
```

## 代码风格

- Python：遵循 PEP 8，保持 4 空格缩进
- Shell：使用 Bash，保持 4 空格缩进
- Vue / JavaScript：保持 2 空格缩进
- 新增代码请添加必要的注释，尤其是脚本中的关键步骤

## 提交 Pull Request

1. Fork 本仓库并创建特性分支
2. 完成修改后，确保功能可正常运行
3. 提交 Pull Request，并在描述中说明改动目的与测试情况
4. 等待维护者 Review

## 报告问题

提交 Issue 时，请尽量包含：

- 运行环境（操作系统、Docker / Python 版本）
- 复现步骤
- 期望行为与实际行为
- 相关日志或报错信息

感谢你的参与！
