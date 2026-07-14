@echo off
chcp 65001 >nul
title 金融实时行情分析大屏

echo ===============================================
echo   金融实时行情分析大屏
echo ===============================================
echo.

REM 检查 Ubuntu API 是否可达
echo 检查 API 服务 (192.168.128.130:8000) ...
curl -s --connect-timeout 3 http://192.168.128.130:8000/health >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] API 在线
) else (
    echo [WARN] API 未响应，请先在 Ubuntu 运行: bash scripts/start_all.sh
)

echo.
echo 打开大屏: http://192.168.128.130:3000/finance_dashboard.html
start http://192.168.128.130:3000/finance_dashboard.html

echo.
echo 大屏已打开，关闭此窗口即可。
timeout /t 3 >nul
