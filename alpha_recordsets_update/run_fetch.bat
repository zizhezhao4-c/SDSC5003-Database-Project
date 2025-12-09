@echo off
REM Alpha Recordsets 数据抓取启动脚本
REM 直接使用 recordsets 模块，无需外层包装

echo ========================================
echo Alpha Recordsets 数据抓取
echo ========================================
echo.

REM 读取配置文件（这里使用默认值，实际应该从 async_config.json 读取）
set WQ_MAX_WORKERS=3
set WQ_UPDATE_MODE=update
set WQ_WRITE_PRIMARY_DB=1
set WQ_WRITE_SECONDARY_DB=0
set WQ_REQUEST_TIMEOUT=90
set WQ_MAX_RETRIES=9
set WQ_INITIAL_BACKOFF=8
set WQ_BACKOFF_MULTIPLIER=2.0
set WQ_MAX_BACKOFF=240

REM Alpha IDs 文件路径
set ALPHA_IDS_FILE=alpha_recordsets_update/filtered_alpha_ids.txt

echo 配置信息:
echo   MAX_WORKERS=%WQ_MAX_WORKERS%
echo   UPDATE_MODE=%WQ_UPDATE_MODE%
echo   ALPHA_IDS_FILE=%ALPHA_IDS_FILE%
echo.

REM 确认执行
set /p confirm="是否开始抓取? (y/N): "
if /i not "%confirm%"=="y" (
    echo 取消执行
    exit /b 0
)

echo.
echo 开始抓取数据...
echo.

REM 直接调用 recordsets 模块
python -m recordsets.fetch_recordsets --alpha-ids-file %ALPHA_IDS_FILE% --max-workers %WQ_MAX_WORKERS%

echo.
echo ========================================
echo 抓取完成
echo ========================================
pause
