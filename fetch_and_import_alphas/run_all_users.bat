@echo off
REM Windows批处理脚本：运行多用户自动化脚本
REM 使用方法: 双击此文件或在命令行运行 run_all_users.bat

echo ================================================================================
echo WorldQuant Alpha 多用户自动化执行脚本
echo ================================================================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到Python，请先安装Python 3.8或更高版本
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [信息] Python版本:
python --version
echo.

REM 检查脚本文件是否存在
if not exist "run_all_users.py" (
    echo [错误] 找不到 run_all_users.py 脚本
    echo 请确保在正确的目录下运行此批处理文件
    pause
    exit /b 1
)

echo [信息] 开始执行多用户自动化脚本...
echo.

REM 运行Python脚本
python run_all_users.py

REM 保存退出码
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ================================================================================
if %EXIT_CODE% equ 0 (
    echo [成功] 脚本执行完成
) else (
    echo [失败] 脚本执行失败，退出码: %EXIT_CODE%
)
echo ================================================================================
echo.
echo 按任意键退出...
pause >nul

exit /b %EXIT_CODE%

