#!/bin/bash
# Linux/Mac Shell脚本：运行多用户自动化脚本
# 使用方法: ./run_all_users.sh 或 bash run_all_users.sh

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "WorldQuant Alpha 多用户自动化执行脚本"
echo "================================================================================"
echo ""

# 检查Python是否安装
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo -e "${RED}[错误]${NC} 未找到Python，请先安装Python 3.8或更高版本"
    exit 1
fi

# 优先使用python3，如果不存在则使用python
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
else
    PYTHON_CMD="python"
fi

echo -e "${GREEN}[信息]${NC} Python版本:"
$PYTHON_CMD --version
echo ""

# 检查脚本文件是否存在
if [ ! -f "run_all_users.py" ]; then
    echo -e "${RED}[错误]${NC} 找不到 run_all_users.py 脚本"
    echo "请确保在正确的目录下运行此脚本"
    exit 1
fi

echo -e "${GREEN}[信息]${NC} 开始执行多用户自动化脚本..."
echo ""

# 运行Python脚本
$PYTHON_CMD run_all_users.py

# 保存退出码
EXIT_CODE=$?

echo ""
echo "================================================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}[成功]${NC} 脚本执行完成"
else
    echo -e "${RED}[失败]${NC} 脚本执行失败，退出码: $EXIT_CODE"
fi
echo "================================================================================"
echo ""

exit $EXIT_CODE

