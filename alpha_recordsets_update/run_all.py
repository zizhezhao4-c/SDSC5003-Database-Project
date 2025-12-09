#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一运行脚本 - 一键执行完整流程或单个步骤

使用方法:
  python run_all.py --all              # 运行全部三个步骤
  python run_all.py --step 1           # 只运行步骤1
  python run_all.py --step 2           # 只运行步骤2
  python run_all.py --step 3           # 只运行步骤3
  python run_all.py --step 1 2         # 运行步骤1和2
  python run_all.py --dry-run          # 预览将要执行的命令
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# 脚本路径
SCRIPT_DIR = Path(__file__).parent
STEP1_SCRIPT = SCRIPT_DIR / "1_select_alpha_range.py"
STEP2_SCRIPT = SCRIPT_DIR / "2_fetch_and_store_local.py"
STEP3_SCRIPT = SCRIPT_DIR / "3_import_to_database.py"

STEPS = {
    1: {
        "name": "筛选Alpha范围",
        "script": STEP1_SCRIPT,
        "description": "从MongoDB筛选符合条件的alpha并生成ID列表"
    },
    2: {
        "name": "抓取数据到本地",
        "script": STEP2_SCRIPT,
        "description": "调用API抓取recordsets数据并保存到本地文件"
    },
    3: {
        "name": "导入到数据库",
        "script": STEP3_SCRIPT,
        "description": "从本地文件读取数据并导入到MongoDB"
    }
}


def print_banner():
    """打印横幅"""
    print("=" * 70)
    print("Alpha Recordsets 生产环境统一运行脚本")
    print("=" * 70)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def print_step_info(step_num: int):
    """打印步骤信息"""
    step = STEPS[step_num]
    print(f"\n{'='*70}")
    print(f"步骤 {step_num}: {step['name']}")
    print(f"{'='*70}")
    print(f"描述: {step['description']}")
    print(f"脚本: {step['script']}")
    print()


def run_step(step_num: int, dry_run: bool = False) -> bool:
    """
    运行指定步骤
    
    Args:
        step_num: 步骤编号 (1, 2, 或 3)
        dry_run: 是否只预览不执行
        
    Returns:
        bool: 是否成功
    """
    if step_num not in STEPS:
        print(f"错误: 无效的步骤编号 {step_num}")
        return False
    
    step = STEPS[step_num]
    script_path = step["script"]
    
    if not script_path.exists():
        print(f"错误: 脚本文件不存在: {script_path}")
        return False
    
    print_step_info(step_num)
    
    cmd = [sys.executable, str(script_path)]
    
    if dry_run:
        print(f"[预览模式] 将执行命令: {' '.join(cmd)}")
        return True
    
    try:
        print(f"开始执行步骤 {step_num}...")
        print(f"命令: {' '.join(cmd)}")
        print()
        
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            text=True
        )
        
        if result.returncode == 0:
            print(f"\n✅ 步骤 {step_num} 执行成功")
            return True
        else:
            print(f"\n❌ 步骤 {step_num} 执行失败 (返回码: {result.returncode})")
            return False
            
    except KeyboardInterrupt:
        print(f"\n⚠️ 用户中断步骤 {step_num}")
        return False
    except Exception as e:
        print(f"\n❌ 步骤 {step_num} 执行出错: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Alpha Recordsets 统一运行脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python run_all.py --all              # 运行全部三个步骤
  python run_all.py --step 1           # 只运行步骤1
  python run_all.py --step 2 3         # 运行步骤2和3
  python run_all.py --dry-run --all    # 预览全部步骤
        """
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="运行全部三个步骤"
    )
    
    parser.add_argument(
        "--step",
        type=int,
        nargs="+",
        choices=[1, 2, 3],
        help="指定要运行的步骤编号（可多选）"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式：只显示将要执行的命令，不实际执行"
    )
    
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="某个步骤失败后继续执行后续步骤"
    )
    
    args = parser.parse_args()
    
    # 确定要运行的步骤
    if args.all:
        steps_to_run = [1, 2, 3]
    elif args.step:
        steps_to_run = sorted(args.step)
    else:
        parser.print_help()
        return
    
    print_banner()
    
    if args.dry_run:
        print("⚠️ 预览模式：只显示命令，不实际执行")
        print()
    
    print(f"将要运行的步骤: {', '.join(map(str, steps_to_run))}")
    print()
    
    # 执行步骤
    results = {}
    for step_num in steps_to_run:
        success = run_step(step_num, dry_run=args.dry_run)
        results[step_num] = success
        
        if not success and not args.continue_on_error and not args.dry_run:
            print(f"\n⚠️ 步骤 {step_num} 失败，停止执行后续步骤")
            break
    
    # 打印总结
    print("\n" + "=" * 70)
    print("执行总结")
    print("=" * 70)
    
    for step_num in steps_to_run:
        step_name = STEPS[step_num]["name"]
        status = "✅ 成功" if results.get(step_num, False) else "❌ 失败"
        if args.dry_run:
            status = "👁️ 预览"
        print(f"步骤 {step_num} ({step_name}): {status}")
    
    print()
    
    # 返回退出码
    if args.dry_run:
        sys.exit(0)
    elif all(results.values()):
        print("🎉 所有步骤执行成功！")
        sys.exit(0)
    else:
        print("⚠️ 部分步骤执行失败，请检查日志")
        sys.exit(1)


if __name__ == "__main__":
    main()
