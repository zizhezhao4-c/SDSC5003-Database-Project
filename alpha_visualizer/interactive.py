#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alpha Visualizer 交互式入口
"""

from visualize_alpha import visualize_alpha

def main():
    print("=" * 50)
    print("Alpha Recordsets 可视化工具")
    print("=" * 50)
    
    while True:
        alpha_id = input("\n请输入 Alpha ID (输入 q 退出): ").strip()
        
        if alpha_id.lower() == 'q':
            print("再见!")
            break
        
        if not alpha_id:
            print("Alpha ID 不能为空")
            continue
        
        visualize_alpha(alpha_id, output_dir='./output', show=True)


if __name__ == "__main__":
    main()
