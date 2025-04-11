#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
直接运行脚本

在funding_arbitrage_bot目录中直接运行的入口脚本，
解决相对导入问题。
"""

import os
import sys
import asyncio
import argparse

# 将上级目录添加到路径，这样可以确保能找到funding_arbitrage_bot包
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="资金费率套利机器人")
    parser.add_argument("--test", action="store_true", help="测试模式 - 只测试API连接和获取持仓")
    parser.add_argument("--config", default="funding_arbitrage_bot/config.yaml", help="配置文件路径")
    args = parser.parse_args()
    
    # 设置事件循环策略
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 使用绝对导入
    from funding_arbitrage_bot.main import main
    
    # 保存原始sys.argv
    original_argv = sys.argv.copy()
    
    # 重新构建sys.argv以传递正确的参数给main函数
    sys.argv = [sys.argv[0]]  # 保留脚本名称
    
    # 添加测试模式参数
    if args.test:
        sys.argv.append("--test")
    
    # 添加配置文件参数
    sys.argv.extend(["--config", args.config])
    
    # 运行主程序
    asyncio.run(main())
    
    # 恢复原始sys.argv
    sys.argv = original_argv 