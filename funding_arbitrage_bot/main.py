#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主模块

资金费率套利机器人的主入口点
"""

import os
import sys
import logging
import argparse
import asyncio
from typing import Dict, Any
import yaml

# 添加导入异常处理
try:
    # 首先尝试从包内导入
    from funding_arbitrage_bot.exchanges.backpack_api import BackpackAPI
    from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
    from funding_arbitrage_bot.core.arbitrage_engine import ArbitrageEngine
    from funding_arbitrage_bot.utils.helpers import load_config
    from funding_arbitrage_bot.utils.logger import setup_logger
except ImportError:
    try:
        # 如果从包内导入失败，尝试相对导入
        from exchanges.backpack_api import BackpackAPI
        from exchanges.hyperliquid_api import HyperliquidAPI
        from core.arbitrage_engine import ArbitrageEngine
        from utils.helpers import load_config
        from utils.logger import setup_logger
    except ImportError:
        # 如果相对导入也失败，可能需要调整Python路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        # 再次尝试相对导入
        from exchanges.backpack_api import BackpackAPI
        from exchanges.hyperliquid_api import HyperliquidAPI
        from core.arbitrage_engine import ArbitrageEngine
        from utils.helpers import load_config
        from utils.logger import setup_logger

async def run_bot(config: Dict[str, Any], test_mode: bool = False):
    """
    运行套利机器人
    
    Args:
        config: 配置字典，已加载的配置内容
        test_mode: 如果为True，只测试API连接而不运行完整的套利策略
    """
    # 设置日志
    log_config = config.get("logging", {})
    
    # 使用新的日志配置函数
    logger = setup_logger(log_config, "funding_arbitrage")
    
    # 静音一些噪声太大的日志记录器
    for noisy_logger in ["websockets", "websockets.client", "websockets.server", "websockets.protocol"]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
    
    # 初始化交易所API
    exchange_config = config.get("exchanges", {})
    
    # 初始化Backpack API
    print("正在初始化Backpack API...")
    bp_config = exchange_config.get("backpack", {})
    backpack_api = BackpackAPI(
        api_key=bp_config.get("api_key", ""),
        api_secret=bp_config.get("api_secret", ""),
        logger=logger,
        config=config
    )
    print("Backpack API初始化完成")
    
    # 初始化Hyperliquid API
    print("正在初始化Hyperliquid API...")
    hl_config = exchange_config.get("hyperliquid", {})
    hyperliquid_api = HyperliquidAPI(
        api_key=hl_config.get("api_key", ""),
        api_secret=hl_config.get("api_secret", ""),
        logger=logger,
        config=config
    )
    print("Hyperliquid API初始化完成")
    
    # 创建套利引擎实例 - 传递日志配置
    arbitrage_engine = ArbitrageEngine(
        config=config,
        backpack_api=backpack_api,
        hyperliquid_api=hyperliquid_api,
        logger=logger
    )
    
    # 测试模式：仅测试API连接
    if test_mode:
        print("正在测试交易所API连接...")
        
        try:
            # 测试Backpack API
            print("测试Backpack API...")
            bp_positions = await backpack_api.get_positions()
            print(f"Backpack持仓信息: {bp_positions}")
            
            # 测试Hyperliquid API
            print("测试Hyperliquid API...")
            hl_positions = await hyperliquid_api.get_positions()
            print(f"Hyperliquid持仓: {hl_positions}")
            
            print("API测试成功!")
            return
        
        except Exception as e:
            print(f"API测试失败: {e}")
            logger.error(f"API测试失败: {e}")
            raise
    
    # 创建套利引擎
    print("正在创建套利引擎...")
    
    # 确保配置包含所有必要的键
    print(f"配置类型: {type(config)}")
    
    # 检查配置中是否包含策略配置
    if "strategy" not in config:
        raise ValueError("配置中缺少'strategy'部分")
    
    # 打印策略配置以便于调试
    strategy_config = config.get("strategy", {})
    print(f"策略配置: {strategy_config}")
    
    # 检查open_conditions是否存在
    if "open_conditions" not in strategy_config:
        # 向下兼容：创建默认的open_conditions结构
        strategy_config["open_conditions"] = {
            "condition_type": "funding_only",
            "min_funding_diff": strategy_config.get("min_funding_diff", 0.00001),
            "min_price_diff_percent": strategy_config.get("min_price_diff_percent", 0.2),
            "max_price_diff_percent": 1.0
        }
        logger.warning("配置文件使用旧格式，已自动转换为新格式")
    
    # 检查close_conditions是否存在
    if "close_conditions" not in strategy_config:
        # 创建默认的close_conditions结构
        strategy_config["close_conditions"] = {
            "condition_type": "any",
            "funding_diff_sign_change": True,
            "min_funding_diff": strategy_config["open_conditions"]["min_funding_diff"] / 2,
            "min_profit_percent": 0.1,
            "max_loss_percent": 0.3,
            "max_position_time": 28800  # 默认8小时
        }
        logger.warning("配置文件缺少close_conditions部分，已使用默认值")
    
    # 启动套利引擎
    print("正在启动套利引擎...")
    await arbitrage_engine.start()
    
    # 清理资源
    print("正在清理资源...")
    await backpack_api.close()
    await hyperliquid_api.close()
    
    print("套利机器人已停止")

async def main(test: bool = False, config_path: str = None):
    """
    兼容旧版本的主函数入口点
    
    Args:
        test: 是否为测试模式
        config_path: 配置文件路径
    """
    if config_path:
        config = load_config(config_path)
        await run_bot(config, test_mode=test)
    else:
        parser = argparse.ArgumentParser(description="运行资金费率套利机器人")
        parser.add_argument("--test", action="store_true", help="仅测试API连接")
        parser.add_argument("--config", type=str, help="配置文件路径")
        args = parser.parse_args()
        
        config_path = args.config or "funding_arbitrage_bot/config.yaml"
        config = load_config(config_path)
        await run_bot(config, test_mode=args.test)

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        配置字典
    """
    try:
        # 如果没有指定配置路径，使用默认路径
        if not config_path:
            # 检查用户主目录
            home_config = os.path.expanduser("~/config.yaml")
            if os.path.exists(home_config):
                config_path = home_config
            else:
                # 使用默认配置路径
                config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        
        print(f"加载配置文件: {config_path}")
        
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            
        # 检查TRACE日志级别设置
        if config.get("logging", {}).get("level", "").upper() == "TRACE":
            print("警告: 当前版本不支持TRACE日志级别，将使用DEBUG级别替代")
            config["logging"]["level"] = "DEBUG"
            
        return config
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

if __name__ == "__main__":
    # Windows系统需要设置事件循环策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行主函数
    asyncio.run(main()) 