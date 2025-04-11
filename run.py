#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
资金费率套利机器人主运行文件
"""

import os
import sys
import asyncio
import logging
import yaml
from logging.handlers import RotatingFileHandler
from datetime import datetime

from funding_arbitrage_bot.strategies.funding_arbitrage import FundingArbitrageStrategy


def setup_logging(config):
    """设置日志系统"""
    # 获取日志配置
    log_config = config.get("logging", {})
    log_level_str = log_config.get("level", "INFO")
    log_file = log_config.get("file", "arbitrage.log")
    max_size_mb = log_config.get("max_size_mb", 10)
    backup_count = log_config.get("backup_count", 5)
    
    # 转换日志级别字符串到日志级别
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    # 创建日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 清除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 创建文件处理器 (如果配置了日志文件)
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # 使用RotatingFileHandler以便轮转日志
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # 记录日志配置信息
    root_logger.info(f"日志级别设置为: {log_level_str}")
    root_logger.info(f"日志文件: {log_file}")
    
    return root_logger


async def main():
    """主函数"""
    # 加载配置
    config_path = "funding_arbitrage_bot/config.yaml"
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    
    # 设置日志系统
    logger = setup_logging(config)
    logger.info("启动资金费率套利程序...")
    
    # 记录启动详情
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"启动时间: {current_time}")
    
    # 获取并记录策略配置
    strategy_config = config.get("strategy", {})
    execution_mode = strategy_config.get("execution_mode", "simulate")
    min_funding_diff = strategy_config.get("min_funding_diff", 0.01)
    coins = strategy_config.get("coins", ["BTC", "ETH", "SOL"])
    
    logger.info(f"执行模式: {execution_mode}")
    logger.info(f"最小资金费率差异阈值: {min_funding_diff}")
    logger.info(f"监控币种: {', '.join(coins)}")
    
    # 获取并记录流动性分析参数
    max_slippage_pct = strategy_config.get("max_slippage_pct", 0.1)
    min_liquidity_ratio = strategy_config.get("min_liquidity_ratio", 3.0)
    logger.info(f"最大允许滑点: {max_slippage_pct}%")
    logger.info(f"最小流动性比率: {min_liquidity_ratio}倍")
    
    # 初始化显示管理器
    from funding_arbitrage_bot.utils.display_manager import DisplayManager
    display_manager = DisplayManager(logger=logger)
    display_manager.start()
    
    # 初始化策略
    strategy = FundingArbitrageStrategy(config=config, logger=logger, display_manager=display_manager)
    logger.info("策略初始化完成")
    
    # 开始连接交易所
    logger.info("正在连接交易所...")
    
    try:
        # 连接Hyperliquid WebSocket
        await strategy.hyperliquid_api.connect_websocket()
        logger.info("Hyperliquid连接成功")
        
        # 连接Backpack API (可能不需要WebSocket连接)
        logger.info("Backpack API就绪")
        
        # 运行策略
        await strategy.run()
    
    except KeyboardInterrupt:
        logger.info("接收到停止信号，正在关闭...")
    except Exception as e:
        logger.exception(f"运行出错: {e}")
    finally:
        # 关闭连接
        logger.info("正在关闭连接...")
        await strategy.hyperliquid_api.close()
        await strategy.backpack_api.close()
        
        # 停止显示管理器
        if display_manager:
            display_manager.stop()
            
        logger.info("程序已停止")


if __name__ == "__main__":
    asyncio.run(main()) 