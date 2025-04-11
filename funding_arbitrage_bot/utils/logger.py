#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志配置模块

配置应用程序的日志系统，支持控制台和文件输出
支持日志轮转功能、文件大小限制和备份
"""

import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# 添加TRACE日志级别（比DEBUG更详细）- 暂时注释掉，因为需要重启应用才能生效
# TRACE = 5
# logging.addLevelName(TRACE, "TRACE")

def setup_logger(config: Dict[str, Any], name: str = "funding_arbitrage") -> logging.Logger:
    """
    设置日志记录器，支持日志轮转
    
    Args:
        config: 日志配置字典，包含级别和文件路径
        name: 日志记录器名称
        
    Returns:
        配置好的日志记录器
    """
    # 导入系统输出进行调试
    import sys
    print(f"设置日志记录器: {name}", file=sys.__stdout__)
    
    # 创建日志目录
    log_file = config.get("file", "logs/arbitrage.log")
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 获取日志级别
    log_level_str = config.get("level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # 获取日志轮转配置
    max_bytes = config.get("max_file_size", 10 * 1024 * 1024)  # 默认10MB
    backup_count = config.get("backup_count", 5)  # 默认保留5个备份
    
    # 检查是否完全禁用控制台日志
    disable_console = (
        os.environ.get("DISABLE_CONSOLE_LOGGING", "0") == "1" or
        config.get("disable_console_logging", False)
    )
    
    print(f"控制台日志输出: {'禁用' if disable_console else '启用'}", file=sys.__stdout__)
    print(f"日志轮转设置: 最大大小={max_bytes/1024/1024:.1f}MB, 备份数量={backup_count}", file=sys.__stdout__)
    
    # 移除所有已有的处理器
    # 获取根日志记录器并删除所有已有处理器
    for logger_name in logging.root.manager.loggerDict:
        logger_obj = logging.getLogger(logger_name)
        # 移除所有处理器
        for handler in logger_obj.handlers[:]:
            logger_obj.removeHandler(handler)
    
    # 移除根日志记录器的所有处理器
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 配置日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.handlers = []  # 清除所有处理器
    
    # 添加轮转文件处理器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 阻止日志传递到上层日志记录器
    logger.propagate = False
    
    # 创建空处理器添加到根日志记录器，防止其他地方添加控制台处理器
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
    
    root_handler = NullHandler()
    root_logger.addHandler(root_handler)
    
    # 确保root_logger设置了级别
    root_logger.setLevel(logging.WARNING)
    
    print(f"日志记录器设置完成，日志文件: {log_file}", file=sys.__stdout__)
    
    # 输出日志初始化信息到文件，但不到控制台
    logger.info(f"日志系统已初始化，级别: {log_level_str}, 文件: {log_file}, 轮转: {max_bytes/1024/1024:.1f}MB/{backup_count}个备份")
    
    return logger 