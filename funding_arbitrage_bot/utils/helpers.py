#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
辅助函数模块

包含各种工具函数，用于支持套利机器人的操作
"""

import os
import yaml
import decimal
from decimal import Decimal
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import logging


def get_backpack_symbol(symbol):
    """获取Backpack格式的交易对"""
    return f"{symbol}_USDC_PERP"


def get_hyperliquid_symbol(symbol):
    """获取Hyperliquid格式的交易对"""
    return symbol


def decimal_adjust(value: float, precision: int, rounding_mode: str = 'ROUND_DOWN') -> float:
    """
    根据指定精度调整数值
    
    Args:
        value: 需要调整的浮点数
        precision: 小数位精度
        rounding_mode: 舍入模式，默认为 'ROUND_DOWN'
    
    Returns:
        调整后的浮点数
    """
    if not hasattr(decimal, rounding_mode):
        raise ValueError(f"无效的舍入模式: {rounding_mode}")
    
    rounding = getattr(decimal, rounding_mode)
    context = decimal.getcontext().copy()
    context.rounding = rounding
    
    quantize_exp = Decimal('0.' + '0' * precision)
    result = Decimal(str(value)).quantize(quantize_exp, context=context)
    
    return float(result)


def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    """
    安全地从嵌套字典中获取值
    
    Args:
        data: 嵌套字典
        keys: 键的路径列表
        default: 如果键不存在时返回的默认值
    
    Returns:
        获取到的值，或默认值
    """
    result = data
    for key in keys:
        if isinstance(result, dict) and key in result:
            result = result[key]
        else:
            return default
    return result


def load_config(config_path: str) -> Dict[str, Any]:
    """
    加载YAML配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    
    Raises:
        FileNotFoundError: 配置文件不存在
        yaml.YAMLError: YAML格式错误
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    
    return config


def calculate_funding_diff(bp_funding: float, hl_funding: float) -> Tuple[float, int]:
    """
    计算资金费率差异和差异符号
    
    由于Hyperliquid的资金费率是每1小时结算一次，而Backpack是每8小时结算一次，
    需要将Hyperliquid的资金费率乘以8进行标准化比较。
    
    Args:
        bp_funding: Backpack资金费率（8小时结算）
        hl_funding: Hyperliquid资金费率（1小时结算）
    
    Returns:
        (资金费率差值, 差值符号(1,-1或0))
    """
    # 将Hyperliquid的资金费率乘以8，以匹配Backpack的8小时周期
    adjusted_hl_funding = hl_funding * 8
    
    # 计算调整后的差异
    diff = bp_funding - adjusted_hl_funding
    
    # 获取差值符号
    if diff > 0:
        sign = 1
    elif diff < 0:
        sign = -1
    else:
        sign = 0
        
    return abs(diff), sign


def get_symbol_from_exchange_symbol(exchange_symbol: str, exchange_type: str) -> Optional[str]:
    """
    从交易所交易对格式获取基础币种
    
    Args:
        exchange_symbol: 交易所格式的交易对，如"BTC_USDC_PERP"或"BTC"
        exchange_type: 交易所类型，"backpack"或"hyperliquid"
    
    Returns:
        基础币种，如"BTC"，如果无法转换则返回None
    """
    if not exchange_symbol:
        return None
        
    if exchange_type.lower() == "backpack":
        # 处理Backpack交易对格式，如"BTC_USDC_PERP"
        if "_" in exchange_symbol:
            # 打印日志以便调试
            print(f"转换Backpack交易对: {exchange_symbol}")
            parts = exchange_symbol.split("_")
            if len(parts) > 0:
                return parts[0]  # 返回第一部分，即基础币种
            return None
    elif exchange_type.lower() == "hyperliquid":
        # Hyperliquid直接使用币种作为交易对
        return exchange_symbol
    
    # 默认情况下尝试直接返回
    return exchange_symbol


def get_hyperliquid_symbol(base_symbol: str) -> str:
    """
    将基础币种名称转换为Hyperliquid交易对格式
    
    Args:
        base_symbol: 基础币种名称，如 "BTC"
    
    Returns:
        Hyperliquid格式的交易对名称，如 "BTC"
    """
    return base_symbol


def get_backpack_symbol(base_symbol: str) -> str:
    """
    将基础币种名称转换为Backpack交易对格式
    
    Args:
        base_symbol: 基础币种名称，如 "BTC"
    
    Returns:
        Backpack格式的交易对名称，如 "BTC_USDC_PERP"
    """
    return f"{base_symbol}_USDC_PERP"


def convert_exchange_positions_to_local(
    bp_positions: Dict[str, Dict[str, Any]], 
    hl_positions: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    将交易所持仓格式转换为本地持仓格式
    
    Args:
        bp_positions: Backpack持仓字典，格式为{"BTC_USDC_PERP": {"size": 0.001, "side": "BUY"}}
        hl_positions: Hyperliquid持仓字典，格式为{"BTC": {"size": 0.001, "side": "BUY"}}
    
    Returns:
        本地持仓格式字典，格式为{"BTC": {"bp_symbol": "BTC_USDC_PERP", "bp_side": "BUY", ...}}
    """
    local_positions = {}
    
    # 处理Backpack持仓
    for bp_symbol, bp_pos in bp_positions.items():
        base_symbol = get_symbol_from_exchange_symbol(bp_symbol, "backpack")
        if not base_symbol:
            continue
            
        if base_symbol not in local_positions:
            local_positions[base_symbol] = {
                "bp_symbol": bp_symbol,
                "bp_side": bp_pos["side"],
                "bp_size": bp_pos["size"],
                "entry_time": datetime.now().isoformat()
            }
    
    # 处理Hyperliquid持仓
    for hl_symbol, hl_pos in hl_positions.items():
        base_symbol = get_symbol_from_exchange_symbol(hl_symbol, "hyperliquid")
        if not base_symbol:
            continue
            
        if base_symbol in local_positions:
            # 如果已经有Backpack持仓，添加Hyperliquid信息
            local_positions[base_symbol].update({
                "hl_symbol": hl_symbol,
                "hl_side": hl_pos["side"],
                "hl_size": hl_pos["size"]
            })
        else:
            # 如果只有Hyperliquid持仓
            local_positions[base_symbol] = {
                "hl_symbol": hl_symbol,
                "hl_side": hl_pos["side"],
                "hl_size": hl_pos["size"],
                "entry_time": datetime.now().isoformat()
            }
    
    # 填充缺失的资金费率信息
    for symbol, pos in local_positions.items():
        if "entry_bp_funding" not in pos:
            pos["entry_bp_funding"] = 0.0
        if "entry_hl_funding" not in pos:
            pos["entry_hl_funding"] = 0.0
        if "entry_funding_diff_sign" not in pos:
            diff, sign = calculate_funding_diff(pos["entry_bp_funding"], pos["entry_hl_funding"])
            pos["entry_funding_diff_sign"] = sign
    
    return local_positions


def configure_logging(
    logger_name: str, 
    log_level: str = "INFO", 
    log_file: Optional[str] = None,
    quiet_loggers: List[str] = None
) -> logging.Logger:
    """
    配置日志记录器
    
    Args:
        logger_name: 日志记录器名称
        log_level: 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
        log_file: 日志文件路径，如果为None则只输出到控制台
        quiet_loggers: 需要静音的日志记录器名称列表（将设置为ERROR级别）
        
    Returns:
        配置好的日志记录器
    """
    # 获取日志级别
    level = getattr(logging, log_level.upper())
    
    # 创建日志记录器
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # 清除可能已存在的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 配置日志格式
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 如果指定了日志文件，添加文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # 静音指定的日志记录器（只记录错误）
    if quiet_loggers:
        for logger_name in quiet_loggers:
            logging.getLogger(logger_name).setLevel(logging.ERROR)
    
    return logger


def round_to_tick(value: float, tick_size: float) -> float:
    """
    将值舍入到指定的刻度大小
    
    Args:
        value: 要舍入的值
        tick_size: 刻度大小
        
    Returns:
        舍入后的值
    """
    return round(value / tick_size) * tick_size


def format_number(value: float, precision: int) -> str:
    """
    将数字格式化为指定精度的字符串
    
    Args:
        value: 要格式化的值
        precision: 小数位数
        
    Returns:
        格式化后的字符串
    """
    format_str = f"{{:.{precision}f}}"
    return format_str.format(value) 