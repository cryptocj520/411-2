#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志工具模块

提供日志频率限制和批量日志处理工具，用于减少冗余日志输出
"""

import time
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime


class RateLimitedLogger:
    """频率限制日志记录器，避免相同类型的日志短时间内重复输出"""
    
    def __init__(self, min_interval_seconds: Dict[str, int] = None):
        """
        初始化频率限制日志记录器
        
        Args:
            min_interval_seconds: 各种日志类型的最小记录间隔（秒），格式为{类型: 间隔}
        """
        self.last_log_times = {}  # 存储每种类型的上次记录时间
        self.min_intervals = min_interval_seconds or {
            "default": 60,         # 默认限制：60秒
            "price_update": 300,   # 价格更新：5分钟
            "connection": 60,      # 连接日志：1分钟
            "api_call": 60,        # API调用：1分钟
            "heartbeat": 300,      # 心跳消息：5分钟
            "websocket": 120,      # WebSocket消息：2分钟
        }
    
    def should_log(self, log_type: str) -> bool:
        """
        检查是否应该记录指定类型的日志
        
        Args:
            log_type: 日志类型标识
            
        Returns:
            如果应该记录返回True，否则返回False
        """
        now = time.time()
        last_time = self.last_log_times.get(log_type, 0)
        interval = self.min_intervals.get(log_type, self.min_intervals["default"])
        
        if now - last_time >= interval:
            self.last_log_times[log_type] = now
            return True
        return False
    
    def log(self, logger: logging.Logger, level: str, log_type: str, message: str, *args, **kwargs) -> None:
        """
        按照频率限制记录日志
        
        Args:
            logger: 日志记录器
            level: 日志级别（'debug', 'info', 'warning', 'error', 'critical'）
            log_type: 日志类型标识
            message: 日志消息
            *args, **kwargs: 传递给日志方法的参数
        """
        if self.should_log(log_type):
            log_method = getattr(logger, level.lower())
            log_method(message, *args, **kwargs)


class LogSummarizer:
    """日志摘要生成器，收集一段时间内的日志并生成摘要"""
    
    def __init__(self, logger: logging.Logger, interval_seconds: int = 300):
        """
        初始化日志摘要生成器
        
        Args:
            logger: 日志记录器
            interval_seconds: 摘要生成间隔（秒）
        """
        self.logger = logger
        self.interval = interval_seconds
        self.last_summary_time = time.time()
        
        # 收集各种事件的临时存储
        self.price_updates = {}  # {symbol: [(old_price, new_price, timestamp), ...]}
        self.funding_updates = {}  # {symbol: (rate, timestamp)}
        self.api_calls = {"success": 0, "failed": 0}
        self.errors = {}  # {error_type: count}
        self.connection_events = {"connect": 0, "disconnect": 0}
    
    def record_price_update(self, symbol: str, exchange: str, old_price: float, new_price: float) -> None:
        """
        记录价格更新事件
        
        Args:
            symbol: 币种符号
            exchange: 交易所名称
            old_price: 旧价格
            new_price: 新价格
        """
        key = f"{exchange}_{symbol}"
        if key not in self.price_updates:
            self.price_updates[key] = []
        
        self.price_updates[key].append((old_price, new_price, time.time()))
        self._check_summary()
    
    def record_funding_update(self, symbol: str, exchange: str, rate: float) -> None:
        """
        记录资金费率更新事件
        
        Args:
            symbol: 币种符号
            exchange: 交易所名称
            rate: 资金费率
        """
        key = f"{exchange}_{symbol}"
        self.funding_updates[key] = (rate, time.time())
        self._check_summary()
    
    def record_api_call(self, success: bool = True) -> None:
        """
        记录API调用结果
        
        Args:
            success: 调用是否成功
        """
        if success:
            self.api_calls["success"] += 1
        else:
            self.api_calls["failed"] += 1
        self._check_summary()
    
    def record_error(self, error_type: str) -> None:
        """
        记录错误事件
        
        Args:
            error_type: 错误类型
        """
        if error_type not in self.errors:
            self.errors[error_type] = 0
        
        self.errors[error_type] += 1
        self._check_summary()
    
    def record_connection_event(self, event_type: str) -> None:
        """
        记录连接事件
        
        Args:
            event_type: 事件类型（'connect'或'disconnect'）
        """
        if event_type in self.connection_events:
            self.connection_events[event_type] += 1
        self._check_summary()
    
    def _check_summary(self) -> None:
        """检查是否应该生成摘要"""
        now = time.time()
        if now - self.last_summary_time >= self.interval:
            self._generate_summary()
            self.last_summary_time = now
    
    def _generate_summary(self) -> None:
        """生成并记录摘要日志"""
        # 处理价格更新摘要
        if self.price_updates:
            significant_updates = []
            for key, updates in self.price_updates.items():
                if not updates:
                    continue
                    
                exchange, symbol = key.split("_")
                first_update = updates[0]
                last_update = updates[-1]
                first_price = first_update[0] or 0  # 处理None值
                last_price = last_update[1]
                update_count = len(updates)
                
                # 计算价格变化百分比
                if first_price > 0:
                    change_pct = ((last_price - first_price) / first_price) * 100
                    # 只记录有显著变化的价格
                    if abs(change_pct) > 0.5 or update_count > 10:  # 变化超过0.5%或者更新次数多
                        significant_updates.append((key, first_price, last_price, change_pct, update_count))
            
            # 按变化幅度排序，显示最显著的变化
            significant_updates.sort(key=lambda x: abs(x[3]), reverse=True)
            top_updates = significant_updates[:5]  # 最多显示5个显著变化
            
            if top_updates:
                updates_text = []
                for key, first, last, change, count in top_updates:
                    exchange, symbol = key.split("_")
                    updates_text.append(f"{exchange}/{symbol}: {first:.2f}→{last:.2f} ({change:+.2f}%, {count}次)")
                
                more_count = len(significant_updates) - len(top_updates)
                more_text = f" 及其他{more_count}个交易对有变化" if more_count > 0 else ""
                
                self.logger.info(f"价格显著变化: {', '.join(updates_text)}{more_text}")
        
        # 处理资金费率摘要
        if self.funding_updates:
            updates_text = []
            sorted_items = sorted(self.funding_updates.items(), 
                                key=lambda x: abs(x[1][0]), 
                                reverse=True)[:5]  # 按费率绝对值排序
            
            for key, (rate, _) in sorted_items:
                exchange, symbol = key.split("_")
                updates_text.append(f"{exchange}/{symbol}: {rate:+.6f}")
            
            more_count = len(self.funding_updates) - len(sorted_items)
            more_text = f" 及其他{more_count}个更新" if more_count > 0 else ""
            
            self.logger.info(f"资金费率更新: {', '.join(updates_text)}{more_text}")
        
        # 处理API调用摘要
        if self.api_calls["success"] > 0 or self.api_calls["failed"] > 0:
            self.logger.info(f"API调用统计: 成功 {self.api_calls['success']}次, 失败 {self.api_calls['failed']}次")
            self.api_calls = {"success": 0, "failed": 0}
        
        # 处理错误摘要
        if self.errors:
            error_texts = []
            for error_type, count in self.errors.items():
                error_texts.append(f"{error_type}: {count}次")
            
            self.logger.warning(f"错误统计: {', '.join(error_texts)}")
            self.errors = {}
        
        # 处理连接事件摘要
        if self.connection_events["connect"] > 0 or self.connection_events["disconnect"] > 0:
            self.logger.info(f"连接事件统计: 连接 {self.connection_events['connect']}次, 断开 {self.connection_events['disconnect']}次")
            self.connection_events = {"connect": 0, "disconnect": 0}
        
        # 清空收集的数据
        self.price_updates = {}
        self.funding_updates = {}
        
    def force_summary(self) -> None:
        """强制生成摘要，不考虑时间间隔"""
        self._generate_summary()
        self.last_summary_time = time.time() 