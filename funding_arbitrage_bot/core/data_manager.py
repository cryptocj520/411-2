#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据管理模块

管理从两个交易所获取的价格和资金费率数据
集成了日志频率限制和批量摘要处理
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from funding_arbitrage_bot.exchanges.backpack_api import BackpackAPI
from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
from funding_arbitrage_bot.utils.helpers import get_backpack_symbol, get_hyperliquid_symbol
from funding_arbitrage_bot.utils.log_utilities import RateLimitedLogger, LogSummarizer


class DataManager:
    """数据管理类，负责获取和管理交易所数据"""
    
    def __init__(
        self, 
        backpack_api: BackpackAPI, 
        hyperliquid_api: HyperliquidAPI,
        symbols: List[str],
        funding_update_interval: int = 60,
        logger: Optional[logging.Logger] = None,
        log_config: Optional[Dict] = None
    ):
        """
        初始化数据管理器
        
        Args:
            backpack_api: Backpack API实例
            hyperliquid_api: Hyperliquid API实例
            symbols: 需要监控的基础币种列表，如 ["BTC", "ETH", "SOL"]
            funding_update_interval: 资金费率更新间隔（秒）
            logger: 日志记录器
            log_config: 日志配置选项
        """
        self.backpack_api = backpack_api
        self.hyperliquid_api = hyperliquid_api
        self.symbols = symbols
        self.funding_update_interval = funding_update_interval
        self.logger = logger or logging.getLogger(__name__)
        
        # 设置日志工具
        log_config = log_config or {}
        self.rate_logger = RateLimitedLogger(min_interval_seconds=log_config.get("throttling", {}))
        self.log_summarizer = LogSummarizer(
            logger=self.logger, 
            interval_seconds=log_config.get("throttling", {}).get("summary_interval", 900)
        )
        
        # 检查API实例是否有效
        self.backpack_available = self.backpack_api is not None
        self.hyperliquid_available = self.hyperliquid_api is not None
        
        if not self.backpack_available:
            self.logger.warning("Backpack API实例无效，将无法获取Backpack数据")
        
        if not self.hyperliquid_available:
            self.logger.warning("Hyperliquid API实例无效，将无法获取Hyperliquid数据")
        
        # 设置Hyperliquid需要获取价格的币种列表
        if self.hyperliquid_available:
            self.hyperliquid_api.set_price_coins(symbols)
        
        # 初始化数据存储
        self.latest_data = {}
        self._init_data_structure()
        
        # 资金费率更新任务
        self.funding_update_task = None
        
    def _init_data_structure(self):
        """初始化数据结构"""
        # 初始化币种映射关系
        self.backpack_symbols_map = {}
        for symbol in self.symbols:
            # 对应Backpack的交易对格式
            bp_symbol = get_backpack_symbol(symbol)
            if bp_symbol not in self.backpack_symbols_map:
                self.backpack_symbols_map[symbol] = [bp_symbol]
            else:
                self.backpack_symbols_map[symbol].append(bp_symbol)
                
        # 初始化数据存储结构
        for symbol in self.symbols:
            self.latest_data[symbol] = {
                "backpack": {
                    "symbol": get_backpack_symbol(symbol),
                    "price": None,
                    "funding_rate": None,
                    "price_timestamp": None,
                    "funding_timestamp": None
                },
                "hyperliquid": {
                    "symbol": get_hyperliquid_symbol(symbol),
                    "price": None,
                    "funding_rate": None,
                    "adjusted_funding_rate": None,
                    "price_timestamp": None,
                    "funding_timestamp": None
                }
            }
    
    async def start_price_feeds(self):
        """启动价格数据流"""
        # 启动WebSocket价格流
        if self.backpack_available:
            await self.backpack_api.start_ws_price_stream()
        
        if self.hyperliquid_available:
            await self.hyperliquid_api.start_ws_price_stream()
        
        # 启动资金费率定期更新
        if not self.funding_update_task:
            self.funding_update_task = asyncio.create_task(self._update_funding_rates_loop())
    
    async def _update_funding_rates_loop(self):
        """资金费率定期更新循环"""
        while True:
            try:
                start_time = time.time()
                await self.update_funding_rates()
            except Exception as e:
                self.logger.error(f"更新资金费率失败: {e}")
                self.log_summarizer.record_error("funding_rate_update")
            finally:
                # 计算已经花费的时间
                elapsed = time.time() - start_time
                # 确保更新间隔至少为配置的秒数
                sleep_time = max(1, self.funding_update_interval - elapsed)
                # 记录等待信息（使用频率限制）
                self.rate_logger.log(
                    self.logger, "debug", "funding_wait", 
                    f"等待{sleep_time:.1f}秒后进行下一次资金费率更新"
                )
                # 等待下一次更新
                await asyncio.sleep(sleep_time)
    
    async def update_funding_rates(self):
        """更新所有交易对的资金费率"""
        try:
            # 尝试使用批量方式获取Hyperliquid资金费率
            if self.hyperliquid_api:
                try:
                    # 检查是否有批量获取方法
                    if hasattr(self.hyperliquid_api, 'get_all_funding_rates'):
                        self.logger.info("尝试批量获取Hyperliquid资金费率...")
                        hl_rates = await self.hyperliquid_api.get_all_funding_rates()
                        
                        if hl_rates and len(hl_rates) > 0:  # 如果成功获取数据
                            self.logger.info(f"批量获取Hyperliquid资金费率成功，共{len(hl_rates)}个")
                            
                            # 更新数据存储
                            updated_count = 0
                            for symbol in self.symbols:
                                if symbol in hl_rates:
                                    rate = hl_rates[symbol]
                                    self.latest_data[symbol]["hyperliquid"]["funding_rate"] = rate
                                    # 计算调整后的资金费率（乘以8）
                                    self.latest_data[symbol]["hyperliquid"]["adjusted_funding_rate"] = rate * 8
                                    self.latest_data[symbol]["hyperliquid"]["funding_timestamp"] = datetime.now()
                                    updated_count += 1
                                    
                                    # 使用摘要记录器收集资金费率信息
                                    self.log_summarizer.record_funding_update(
                                        symbol, "Hyperliquid", rate * 8
                                    )
                                    
                                    # 只有当资金费率相对较高时才单独记录
                                    if abs(rate) > 0.0001:
                                        self.rate_logger.log(
                                            self.logger, "debug", f"hl_funding_{symbol}",
                                            f"较高Hyperliquid {symbol}资金费率: {rate}(1h), 调整后: {rate * 8}(8h)"
                                        )
                            
                            self.logger.info(f"成功批量更新 {updated_count}/{len(self.symbols)} 个Hyperliquid币种的资金费率")
                        else:
                            self.logger.warning("批量获取Hyperliquid资金费率结果为空，将使用单个请求模式")
                    else:
                        self.logger.debug("Hyperliquid API不支持批量获取资金费率，将使用单个请求模式")
                        
                except Exception as e:
                    self.logger.error(f"批量获取Hyperliquid资金费率失败: {e}")
                    self.logger.info("将使用单个请求模式获取Hyperliquid资金费率")
            
            # 尝试使用批量方式获取Backpack资金费率
            if self.backpack_api:
                try:
                    # 检查是否有批量获取方法
                    if hasattr(self.backpack_api, 'get_all_funding_rates'):
                        self.logger.info("尝试批量获取Backpack资金费率...")
                        bp_rates = await self.backpack_api.get_all_funding_rates()
                        
                        if bp_rates and len(bp_rates) > 0:  # 如果成功获取数据
                            self.logger.info(f"批量获取Backpack资金费率成功，共{len(bp_rates)}个")
                            
                            # 更新数据存储
                            updated_count = 0
                            for symbol in self.symbols:
                                # 在Backpack中查找匹配的交易对
                                bp_symbol = get_backpack_symbol(symbol)
                                if bp_symbol in bp_rates:
                                    rate = bp_rates[bp_symbol]
                                    self.latest_data[symbol]["backpack"]["funding_rate"] = rate
                                    self.latest_data[symbol]["backpack"]["funding_timestamp"] = datetime.now()
                                    updated_count += 1
                                    
                                    # 使用摘要记录器收集资金费率信息
                                    self.log_summarizer.record_funding_update(
                                        symbol, "Backpack", rate
                                    )
                                    
                                    # 只有当资金费率相对较高时才单独记录
                                    if abs(rate) > 0.0001:
                                        self.rate_logger.log(
                                            self.logger, "debug", f"bp_funding_{symbol}",
                                            f"较高Backpack {bp_symbol}资金费率: {rate}"
                                        )
                            
                            self.logger.info(f"成功批量更新 {updated_count}/{len(self.symbols)} 个Backpack币种的资金费率")
                        else:
                            self.logger.warning("批量获取Backpack资金费率结果为空，将使用单个请求模式")
                    else:
                        self.logger.debug("Backpack API不支持批量获取资金费率，将使用单个请求模式")
                        
                except Exception as e:
                    self.logger.error(f"批量获取Backpack资金费率失败: {e}")
                    self.logger.info("将使用单个请求模式获取Backpack资金费率")
            
            # 如果当前没有任何资金费率数据，则使用传统的单个请求获取
            need_traditional_update = False
            
            for symbol in self.symbols:
                hl_data = self.latest_data[symbol]["hyperliquid"]
                bp_data = self.latest_data[symbol]["backpack"]
                
                # 检查是否有任何交易所缺少数据
                if hl_data["funding_rate"] is None or bp_data["funding_rate"] is None:
                    need_traditional_update = True
                    break
            
            # 如果需要，使用传统的单个请求获取方式（保留原有逻辑）
            if need_traditional_update:
                self.logger.info("检测到部分币种缺失资金费率数据，将使用单个请求模式补充")
                for symbol in self.symbols:
                    # 尝试从Hyperliquid获取资金费率(针对缺失的数据)
                    if self.hyperliquid_api and self.latest_data[symbol]["hyperliquid"]["funding_rate"] is None:
                        try:
                            funding_rate = await self.hyperliquid_api.get_funding_rate(symbol)
                            # 更新资金费率
                            if funding_rate is not None:
                                self.latest_data[symbol]["hyperliquid"]["funding_rate"] = funding_rate
                                # 计算调整后的资金费率（乘以8）
                                self.latest_data[symbol]["hyperliquid"]["adjusted_funding_rate"] = funding_rate * 8
                                self.latest_data[symbol]["hyperliquid"]["funding_timestamp"] = datetime.now()
                                
                                # 使用摘要记录器收集资金费率信息
                                self.log_summarizer.record_funding_update(
                                    symbol, "Hyperliquid", funding_rate * 8
                                )
                                
                                # 只有当资金费率相对较高时才单独记录
                                if abs(funding_rate) > 0.0001:
                                    self.rate_logger.log(
                                        self.logger, "debug", f"hl_funding_{symbol}",
                                        f"较高Hyperliquid {symbol}资金费率: {funding_rate}(1h), 调整后: {funding_rate * 8}(8h)"
                                    )
                        except Exception as e:
                            self.logger.error(f"获取Hyperliquid {symbol}资金费率失败: {e}")
                            self.log_summarizer.record_error("hl_funding_error")
                    
                    # 尝试从Backpack获取资金费率(针对缺失的数据)
                    if self.backpack_api and self.latest_data[symbol]["backpack"]["funding_rate"] is None:
                        try:
                            # 在Backpack中查找匹配的交易对
                            for backpack_symbol in self.backpack_symbols_map.get(symbol, []):
                                funding_rate = await self.backpack_api.get_funding_rate(backpack_symbol)
                                # 更新资金费率
                                if funding_rate is not None:
                                    self.latest_data[symbol]["backpack"]["funding_rate"] = funding_rate
                                    self.latest_data[symbol]["backpack"]["funding_timestamp"] = datetime.now()
                                    
                                    # 使用摘要记录器收集资金费率信息
                                    self.log_summarizer.record_funding_update(
                                        symbol, "Backpack", funding_rate
                                    )
                                    
                                    # 只有当资金费率相对较高时才单独记录
                                    if abs(funding_rate) > 0.0001:
                                        self.rate_logger.log(
                                            self.logger, "debug", f"bp_funding_{symbol}",
                                            f"较高Backpack {backpack_symbol}资金费率: {funding_rate}"
                                        )
                        except Exception as e:
                            self.logger.error(f"获取Backpack {symbol}资金费率失败: {e}")
                            self.log_summarizer.record_error("bp_funding_error")
            
            # 使用频率限制记录更新完成信息
            self.rate_logger.log(self.logger, "info", "funding_complete", "资金费率更新完成")
        except Exception as e:
            self.logger.error(f"更新资金费率过程中发生错误: {e}")
            self.log_summarizer.record_error("funding_update_error")
    
    async def update_prices(self):
        """更新所有交易对的价格数据"""
        try:
            for symbol in self.symbols:
                # 尝试从Hyperliquid获取价格
                if self.hyperliquid_api:
                    try:
                        price = await self.hyperliquid_api.get_price(symbol)
                        # 更新价格
                        if price is not None:
                            # 检查是否有显著变化
                            old_price = self.latest_data[symbol]["hyperliquid"]["price"]
                            self.latest_data[symbol]["hyperliquid"]["price"] = price
                            self.latest_data[symbol]["hyperliquid"]["price_timestamp"] = datetime.now()
                            
                            # 记录到价格摘要收集器
                            self.log_summarizer.record_price_update(
                                symbol, "Hyperliquid", old_price, price
                            )
                            
                            # 只有当价格变化超过1%时才单独记录日志
                            if old_price is not None and abs((price - old_price) / old_price) > 0.01:
                                self.rate_logger.log(
                                    self.logger, "debug", f"hl_price_{symbol}",
                                    f"Hyperliquid {symbol}价格显著变化: {old_price:.4f} → {price:.4f} " +
                                    f"({((price-old_price)/old_price*100):.2f}%)"
                                )
                    except Exception as e:
                        self.logger.error(f"获取Hyperliquid {symbol}价格失败: {e}")
                        self.log_summarizer.record_error("hl_price_error")
                
                # 尝试从Backpack获取价格
                if self.backpack_api:
                    try:
                        # 在Backpack中查找匹配的交易对
                        for backpack_symbol in self.backpack_symbols_map.get(symbol, []):
                            price = await self.backpack_api.get_price(backpack_symbol)
                            # 更新价格
                            if price is not None:
                                # 检查是否有显著变化
                                old_price = self.latest_data[symbol]["backpack"]["price"]
                                self.latest_data[symbol]["backpack"]["price"] = price
                                self.latest_data[symbol]["backpack"]["price_timestamp"] = datetime.now()
                                
                                # 记录到价格摘要收集器
                                self.log_summarizer.record_price_update(
                                    symbol, "Backpack", old_price, price
                                )
                                
                                # 只有当价格变化超过1%时才单独记录日志
                                if old_price is not None and abs((price - old_price) / old_price) > 0.01:
                                    self.rate_logger.log(
                                        self.logger, "debug", f"bp_price_{symbol}",
                                        f"Backpack {backpack_symbol}价格显著变化: {old_price:.4f} → {price:.4f} " +
                                        f"({((price-old_price)/old_price*100):.2f}%)"
                                    )
                    except Exception as e:
                        self.logger.error(f"获取Backpack {symbol}价格失败: {e}")
                        self.log_summarizer.record_error("bp_price_error")
        except Exception as e:
            self.logger.error(f"更新价格过程中发生错误: {e}")
            self.log_summarizer.record_error("price_update_error")
    
    async def get_data(self, symbol: str) -> Dict:
        """
        获取指定币种的最新数据
        
        Args:
            symbol: 基础币种，如 "BTC"
            
        Returns:
            包含价格和资金费率的数据字典
        """
        # 确保价格是最新的
        await self.update_prices()
        
        return self.latest_data.get(symbol, {})
    
    async def get_latest_data(self, symbol: str) -> Dict:
        """
        获取指定币种的最新数据（get_data的别名，为了兼容性）
        
        Args:
            symbol: 基础币种，如 "BTC"
            
        Returns:
            包含价格和资金费率的数据字典
        """
        return await self.get_data(symbol)
    
    def get_all_data(self) -> Dict:
        """
        获取所有币种的最新数据
        
        Returns:
            所有币种的数据字典
        """
        return self.latest_data
    
    def is_data_valid(self, symbol: str, max_age_seconds: int = 300) -> bool:
        """
        检查数据是否有效（不太旧）
        
        Args:
            symbol: 基础币种，如 "BTC"
            max_age_seconds: 数据最大有效期（秒）
            
        Returns:
            如果数据有效返回True，否则返回False
        """
        if symbol not in self.latest_data:
            return False
        
        data = self.latest_data[symbol]
        now = datetime.now()
        
        # 如果某个交易所不可用，则忽略其数据检查
        backpack_check = True
        hyperliquid_check = True
        
        # 检查Backpack数据
        if self.backpack_available:
            bp_data = data["backpack"]
            if (bp_data["price"] is None or 
                bp_data["funding_rate"] is None or 
                bp_data["price_timestamp"] is None or 
                bp_data["funding_timestamp"] is None):
                backpack_check = False
            else:
                bp_price_age = (now - bp_data["price_timestamp"]).total_seconds()
                bp_funding_age = (now - bp_data["funding_timestamp"]).total_seconds()
                
                if bp_price_age > max_age_seconds or bp_funding_age > max_age_seconds:
                    backpack_check = False
        
        # 检查Hyperliquid数据
        if self.hyperliquid_available:
            hl_data = data["hyperliquid"]
            if (hl_data["price"] is None or 
                hl_data["funding_rate"] is None or 
                hl_data["price_timestamp"] is None or 
                hl_data["funding_timestamp"] is None):
                hyperliquid_check = False
            else:
                hl_price_age = (now - hl_data["price_timestamp"]).total_seconds()
                hl_funding_age = (now - hl_data["funding_timestamp"]).total_seconds()
                
                if hl_price_age > max_age_seconds or hl_funding_age > max_age_seconds:
                    hyperliquid_check = False
        
        # 根据可用交易所的数量返回结果
        if self.backpack_available and self.hyperliquid_available:
            # 两个交易所都可用时，需要两个都有效
            return backpack_check and hyperliquid_check
        elif self.backpack_available:
            # 只有Backpack可用时，只检查Backpack
            return backpack_check
        elif self.hyperliquid_available:
            # 只有Hyperliquid可用时，只检查Hyperliquid
            return hyperliquid_check
        else:
            # 都不可用时返回False
            return False
    
    async def close(self):
        """关闭数据管理器"""
        # 强制生成最终摘要
        self.log_summarizer.force_summary()
        
        # 取消资金费率更新任务
        if self.funding_update_task:
            self.funding_update_task.cancel()
            try:
                await self.funding_update_task
            except asyncio.CancelledError:
                pass 