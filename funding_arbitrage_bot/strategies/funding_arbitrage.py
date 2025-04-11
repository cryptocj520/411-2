import time
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from datetime import datetime, timezone
import numpy as np
import sys

from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidApi
from funding_arbitrage_bot.exchanges.backpack_api import BackpackApi

class FundingArbitrageStrategy:
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None, display_manager = None):
        """
        资金费率套利策略初始化
        
        Args:
            config: 配置字典
            logger: 日志记录器
            display_manager: 显示管理器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = config
        self.display_manager = display_manager
        
        # 创建交易所API实例
        self.hyperliquid_api = HyperliquidApi(
            config=config,
            logger=self.logger
        )
        
        self.backpack_api = BackpackApi(
            config=config,
            logger=self.logger
        )
        
        # 提取配置
        strategy_config = config.get("strategy", {})
        
        # 交易配置参数
        self.min_funding_diff = strategy_config.get("min_funding_diff", 0.01)  # 最小资金费率差异
        self.min_price_diff_pct = strategy_config.get("min_price_diff_pct", 0.001)  # 最小价格差异百分比
        self.coins_to_monitor = strategy_config.get("coins", ["BTC", "ETH", "SOL"])  # 监控的币种
        
        # 资金费率套利参数
        self.trade_size_usd = strategy_config.get("trade_size_usd", 100)  # 每笔交易的美元价值
        self.max_position_usd = strategy_config.get("max_position_usd", 500)  # 每个币种的最大仓位
        self.trade_timeout = strategy_config.get("trade_timeout", 30)  # 交易超时时间(秒)
        
        # 订单深度和滑点设置
        self.max_slippage_pct = strategy_config.get("max_slippage_pct", 0.1)  # 最大允许滑点百分比
        self.min_liquidity_ratio = strategy_config.get("min_liquidity_ratio", 3.0)  # 最小流动性比率(交易金额的倍数)
        
        # 价格更新和检查设置
        self.price_check_interval = strategy_config.get("price_check_interval", 5)  # 价格检查间隔(秒)
        self.arbitrage_check_interval = strategy_config.get("arbitrage_check_interval", 60)  # 套利检查间隔(秒)
        
        # 运行控制
        self.is_running = False
        self.last_check_time = 0
        self.last_trade_time = {}  # 币种 -> 上次交易时间
        self.trade_cooldown = strategy_config.get("trade_cooldown", 3600)  # 交易冷却时间(秒)
        
        # 执行模式
        self.execution_mode = strategy_config.get("execution_mode", "simulate")  # simulate, live
        
        # 记录器
        self.trade_history = []
        
        # 设置价格监控的币种列表
        self.hyperliquid_api.price_coins = self.coins_to_monitor
        
        # 统计信息
        self.stats = {
            "checks": 0,
            "opportunities_found": 0,
            "trades_executed": 0,
            "total_profit_usd": 0,
            "start_time": time.time()
        }
        
        # 初始化市场数据字典
        self.market_data = {}

    async def analyze_liquidity(self, coin: str) -> Dict[str, Any]:
        """
        分析指定币种在两个交易所的流动性情况和可能的滑点
        
        Args:
            coin: 币种名称
        
        Returns:
            包含两个交易所流动性分析结果的字典
        """
        try:
            results = {}
            
            # 获取Hyperliquid订单深度数据并分析
            hl_orderbook = await self.hyperliquid_api.get_orderbook(coin)
            hl_price = self.hyperliquid_api.prices.get(coin)
            hl_analysis = await self._analyze_single_exchange_liquidity(
                "hyperliquid", coin, hl_orderbook, hl_price
            )
            results["hyperliquid"] = hl_analysis
            
            # 获取Backpack订单深度数据并分析
            bp_orderbook = await self.backpack_api.get_orderbook(coin)
            bp_price = await self.backpack_api.get_price(coin)
            bp_analysis = await self._analyze_single_exchange_liquidity(
                "backpack", coin, bp_orderbook, bp_price
            )
            results["backpack"] = bp_analysis
            
            # 综合评估两个交易所的流动性情况
            has_sufficient_liquidity = (
                hl_analysis.get("has_sufficient_liquidity", False) and
                bp_analysis.get("has_sufficient_liquidity", False)
            )
            
            results["combined"] = {
                "has_sufficient_liquidity": has_sufficient_liquidity,
                "issues": []
            }
            
            # 记录任何流动性问题
            if not hl_analysis.get("has_sufficient_liquidity", False):
                results["combined"]["issues"].append(
                    f"Hyperliquid流动性不足: {hl_analysis.get('error', '未知原因')}"
                )
                
            if not bp_analysis.get("has_sufficient_liquidity", False):
                results["combined"]["issues"].append(
                    f"Backpack流动性不足: {bp_analysis.get('error', '未知原因')}"
                )
            
            # 确定做多和做空的交易所
            hl_funding = self.funding_rates.get(f"HL_{coin}", 0)
            bp_funding = self.funding_rates.get(f"BP_{coin}", 0)
            funding_diff = hl_funding - bp_funding if hl_funding is not None and bp_funding is not None else 0
            
            long_exchange = "hyperliquid" if funding_diff < 0 else "backpack"
            short_exchange = "backpack" if funding_diff < 0 else "hyperliquid"
            
            # 获取滑点信息
            long_analysis = results.get(long_exchange, {})
            short_analysis = results.get(short_exchange, {})
            
            long_slippage = long_analysis.get("bid_slippage_pct", 0)
            short_slippage = short_analysis.get("ask_slippage_pct", 0)
            total_slippage = long_slippage + short_slippage
            
            # 将滑点信息添加到results中
            results["long_exchange"] = long_exchange
            results["short_exchange"] = short_exchange
            results["long_slippage"] = long_slippage
            results["short_slippage"] = short_slippage
            results["total_slippage"] = total_slippage
            
            # 将滑点信息添加到市场数据中
            if hasattr(self, "market_data") and coin in self.market_data:
                self.market_data[coin]["total_slippage"] = total_slippage
                self.market_data[coin]["long_slippage"] = long_slippage
                self.market_data[coin]["short_slippage"] = short_slippage
                self.market_data[coin]["liquidity_analysis"] = results
                self.logger.debug(f"在流动性分析中添加{coin}的滑点信息: total_slippage={total_slippage}")
                
                # 如果有display_manager，立即更新显示
                if hasattr(self, "display_manager") and self.display_manager:
                    self.display_manager.update_market_data(self.market_data)
            
            return results
            
        except Exception as e:
            self.logger.error(f"分析{coin}流动性出错: {e}")
            return {
                "combined": {
                    "has_sufficient_liquidity": False,
                    "error": f"流动性分析错误: {e}"
                }
            }
    
    async def _analyze_single_exchange_liquidity(
        self, exchange: str, coin: str, orderbook: Dict, current_price: float
    ) -> Dict[str, Any]:
        """
        分析单个交易所的流动性情况
        
        Args:
            exchange: 交易所名称
            coin: 币种名称
            orderbook: 订单深度数据
            current_price: 当前价格
            
        Returns:
            包含流动性分析结果的字典
        """
        if not orderbook or not current_price:
            return {
                "has_sufficient_liquidity": False,
                "error": f"无法获取{exchange}的{coin}订单深度数据或价格"
            }
        
        try:
            # 计算买入/卖出所需的金额
            trade_size_usd = self.trade_size_usd
            trade_size_coin = trade_size_usd / current_price
            
            # 分析买入深度
            bid_liquidity = 0
            bid_executed_price = 0
            bid_slippage = 0
            bid_remaining = 0
            
            if orderbook["bids"]:
                # 计算买入执行价格和滑点
                remaining_size = trade_size_coin
                weighted_price_sum = 0
                
                for bid in orderbook["bids"]:
                    price = float(bid["px"])
                    size = float(bid["sz"])
                    
                    if remaining_size <= 0:
                        break
                    
                    executed_size = min(size, remaining_size)
                    weighted_price_sum += price * executed_size
                    bid_liquidity += price * size
                    remaining_size -= executed_size
                
                if remaining_size <= 0:
                    bid_executed_price = weighted_price_sum / trade_size_coin
                    bid_slippage = (current_price - bid_executed_price) / current_price * 100
                else:
                    bid_remaining = remaining_size
            
            # 分析卖出深度
            ask_liquidity = 0
            ask_executed_price = 0
            ask_slippage = 0
            ask_remaining = 0
            
            if orderbook["asks"]:
                # 计算卖出执行价格和滑点
                remaining_size = trade_size_coin
                weighted_price_sum = 0
                
                for ask in orderbook["asks"]:
                    price = float(ask["px"])
                    size = float(ask["sz"])
                    
                    if remaining_size <= 0:
                        break
                    
                    executed_size = min(size, remaining_size)
                    weighted_price_sum += price * executed_size
                    ask_liquidity += price * size
                    remaining_size -= executed_size
                
                if remaining_size <= 0:
                    ask_executed_price = weighted_price_sum / trade_size_coin
                    ask_slippage = (ask_executed_price - current_price) / current_price * 100
                else:
                    ask_remaining = remaining_size
            
            # 判断流动性是否充足
            has_sufficient_liquidity = (
                bid_liquidity >= trade_size_usd * self.min_liquidity_ratio and
                ask_liquidity >= trade_size_usd * self.min_liquidity_ratio and
                bid_slippage <= self.max_slippage_pct and
                ask_slippage <= self.max_slippage_pct and
                bid_remaining <= 0 and
                ask_remaining <= 0
            )
            
            # 如果流动性不足，确定具体原因
            issues = []
            if bid_liquidity < trade_size_usd * self.min_liquidity_ratio:
                issues.append(f"买单深度不足 (${bid_liquidity:.2f} < ${trade_size_usd * self.min_liquidity_ratio:.2f})")
            
            if ask_liquidity < trade_size_usd * self.min_liquidity_ratio:
                issues.append(f"卖单深度不足 (${ask_liquidity:.2f} < ${trade_size_usd * self.min_liquidity_ratio:.2f})")
            
            if bid_slippage > self.max_slippage_pct:
                issues.append(f"买入滑点过高 ({bid_slippage:.4f}% > {self.max_slippage_pct:.4f}%)")
            
            if ask_slippage > self.max_slippage_pct:
                issues.append(f"卖出滑点过高 ({ask_slippage:.4f}% > {self.max_slippage_pct:.4f}%)")
            
            if bid_remaining > 0:
                issues.append(f"买单深度不足以完成交易 (剩余{bid_remaining:.6f}{coin})")
            
            if ask_remaining > 0:
                issues.append(f"卖单深度不足以完成交易 (剩余{ask_remaining:.6f}{coin})")
            
            error = "; ".join(issues) if issues else None
            
            return {
                "has_sufficient_liquidity": has_sufficient_liquidity,
                "current_price": current_price,
                "trade_size_usd": trade_size_usd,
                "trade_size_coin": trade_size_coin,
                "bid_liquidity_usd": bid_liquidity,
                "ask_liquidity_usd": ask_liquidity,
                "bid_executed_price": bid_executed_price,
                "ask_executed_price": ask_executed_price,
                "bid_slippage_pct": bid_slippage,
                "ask_slippage_pct": ask_slippage,
                "min_liquidity_ratio": self.min_liquidity_ratio,
                "required_liquidity_usd": trade_size_usd * self.min_liquidity_ratio,
                "error": error
            }
        except Exception as e:
            return {
                "has_sufficient_liquidity": False,
                "error": f"分析{exchange}的{coin}流动性时出错: {e}"
            }

    async def run(self):
        """
        运行套利策略
        """
        self.is_running = True
        self.logger.info("套利策略已启动")
        
        try:
            # 初始化市场数据字典
            self.market_data = {}
            
            while self.is_running:
                # 检查套利机会
                await self.check_for_opportunities()
                
                # 等待指定时间
                await asyncio.sleep(self.price_check_interval)
                
        except asyncio.CancelledError:
            self.logger.info("套利策略已取消")
            self.is_running = False
        except Exception as e:
            self.logger.error(f"套利策略运行出错: {e}")
            self.is_running = False
            raise
        
        self.logger.info("套利策略已停止")
        
    async def update_market_data(self):
        """
        更新市场数据字典，用于显示和记录
        """
        try:
            # 如果市场数据字典尚未初始化
            if not hasattr(self, "market_data"):
                self.market_data = {}
                
            # 更新每个币种的市场数据
            for coin in self.coins_to_monitor:
                if coin not in self.market_data:
                    self.market_data[coin] = {}
                    
                # 获取Hyperliquid数据
                hl_price = self.hyperliquid_api.prices.get(coin)
                hl_funding_rate = self.funding_rates.get(f"HL_{coin}")
                
                if hl_price:
                    if "hyperliquid" not in self.market_data[coin]:
                        self.market_data[coin]["hyperliquid"] = {}
                    
                    self.market_data[coin]["hyperliquid"]["price"] = hl_price
                    
                if hl_funding_rate is not None:
                    if "hyperliquid" not in self.market_data[coin]:
                        self.market_data[coin]["hyperliquid"] = {}
                        
                    self.market_data[coin]["hyperliquid"]["funding_rate"] = hl_funding_rate
                    # 调整为8小时资金费率，方便与BP比较
                    self.market_data[coin]["hyperliquid"]["adjusted_funding_rate"] = hl_funding_rate * 8
                    
                # 获取Backpack数据
                bp_price = await self.backpack_api.get_price(coin)
                bp_funding_rate = self.funding_rates.get(f"BP_{coin}")
                
                if bp_price:
                    if "backpack" not in self.market_data[coin]:
                        self.market_data[coin]["backpack"] = {}
                        
                    self.market_data[coin]["backpack"]["price"] = bp_price
                    
                if bp_funding_rate is not None:
                    if "backpack" not in self.market_data[coin]:
                        self.market_data[coin]["backpack"] = {}
                        
                    self.market_data[coin]["backpack"]["funding_rate"] = bp_funding_rate
            
            # 更新DisplayManager显示
            if self.display_manager:
                self.display_manager.update_market_data(self.market_data)
                    
            return self.market_data
            
        except Exception as e:
            self.logger.error(f"更新市场数据出错: {e}")
            return {}
    
    async def check_for_opportunities(self):
        """
        检查所有交易对的套利机会
        扩展版本: 检查资金费率、价格差异、流动性情况和滑点控制
        """
        current_time = time.time()
        
        # 避免频繁检查
        if current_time - self.last_check_time < self.arbitrage_check_interval:
            return
        
        self.last_check_time = current_time
        self.stats["checks"] += 1
        
        # 获取资金费率信息
        try:
            await self.update_funding_rates()
            
            # 更新市场数据
            await self.update_market_data()
            
            # 检查每个币种的套利机会
            for coin in self.coins_to_monitor:
                # 检查是否在冷却期
                if coin in self.last_trade_time:
                    time_since_last_trade = current_time - self.last_trade_time[coin]
                    if time_since_last_trade < self.trade_cooldown:
                        cooldown_left = self.trade_cooldown - time_since_last_trade
                        self.logger.debug(f"{coin}仍在交易冷却期 (剩余{cooldown_left:.1f}秒)")
                        continue
                
                # 获取资金费率
                hl_funding_rate = self.funding_rates.get(f"HL_{coin}", 0)
                bp_funding_rate = self.funding_rates.get(f"BP_{coin}", 0)
                
                if hl_funding_rate is None or bp_funding_rate is None:
                    self.logger.warning(f"无法获取{coin}的完整资金费率")
                    continue
                
                # 计算资金费率差异
                funding_diff = hl_funding_rate - bp_funding_rate
                abs_funding_diff = abs(funding_diff)
                
                # 获取两个交易所的价格
                hl_price = self.hyperliquid_api.prices.get(coin)
                bp_price = await self.backpack_api.get_price(coin)
                
                if not hl_price or not bp_price:
                    self.logger.warning(f"无法获取{coin}的完整价格信息")
                    continue
                
                # 计算价格差异
                price_diff_pct = abs(hl_price - bp_price) / min(hl_price, bp_price)
                
                # 分析两个交易所的流动性情况（提前获取滑点信息用于日志记录）
                liquidity_analysis = await self.analyze_liquidity(coin)
                combined_analysis = liquidity_analysis.get("combined", {})
                
                # 确定做多和做空的交易所
                long_exchange = "hyperliquid" if funding_diff < 0 else "backpack"
                short_exchange = "backpack" if funding_diff < 0 else "hyperliquid"
                
                # 获取相应的滑点信息
                long_analysis = liquidity_analysis.get(long_exchange, {})
                short_analysis = liquidity_analysis.get(short_exchange, {})
                
                long_slippage = long_analysis.get("bid_slippage_pct", 0)
                short_slippage = short_analysis.get("ask_slippage_pct", 0)
                total_slippage = long_slippage + short_slippage
                
                # 判断滑点是否在允许范围内
                slippage_ok = total_slippage <= self.max_slippage_pct * 2
                
                # 添加INFO级别的滑点信息日志，确保在INFO级别也可以看到
                self.logger.info(
                    f"{coin} - 滑点分析: 总滑点={total_slippage:.4f}%, "
                    f"做多交易所({long_exchange})滑点={long_slippage:.4f}%, "
                    f"做空交易所({short_exchange})滑点={short_slippage:.4f}%, "
                    f"是否符合条件: {'' if slippage_ok else '不'}符合"
                )
                
                # 将滑点信息添加到市场数据中，用于显示
                if coin in self.market_data:
                    self.market_data[coin]["total_slippage"] = total_slippage
                    self.market_data[coin]["long_slippage"] = long_slippage
                    self.market_data[coin]["short_slippage"] = short_slippage
                    self.market_data[coin]["liquidity_analysis"] = liquidity_analysis
                    self.logger.debug(f"已将{coin}的滑点信息添加到市场数据: total_slippage={total_slippage}")
                
                # 记录基本信息（添加滑点信息）
                self.logger.debug(
                    f"{coin} - 资金费率差: {funding_diff:.6f} "
                    f"(HL: {hl_funding_rate:.6f}, BP: {bp_funding_rate:.6f}), "
                    f"价格差: {price_diff_pct:.4%} "
                    f"(HL: {hl_price:.2f}, BP: {bp_price:.2f}), "
                    f"滑点: {long_exchange}买入{long_slippage:.4f}%, "
                    f"{short_exchange}卖出{short_slippage:.4f}%, "
                    f"总滑点: {total_slippage:.4f}% "
                    f"[{'' if slippage_ok else '不'}符合条件: {self.max_slippage_pct * 2:.4f}%]"
                )
                
                # 资金费率和价格差异条件检查
                funding_ok = abs_funding_diff >= self.min_funding_diff
                price_ok = price_diff_pct <= self.min_price_diff_pct
                
                self.logger.debug(
                    f"{coin} - 条件检查: "
                    f"资金费率差: {abs_funding_diff:.6f}% >= {self.min_funding_diff:.6f}% [{'' if funding_ok else '不'}符合], "
                    f"价格差: {price_diff_pct:.4%} <= {self.min_price_diff_pct:.4%} [{'' if price_ok else '不'}符合], "
                    f"滑点: {total_slippage:.4f}% <= {self.max_slippage_pct * 2:.4f}% [{'' if slippage_ok else '不'}符合]"
                )
                
                # 检查是否满足套利条件
                if funding_ok and price_ok:
                    # 检查流动性是否充足
                    if not combined_analysis.get("has_sufficient_liquidity", False):
                        issues = combined_analysis.get("issues", [])
                        issues_text = "; ".join(issues) if issues else "未知流动性问题"
                        self.logger.info(f"{coin}套利机会 - 但{issues_text}")
                        
                        # 记录详细流动性分析结果
                        if self.logger.level <= logging.DEBUG:
                            self.logger.debug(f"Hyperliquid流动性分析: {liquidity_analysis.get('hyperliquid', {})}")
                            self.logger.debug(f"Backpack流动性分析: {liquidity_analysis.get('backpack', {})}")
                        continue
                    
                    # 检查滑点是否在可接受范围内
                    if not slippage_ok:
                        self.logger.info(
                            f"{coin}套利机会 - 但总滑点过高: {total_slippage:.4f}% > {self.max_slippage_pct * 2:.4f}%, "
                            f"({long_exchange}买入{long_slippage:.4f}%, {short_exchange}卖出{short_slippage:.4f}%)"
                        )
                        continue
                    
                    # 检查预期收益是否能覆盖滑点成本
                    expected_daily_return = abs(funding_diff) * self.trade_size_usd
                    slippage_cost = (total_slippage / 100) * self.trade_size_usd
                    profit_cover_slippage = slippage_cost <= expected_daily_return * 0.5
                    
                    self.logger.debug(
                        f"{coin} - 收益分析: "
                        f"预期日收益: ${expected_daily_return:.2f}, "
                        f"滑点成本: ${slippage_cost:.2f}, "
                        f"成本占比: {slippage_cost/expected_daily_return*100:.2f}% <= 50% [{'' if profit_cover_slippage else '不'}符合]"
                    )
                    
                    if not profit_cover_slippage:
                        self.logger.info(
                            f"{coin}套利机会 - 但滑点成本过高: ${slippage_cost:.2f} > ${expected_daily_return * 0.5:.2f} "
                            f"(预期日收益的50%)"
                        )
                        continue
                    
                    self.stats["opportunities_found"] += 1
                    
                    # 记录套利机会详情
                    self.logger.info(
                        f"发现{coin}套利机会! 资金费率差: {funding_diff:.6f}, "
                        f"价格差: {price_diff_pct:.4%}, "
                        f"预计滑点: {long_exchange}买入{long_slippage:.4f}%, "
                        f"{short_exchange}卖出{short_slippage:.4f}%, "
                        f"总滑点: {total_slippage:.4f}% [符合条件: <={self.max_slippage_pct * 2:.4f}%], "
                        f"预期收益: ${expected_daily_return:.2f}/天, 滑点成本: ${slippage_cost:.2f} "
                        f"[符合条件: <={expected_daily_return * 0.5:.2f}]"
                    )
                    
                    # 执行套利
                    if self.execution_mode == "live":
                        await self.execute_arbitrage(coin, funding_diff, liquidity_analysis)
            
            # 在所有币种检查完成后，再次更新市场数据以确保滑点信息显示在终端表格中
            if self.display_manager:
                self.display_manager.update_market_data(self.market_data)
                
        except Exception as e:
            self.logger.error(f"检查套利机会时出错: {e}")
            # 即使出错也尝试更新显示
            if self.display_manager:
                try:
                    self.display_manager.update_market_data(self.market_data)
                except Exception as display_e:
                    self.logger.error(f"更新显示时出错: {display_e}")

    async def execute_arbitrage(self, coin: str, funding_diff: float, liquidity_analysis: Dict[str, Any]):
        """
        执行套利交易
        
        Args:
            coin: 币种
            funding_diff: 资金费率差异
            liquidity_analysis: 流动性分析结果
        """
        try:
            # 记录交易时间
            self.last_trade_time[coin] = time.time()
            
            # 确定交易方向
            long_exchange = "hyperliquid" if funding_diff < 0 else "backpack"
            short_exchange = "backpack" if funding_diff < 0 else "hyperliquid"
            
            # 获取相应交易所的分析结果
            long_analysis = liquidity_analysis.get(long_exchange, {})
            short_analysis = liquidity_analysis.get(short_exchange, {})
            
            trade_size_usd = self.trade_size_usd
            long_price = long_analysis.get("current_price", 0)
            short_price = short_analysis.get("current_price", 0)
            
            # 获取滑点信息
            long_slippage = long_analysis.get("bid_slippage_pct", 0)
            short_slippage = short_analysis.get("ask_slippage_pct", 0)
            
            self.logger.info(
                f"执行{coin}套利: 在{long_exchange}做多, 在{short_exchange}做空, "
                f"交易金额: ${trade_size_usd:.2f}, "
                f"价格: {long_exchange}${long_price:.2f}, {short_exchange}${short_price:.2f}"
            )
            
            # 计算预期收益
            expected_daily_return = abs(funding_diff) * trade_size_usd
            expected_return_per_hour = expected_daily_return / 24
            
            # 考虑滑点的净收益
            slippage_cost = (long_slippage + short_slippage) / 100 * trade_size_usd
            net_expected_daily_return = expected_daily_return - slippage_cost
            
            self.logger.info(
                f"预期收益: 每天${expected_daily_return:.2f}(扣除滑点后${net_expected_daily_return:.2f}), "
                f"每小时${expected_return_per_hour:.2f}"
            )
            
            # 在这里添加实际交易代码
            # ...
            
            # 记录交易
            trade_record = {
                "timestamp": time.time(),
                "coin": coin,
                "funding_diff": funding_diff,
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "trade_size_usd": trade_size_usd,
                "long_price": long_price,
                "short_price": short_price,
                "expected_daily_return": expected_daily_return,
                "slippage_cost": slippage_cost,
                "net_expected_daily_return": net_expected_daily_return,
                "slippage_long": long_slippage,
                "slippage_short": short_slippage
            }
            
            self.trade_history.append(trade_record)
            self.stats["trades_executed"] += 1
            self.stats["total_profit_usd"] += net_expected_daily_return
            
            self.logger.info(f"{coin}套利交易已执行")
            
        except Exception as e:
            self.logger.error(f"执行{coin}套利时出错: {e}") 

    async def update_funding_rates(self):
        """
        更新所有监控币种的资金费率
        """
        try:
            # 初始化资金费率字典
            if not hasattr(self, "funding_rates"):
                self.funding_rates = {}
            
            # 获取Hyperliquid资金费率
            hl_funding_rates = await self.hyperliquid_api.get_funding_rates()
            if hl_funding_rates:
                for coin, rate in hl_funding_rates.items():
                    self.funding_rates[f"HL_{coin}"] = rate
            
            # 获取Backpack资金费率
            bp_funding_rates = await self.backpack_api.get_funding_rates()
            if bp_funding_rates:
                for coin, rate in bp_funding_rates.items():
                    self.funding_rates[f"BP_{coin}"] = rate
            
            # 记录资金费率更新
            funding_info = []
            for coin in self.coins_to_monitor:
                hl_rate = self.funding_rates.get(f"HL_{coin}")
                bp_rate = self.funding_rates.get(f"BP_{coin}")
                
                if hl_rate is not None and bp_rate is not None:
                    diff = hl_rate - bp_rate
                    funding_info.append(f"{coin}: HL={hl_rate:.6f}, BP={bp_rate:.6f}, 差={diff:.6f}")
            
            if funding_info:
                self.logger.debug(f"资金费率更新: {'; '.join(funding_info)}")
                
        except Exception as e:
            self.logger.error(f"更新资金费率时出错: {e}")

    def stop(self):
        """停止策略运行"""
        self.is_running = False
        self.logger.info("正在停止套利策略...")
    
    async def get_statistics(self):
        """获取策略运行统计信息"""
        stats = self.stats.copy()
        
        # 计算运行时间
        stats["run_time"] = time.time() - stats["start_time"]
        
        # 格式化数字
        stats["run_time_formatted"] = f"{stats['run_time'] / 60:.1f}分钟"
        stats["profit_per_check"] = stats["total_profit_usd"] / max(stats["checks"], 1)
        
        # 计算机会转化率
        if stats["opportunities_found"] > 0:
            stats["conversion_rate"] = stats["trades_executed"] / stats["opportunities_found"]
        else:
            stats["conversion_rate"] = 0
            
        # 记录资金费率信息
        stats["funding_rates"] = {}
        for coin in self.coins_to_monitor:
            hl_rate = self.funding_rates.get(f"HL_{coin}")
            bp_rate = self.funding_rates.get(f"BP_{coin}")
            
            if hl_rate is not None and bp_rate is not None:
                stats["funding_rates"][coin] = {
                    "hyperliquid": hl_rate,
                    "backpack": bp_rate,
                    "diff": hl_rate - bp_rate
                }
        
        return stats 