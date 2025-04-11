#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
套利引擎模块

实现资金费率套利策略的核心逻辑
"""

import asyncio
import logging
import time
import os
import json
import sys
import math
from typing import Dict, List, Optional, Any
from datetime import datetime

# 尝试使用包内相对导入（当作为包导入时）
try:
    from funding_arbitrage_bot.exchanges.backpack_api import BackpackAPI
    from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
    from funding_arbitrage_bot.core.data_manager import DataManager
    from funding_arbitrage_bot.utils.display_manager import DisplayManager
    from funding_arbitrage_bot.utils.webhook_alerter import WebhookAlerter
    from funding_arbitrage_bot.utils.helpers import (
        calculate_funding_diff,
        get_backpack_symbol,
        get_hyperliquid_symbol
    )
# 当直接运行时尝试使用直接导入
except ImportError:
    try:
        from exchanges.backpack_api import BackpackAPI
        from exchanges.hyperliquid_api import HyperliquidAPI
        from core.data_manager import DataManager
        from utils.display_manager import DisplayManager
        from utils.webhook_alerter import WebhookAlerter
        from utils.helpers import (
            calculate_funding_diff,
            get_backpack_symbol,
            get_hyperliquid_symbol
        )
    # 如果以上都失败，尝试相对导入（当在包内运行时）
    except ImportError:
        from ..exchanges.backpack_api import BackpackAPI
        from ..exchanges.hyperliquid_api import HyperliquidAPI
        from ..core.data_manager import DataManager
        from ..utils.display_manager import DisplayManager
        from ..utils.webhook_alerter import WebhookAlerter
        from ..utils.helpers import (
            calculate_funding_diff,
            get_backpack_symbol,
            get_hyperliquid_symbol
        )


class ArbitrageEngine:
    """套利引擎类，负责执行套利策略"""

    def __init__(
        self,
        config: Dict[str, Any],
        backpack_api: BackpackAPI,
        hyperliquid_api: HyperliquidAPI,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化套利引擎

        Args:
            config: 配置字典
            backpack_api: Backpack API实例
            hyperliquid_api: Hyperliquid API实例
            logger: 日志记录器
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

        # 初始化API实例
        self.backpack_api = backpack_api
        self.hyperliquid_api = hyperliquid_api

        # 初始化数据管理器
        self.data_manager = DataManager(
            backpack_api=backpack_api,
            hyperliquid_api=hyperliquid_api,
            symbols=config["strategy"]["symbols"],
            funding_update_interval=config["strategy"]["funding_update_interval"],
            logger=self.logger)

        # 先不初始化显示管理器，等start方法中再初始化
        self.display_manager = None

        # 设置策略参数
        strategy_config = config["strategy"]
        open_conditions = strategy_config.get("open_conditions", {})
        close_conditions = strategy_config.get("close_conditions", {})

        # 读取最大持仓数量限制并记录
        self.max_positions_count = strategy_config.get(
            "max_positions_count", 5)
        self.logger.info(f"最大持仓数量限制: {self.max_positions_count}个不同币种")

        # 从open_conditions中获取min_funding_diff（新配置结构）
        self.arb_threshold = open_conditions.get("min_funding_diff", 0.00001)

        # 如果open_conditions不存在或min_funding_diff不在其中，尝试从旧的配置结构获取
        if self.arb_threshold == 0.00001 and "min_funding_diff" in strategy_config:
            self.arb_threshold = strategy_config["min_funding_diff"]
            self.logger.warning("使用旧配置结构中的min_funding_diff参数")

        self.position_sizes = strategy_config.get("position_sizes", {})
        self.max_position_time = close_conditions.get(
            "max_position_time", 28800)  # 默认8小时
        self.trading_pairs = strategy_config.get("trading_pairs", [])

        # 价差参数 - 从新配置结构获取
        self.min_price_diff_percent = open_conditions.get(
            "min_price_diff_percent", 0.2)
        self.max_price_diff_percent = open_conditions.get(
            "max_price_diff_percent", 1.0)

        # 获取开仓和平仓条件类型
        self.open_condition_type = open_conditions.get(
            "condition_type", "funding_only")
        self.close_condition_type = close_conditions.get(
            "condition_type", "any")

        # 平仓条件参数
        self.funding_diff_sign_change = close_conditions.get(
            "funding_diff_sign_change", True)
        self.min_profit_percent = close_conditions.get(
            "min_profit_percent", 0.1)
        self.max_loss_percent = close_conditions.get("max_loss_percent", 0.3)
        self.close_min_funding_diff = close_conditions.get(
            "min_funding_diff", self.arb_threshold / 2)

        # 初始化价格和资金费率数据
        self.prices = {}
        self.funding_rates = {}
        self.positions = {}
        self.positions_lock = asyncio.Lock()

        # 初始化交易对映射
        self.symbol_mapping = {}

        # 资金费率符号记录文件路径
        self.funding_signs_file = os.path.join(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(__file__)))),
            'data',
            'funding_diff_signs.json'
        )

        # 确保data目录存在
        os.makedirs(os.path.dirname(self.funding_signs_file), exist_ok=True)

        # 添加开仓时的资金费率符号记录 - 从文件加载
        self.funding_diff_signs = self._load_funding_diff_signs()
        self.logger.info(f"从文件加载资金费率符号记录: {self.funding_diff_signs}")

        # 新增：开仓时的资金费率和价格记录
        self.entry_funding_rates = {}
        self.entry_prices = {}

        # 新增：开仓时间记录
        self.position_open_times = {}

        # 新增：交易快照文件路径
        self.snapshots_dir = os.path.join(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(__file__)))),
            'data',
            'positions'
        )
        os.makedirs(self.snapshots_dir, exist_ok=True)

        # 新增：持仓记录文件和资金费率记录文件
        self.positions_file = os.path.join(
            os.path.dirname(self.funding_signs_file),
            'positions_record.json'
        )
        self.funding_rates_file = os.path.join(
            os.path.dirname(self.funding_signs_file),
            'funding_rates_record.json'
        )

        # 初始化事件循环和任务列表
        self.loop = asyncio.get_event_loop()
        self.tasks = []

        # 获取更新间隔
        update_intervals = strategy_config.get("update_intervals", {})
        self.price_update_interval = update_intervals.get("price", 1)
        self.funding_update_interval = update_intervals.get("funding", 60)
        self.position_update_interval = update_intervals.get("position", 10)
        self.check_interval = update_intervals.get("check", 5)

        # 初始化统计数据
        self.stats = {
            "total_trades": 0,
            "successful_trades": 0,
            "failed_trades": 0,
            "total_profit": 0,
            "start_time": None,
            "last_trade_time": None
        }

        # 初始化停止事件
        self.stop_event = asyncio.Event()

        # 打印配置摘要
        self.logger.info(f"套利引擎初始化完成，套利阈值: {self.arb_threshold}")
        self.logger.info(f"交易对: {self.trading_pairs}")
        self.logger.info(
            f"价差参数 - 最小: {self.min_price_diff_percent}%, 最大: {self.max_price_diff_percent}%")
        self.logger.info(
            f"开仓条件类型: {self.open_condition_type}, 平仓条件类型: {self.close_condition_type}")

        # 持仓同步参数
        self.position_sync_interval = config.get(
            "strategy", {}).get(
            "position_sync_interval", 300)  # 默认5分钟同步一次
        self.last_sync_time = time.time()  # 初始化为当前时间，避免启动后立即同步

        # 运行标志
        self.is_running = False

        # 初始化报警管理器
        order_hook_url = config.get(
            "notification", {}).get("order_webhook_url")
        if order_hook_url:
            self.alerter = WebhookAlerter(order_hook_url)
            self.logger.info(f"已配置订单通知Webhook: {order_hook_url}")
        else:
            self.alerter = None
            self.logger.info("未配置订单通知")

    def _load_funding_diff_signs(self) -> Dict[str, int]:
        """
        从文件加载资金费率符号记录

        Returns:
            Dict[str, int]: 资金费率符号记录字典
        """
        try:
            if os.path.exists(self.funding_signs_file):
                with open(self.funding_signs_file, 'r') as f:
                    # 从文件中读取的是字符串形式的字典，需要将键的字符串形式转换为整数
                    signs_data = json.load(f)
                    # 确保符号值是整数类型
                    return {symbol: int(sign)
                            for symbol, sign in signs_data.items()}
            return {}
        except Exception as e:
            self.logger.error(f"加载资金费率符号记录文件失败: {e}")
            return {}

    def _save_funding_diff_signs(self) -> None:
        """
        将资金费率符号记录保存到文件
        """
        try:
            with open(self.funding_signs_file, 'w') as f:
                json.dump(self.funding_diff_signs, f)
            self.logger.debug(f"资金费率符号记录已保存到文件: {self.funding_signs_file}")
        except Exception as e:
            self.logger.error(f"保存资金费率符号记录到文件失败: {e}")

    def _load_positions_and_records(self) -> None:
        """
        从文件加载开仓时间和资金费率记录
        在引擎启动时调用，确保能够恢复之前的状态
        注意：不再加载持仓方向记录，而是每次从API获取最新数据
        """
        try:
            # 加载持仓时间记录
            if os.path.exists(self.positions_file):
                with open(self.positions_file, 'r') as f:
                    positions_data = json.load(f)

                    # 不再恢复持仓方向记录
                    if 'position_directions' in positions_data:
                        self.logger.info("不再从本地文件加载持仓方向记录，将使用API获取的实时数据")

                    # 恢复开仓时间记录
                    if 'position_open_times' in positions_data:
                        # 时间记录需要转换为浮点数
                        self.position_open_times = {symbol: float(time_str)
                                                    for symbol, time_str in positions_data['position_open_times'].items()}
                        self.logger.info(
                            f"已加载开仓时间记录: {len(self.position_open_times)}个币种")

            # 加载资金费率和开仓价格记录
            if os.path.exists(self.funding_rates_file):
                with open(self.funding_rates_file, 'r') as f:
                    rates_data = json.load(f)

                    # 恢复开仓资金费率记录
                    if 'entry_funding_rates' in rates_data:
                        self.entry_funding_rates = rates_data['entry_funding_rates']
                        self.logger.info(
                            f"已加载开仓资金费率记录: {len(self.entry_funding_rates)}个币种")

                    # 恢复开仓价格记录
                    if 'entry_prices' in rates_data:
                        self.entry_prices = rates_data['entry_prices']
                        self.logger.info(
                            f"已加载开仓价格记录: {len(self.entry_prices)}个币种")

            self.logger.info("资金费率记录和开仓时间记录加载完成")
        except Exception as e:
            self.logger.error(f"加载持仓时间和资金费率记录失败: {e}")

    def _save_positions_and_records(self) -> None:
        """
        将开仓时间和资金费率记录保存到文件
        注意：不再保存持仓方向记录，而是每次从API获取最新数据
        """
        try:
            # 保存开仓时间记录（不保存持仓方向记录）
            positions_data = {
                'position_open_times': self.position_open_times
            }

            with open(self.positions_file, 'w') as f:
                json.dump(positions_data, f, indent=2)

            self.logger.debug(f"开仓时间记录已保存到文件: {self.positions_file}")

            # 保存资金费率和开仓价格记录
            rates_data = {
                'entry_funding_rates': self.entry_funding_rates,
                'entry_prices': self.entry_prices
            }

            with open(self.funding_rates_file, 'w') as f:
                json.dump(rates_data, f, indent=2)

            self.logger.debug(f"资金费率和价格记录已保存到文件: {self.funding_rates_file}")
        except Exception as e:
            self.logger.error(f"保存开仓时间和资金费率记录失败: {e}")

    def _save_position_snapshot(self, symbol, action, bp_position, hl_position, bp_price, hl_price, bp_funding, hl_funding):
        """
        保存开仓或平仓快照到文件

        Args:
            symbol: 币种
            action: 操作类型，"open"或"close"
            bp_position: BP持仓信息
            hl_position: HL持仓信息
            bp_price: BP价格
            hl_price: HL价格
            bp_funding: BP资金费率
            hl_funding: HL资金费率
        """
        try:
            # 准备快照数据
            timestamp = time.time()
            formatted_time = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            price_diff = bp_price - hl_price
            price_diff_percent = (price_diff / hl_price) * \
                100 if hl_price != 0 else 0
            funding_diff = bp_funding - hl_funding

            # 获取持仓方向
            bp_side = bp_position.get(
                "side", "UNKNOWN") if bp_position else "UNKNOWN"
            hl_side = hl_position.get(
                "side", "UNKNOWN") if hl_position else "UNKNOWN"

            # 获取持仓数量
            bp_size = bp_position.get("size", 0) if bp_position else 0
            if "quantity" in bp_position and not bp_size:
                bp_size = bp_position.get("quantity", 0)
            hl_size = hl_position.get("size", 0) if hl_position else 0

            snapshot = {
                "symbol": symbol,
                "action": action,
                "timestamp": timestamp,
                "formatted_time": formatted_time,
                "bp_side": bp_side,
                "hl_side": hl_side,
                "bp_size": bp_size,
                "hl_size": hl_size,
                "bp_price": bp_price,
                "hl_price": hl_price,
                "price_diff": price_diff,
                "price_diff_percent": price_diff_percent,
                "bp_funding": bp_funding,
                "hl_funding": hl_funding,
                "funding_diff": funding_diff
            }

            # 保存到文件
            filename = f"{self.snapshots_dir}/{symbol}_{action}_{int(timestamp)}.json"
            with open(filename, "w") as f:
                json.dump(snapshot, f, indent=2)

            self.logger.info(f"{symbol} - 已保存{action}快照到文件: {filename}")

            # 交易记录保存完成后，同时更新持仓和资金费率记录文件
            if action == "open":
                # 开仓后保存记录
                self._save_positions_and_records()
            elif action == "close":
                # 平仓后保存记录（主要是为了确保删除的记录已持久化）
                self._save_positions_and_records()

        except Exception as e:
            self.logger.error(f"{symbol} - 保存{action}快照失败: {str(e)}")

    def _analyze_orderbook(self, orderbook, side, amount_usd, price):
        """
        分析订单簿计算滑点

        Args:
            orderbook: 订单簿数据
            side: 'bids'表示买单(做多)，'asks'表示卖单(做空)
            amount_usd: 欲交易的美元金额
            price: 当前市场价格

        Returns:
            float: 百分比形式的滑点
        """
        try:
            # 添加调试日志
            self.logger.debug(
                f"分析订单簿: side={side}, amount_usd={amount_usd}, price={price}")

            # 检查订单簿数据有效性
            if not orderbook:
                self.logger.warning("订单簿数据为空")
                return 0.1  # 默认较高滑点

            if side not in orderbook:
                self.logger.warning(f"订单簿中不存在{side}列表")
                return 0.1  # 默认较高滑点

            if not orderbook[side]:
                self.logger.warning(f"订单簿{side}列表为空")
                return 0.1  # 默认较高滑点

            # 查看数据结构
            sample_item = orderbook[side][0] if orderbook[side] else None
            self.logger.debug(f"订单簿数据样例: {sample_item}")

            # 处理不同交易所可能的数据格式差异
            book_side = []
            for level in orderbook[side]:
                # 对Hyperliquid格式 [{"px": price, "sz": size}, ...] 的处理
                if isinstance(level, dict) and "px" in level and "sz" in level:
                    book_side.append([float(level["px"]), float(level["sz"])])
                # 对Backpack格式 [{"px": price, "sz": size}, ...] 的处理 (已在API中统一)
                elif isinstance(level, dict) and "price" in level and "size" in level:
                    book_side.append(
                        [float(level["price"]), float(level["size"])])
                # 对价格和数量已经是列表 [price, size] 的处理
                elif isinstance(level, list) and len(level) >= 2:
                    book_side.append([float(level[0]), float(level[1])])
                else:  # Fix indentation for the 'else' block
                    self.logger.warning(f"无法识别的订单簿数据格式: {level}")

            # 如果数据转换后为空，返回默认值
            if not book_side:
                self.logger.warning("数据格式转换后订单簿为空")
                return 0.1  # 默认较高滑点

            # 确保订单按价格排序
            if side == 'bids':
                # 买单从高到低
                book_side = sorted(
                    book_side, key=lambda x: float(
                        x[0]), reverse=True)
            else:
                # 卖单从低到高
                book_side = sorted(book_side, key=lambda x: float(x[0]))

            # 记录排序后的前几个价格
            self.logger.debug(
                f"排序后的{side}前5个价格: {[item[0] for item in book_side[:5]]}")

            # 计算滑点
            amount_filled = 0.0
            weighted_price = 0.0

            levels_checked = 0
            for level in book_side:
                level_price = float(level[0])
                level_size = float(level[1])

                # 计算此价格级别的美元价值
                level_value = level_price * level_size
                level_contribution = min(
                    level_value, amount_usd - amount_filled)

                self.logger.debug(
                    f"  深度{levels_checked}: 价格={level_price}, 数量={level_size}, 美元价值={level_value}, 贡献={level_contribution}")

                if amount_filled + level_value >= amount_usd:
                    # 计算需要从此级别填充的部分
                    remaining = amount_usd - amount_filled
                    size_needed = remaining / level_price

                    # 添加到加权平均价格计算
                    weighted_price += level_price * size_needed
                    amount_filled = amount_usd  # 已完全填充

                    self.logger.debug(
                        f"  已填满订单: 剩余={remaining}, 所需数量={size_needed}, 总填充量={amount_filled}")
                    break
                else:
                    # 添加整个级别到加权平均价格
                    weighted_price += level_price * level_size
                    amount_filled += level_value

                    self.logger.debug(f"  部分填充: 累计填充量={amount_filled}")

                levels_checked += 1
                # 只检查前10个深度级别
                if levels_checked >= 10:
                    self.logger.debug("已检查10个深度级别，中断检查")
                    break

            # 如果未能完全填充订单，但已填充超过80%，使用已填充部分计算
            if amount_filled < amount_usd:
                fill_percentage = (amount_filled / amount_usd) * 100
                self.logger.warning(
                    f"未能完全填充订单: 已填充{fill_percentage:.2f}% (${amount_filled:.2f}/${amount_usd:.2f})")

                if fill_percentage >= 80:
                    self.logger.info(
                        f"已填充{fill_percentage:.2f}%，继续使用已填充部分计算滑点")
                else:
                    # 流动性不足，但不要直接返回固定值，而是基于填充比例计算滑点
                    slippage = (100 - fill_percentage) / 100
                    # 从配置中获取最大滑点，默认为0.3%
                    max_slippage = self.config.get("strategy", {}).get(
                        "open_conditions", {}).get("max_slippage_percent", 0.3)
                    # 限制最大滑点
                    slippage = min(max_slippage, slippage)
                    self.logger.info(f"流动性不足，基于填充比例计算滑点: {slippage:.4f}")
                    return slippage

            # 使用实际填充的金额计算平均价格
            if amount_filled > 0:
                # 原始计算公式有问题，导致加权平均价几乎总是等于市场价
                # average_price = weighted_price / (amount_filled / price)

                # 修正后的计算公式：使用最后处理的level_price作为基准
                average_price = weighted_price / (amount_filled / level_price)
            else:
                self.logger.warning("填充金额为0，无法计算滑点")
                return 0.1  # 默认较高滑点

            # 计算滑点百分比
            if side == 'bids':
                # 买单，滑点 = (市场价 - 加权平均价) / 市场价
                slippage = (price - average_price) / price * 100
            else:
                # 卖单，滑点 = (加权平均价 - 市场价) / 市场价
                slippage = (average_price - price) / price * 100

            # 确保滑点为正值
            slippage = abs(slippage)

            self.logger.info(
                f"计算得到的滑点: {slippage:.4f}%, 市场价: {price}, 加权平均价: {average_price}")

            # 限制最小滑点为0.01%，最大滑点为0.5%
            slippage = max(0.01, min(0.5, slippage))

            return slippage

        except Exception as e:
            import traceback
            self.logger.error(f"分析订单簿计算滑点时出错: {e}")
            self.logger.error(traceback.format_exc())
            return 0.1  # 出错时返回默认滑点

    async def _collect_arbitrage_opportunity(
        self,
        symbol: str,
        open_candidates: list,
        close_candidates: list,
        bp_positions: dict,
        hl_positions: dict
    ):
        """
        收集套利机会，但不立即执行，而是将满足条件的币种添加到候选列表中

        Args:
            symbol: 基础币种，如 "BTC"
            open_candidates: 存储满足开仓条件的币种信息的列表
            close_candidates: 存储满足平仓条件的币种信息的列表
            bp_positions: Backpack持仓信息
            hl_positions: Hyperliquid持仓信息
        """
        try:
            # 获取最新数据
            data = await self.data_manager.get_data(symbol)

            # 检查数据有效性
            if not self.data_manager.is_data_valid(symbol):
                self.logger.warning(f"{symbol}数据无效，跳过检查")
                return

            # 提取价格和资金费率
            bp_data = data["backpack"]
            hl_data = data["hyperliquid"]

            bp_price = bp_data["price"]
            bp_funding = bp_data["funding_rate"]
            hl_price = hl_data["price"]
            hl_funding = hl_data["funding_rate"]

            # 调整Hyperliquid资金费率以匹配Backpack的8小时周期
            adjusted_hl_funding = hl_funding * 8

            # 计算价格差异（百分比）
            price_diff_percent = (bp_price - hl_price) / hl_price * 100

            # 计算资金费率差异
            funding_diff, funding_diff_sign = calculate_funding_diff(
                bp_funding, hl_funding)
            funding_diff_percent = funding_diff * 100  # 转为百分比

            bp_symbol = get_backpack_symbol(symbol)
            has_position = (
                bp_symbol in bp_positions) or (
                symbol in hl_positions)

            # 计算滑点信息
            # 确定做多和做空的交易所
            long_exchange = "hyperliquid" if funding_diff_sign < 0 else "backpack"
            short_exchange = "backpack" if funding_diff_sign < 0 else "hyperliquid"

            # 分析订单深度获取滑点信息
            try:
                # 获取交易金额
                trade_size_usd = self.config["strategy"].get(
                    "trade_size_usd", {}).get(symbol, 100)

                # 获取Hyperliquid订单深度数据
                hl_orderbook = await self.hyperliquid_api.get_orderbook(symbol)
                # 获取Backpack订单深度数据
                bp_orderbook = await self.backpack_api.get_orderbook(symbol)

                # 分析订单簿计算精确滑点
                long_slippage = 0.05  # 默认值
                short_slippage = 0.05  # 默认值

                # 根据做多/做空交易所计算实际滑点
                if long_exchange == "hyperliquid":
                    long_slippage = self._analyze_orderbook(
                        hl_orderbook, "bids", trade_size_usd, hl_price)
                else:  # long_exchange == "backpack"
                    long_slippage = self._analyze_orderbook(
                        bp_orderbook, "bids", trade_size_usd, bp_price)

                if short_exchange == "hyperliquid":
                    short_slippage = self._analyze_orderbook(
                        hl_orderbook, "asks", trade_size_usd, hl_price)
                else:  # short_exchange == "backpack"
                    short_slippage = self._analyze_orderbook(
                        bp_orderbook, "asks", trade_size_usd, bp_price)

                # 计算总滑点
                total_slippage = long_slippage + short_slippage

                # 将滑点信息添加到市场数据中
                market_data = self.data_manager.get_all_data()
                if symbol in market_data:
                    market_data[symbol]["total_slippage"] = total_slippage
                    market_data[symbol]["long_slippage"] = long_slippage
                    market_data[symbol]["short_slippage"] = short_slippage
                    market_data[symbol]["long_exchange"] = long_exchange
                    market_data[symbol]["short_exchange"] = short_exchange

                    # 获取是否检查方向一致性的配置
                    check_direction_consistency = self.config.get("strategy", {}).get(
                        "open_conditions", {}).get("check_direction_consistency", False)

                    # 检查方向一致性并添加到市场数据中
                    if check_direction_consistency:
                        is_consistent, bp_suggested_side, hl_suggested_side = self.check_direction_consistency(
                            symbol, bp_price, hl_price, bp_funding, adjusted_hl_funding)

                        # 添加建议方向到市场数据
                        market_data[symbol]["bp_suggested_side"] = bp_suggested_side
                        market_data[symbol]["hl_suggested_side"] = hl_suggested_side

                        # 根据是否有持仓，添加不同的方向一致性信息
                        if has_position:
                            # 已有持仓，需要计算平仓方向一致性
                            bp_position_side = market_data[symbol].get(
                                "bp_position_side")
                            hl_position_side = market_data[symbol].get(
                                "hl_position_side")

                            # 检查方向是否反转（现在建议的方向与开仓方向相反）
                            bp_direction_reversed = False
                            hl_direction_reversed = False

                            if bp_position_side:
                                bp_direction_reversed = (bp_position_side == "BUY" and bp_suggested_side == "SELL") or \
                                    (bp_position_side ==
                                     "SELL" and bp_suggested_side == "BUY")

                            if hl_position_side:
                                hl_direction_reversed = (hl_position_side == "BUY" and hl_suggested_side == "SELL") or \
                                    (hl_position_side == "SELL" and hl_suggested_side == "BUY") or \
                                    (hl_position_side == "LONG" and hl_suggested_side == "SELL") or \
                                    (hl_position_side ==
                                     "SHORT" and hl_suggested_side == "BUY")

                            # 方向反转条件：双向反转即可，不需要方向一致
                            direction_reversed = bp_direction_reversed and hl_direction_reversed

                            # 更新市场数据中的方向一致性标记
                            market_data[symbol]["direction_consistent"] = "是" if direction_reversed else "-"
                            if direction_reversed:
                                self.logger.info(
                                    f"{symbol}的平仓方向一致: 持仓(BP={bp_position_side}, HL={hl_position_side})，建议(BP={bp_suggested_side}, HL={hl_suggested_side})")

                            self.logger.info(
                                f"{symbol} - 方向反转检查: 持仓方向(BP={bp_position_side}, HL={hl_position_side})，"
                                f"当前建议方向(BP={bp_suggested_side}, HL={hl_suggested_side})，"
                                f"方向反转={direction_reversed}，方向一致={is_consistent}"
                            )
                        else:
                            # 无持仓，使用开仓方向一致性
                            market_data[symbol]["direction_consistent"] = "是" if is_consistent else "-"
                            if is_consistent:
                                self.logger.info(
                                    f"{symbol}的开仓方向一致: BP={bp_suggested_side}, HL={hl_suggested_side}")
                            else:
                                self.logger.info(
                                    f"{symbol}的开仓方向不一致: BP={bp_suggested_side}, HL={hl_suggested_side}")
                    else:
                        # 不检查方向一致性，默认设置为一致
                        market_data[symbol]["direction_consistent"] = "是"

                        # 仍然需要计算建议方向以供显示，但不进行一致性检查
                        bp_suggested_side = "BUY" if funding_diff < 0 else "SELL"
                        hl_suggested_side = "SELL" if funding_diff < 0 else "BUY"
                        market_data[symbol]["bp_suggested_side"] = bp_suggested_side
                        market_data[symbol]["hl_suggested_side"] = hl_suggested_side

                    # 调试输出滑点信息
                    self.logger.debug(
                        f"{symbol}滑点分析: 总滑点={total_slippage:.4f}%, 做多({long_exchange})={long_slippage:.4f}%, 做空({short_exchange})={short_slippage:.4f}%")

                    # 更新显示（仅在计算滑点后）
                    if self.display_manager:
                        self.display_manager.update_market_data(market_data)
            except Exception as e:
                self.logger.error(f"计算{symbol}滑点信息时出错: {e}")

            # 记录当前状态和调整后的资金费率
            self.logger.info(
                f"{symbol} - 价格差: {price_diff_percent:.4f}%, "
                f"资金费率差: {funding_diff_percent:.6f}%, "
                f"BP: {bp_funding:.6f}(8h), HL原始: {hl_funding:.6f}(1h), HL调整后: {adjusted_hl_funding:.6f}(8h), "
                f"持仓: {'是' if has_position else '否'}")

            if not has_position:
                # 没有仓位，检查是否满足开仓条件
                should_open = await self._check_open_conditions_without_execution(
                    symbol,
                    funding_diff,
                    bp_funding,
                    hl_funding,
                    adjusted_hl_funding,  # 添加调整后的8小时资金费率参数
                    self.config.get("strategy", {}).get(
                        "trade_size_usd", {}).get(symbol, 100),  # 默认交易大小
                    bp_positions,
                    hl_positions
                )

                if should_open:
                    self.logger.info(f"{symbol} - 满足开仓条件")
                    # 将满足条件的币种添加到开仓候选列表
                    open_candidates.append({
                        "symbol": symbol,
                        "funding_diff": funding_diff,
                        "bp_funding": bp_funding,
                        "hl_funding": hl_funding,
                        "available_size": self.config.get("strategy", {}).get("trade_size_usd", {}).get(symbol, 100),
                        "reason": "符合资金费率套利条件"
                    })
            else:
                # 有仓位，检查是否满足平仓条件
                bp_position = None
                hl_position = None

                # 获取持仓详情
                if bp_symbol in bp_positions:
                    bp_position = bp_positions[bp_symbol]
                if symbol in hl_positions:
                    hl_position = hl_positions[symbol]

                if bp_position and hl_position:
                    should_close, reason, position = await self._check_close_conditions_without_execution(
                        symbol,
                        bp_position,
                        hl_position,
                        bp_price,
                        hl_price,
                        bp_funding,
                        adjusted_hl_funding,
                        price_diff_percent,
                        funding_diff,
                        funding_diff_sign
                    )

                    if should_close:
                        self.logger.info(f"{symbol} - 满足平仓条件: {reason}")
                        # 记录详细的持仓信息
                        self.logger.debug(
                            f"{symbol} - 平仓持仓详情: BP={bp_position}, HL={hl_position}")
                        self.logger.debug(
                            f"{symbol} - 平仓position对象: {json.dumps(position, default=str)}")

                        # 将满足条件的币种添加到平仓候选列表
                        close_candidates.append({
                            "symbol": symbol,
                            "position": position,
                            "reason": reason
                        })
                else:
                    # 记录详细的持仓信息缺失情况
                    if not bp_position:
                        self.logger.warning(
                            f"{symbol} - Backpack持仓信息缺失，无法进行平仓条件检查")
                    if not hl_position:
                        self.logger.warning(
                            f"{symbol} - Hyperliquid持仓信息缺失，无法进行平仓条件检查")

        except Exception as e:
            self.logger.error(f"收集{symbol}套利机会时出错: {e}")

            # 检查是否是协程未等待的问题
            import inspect
            if inspect.iscoroutine(e.__cause__):
                self.logger.error(f"检测到未等待的协程: {e.__cause__}")
                try:
                    # 尝试等待协程
                    await e.__cause__
                except Exception as ce:
                    self.logger.error(f"等待协程时发生错误: {ce}")

            # 输出更详细的堆栈信息
            import traceback
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")

    async def start(self):
        """启动套利引擎"""
        try:
            self.logger.info("正在启动套利引擎...")

            # 添加调试输出
            print("==== 套利引擎启动 ====", file=sys.__stdout__)
            print(
                f"策略配置: {self.config.get('strategy', {})}",
                file=sys.__stdout__)
            print(
                f"交易对: {self.config.get('strategy', {}).get('symbols', [])}",
                file=sys.__stdout__)

            # 加载持仓记录和资金费率记录
            self.logger.info("加载持仓记录和资金费率记录...")
            self._load_positions_and_records()

            # 初始检查当前持仓状态
            self.logger.info("启动时检查当前持仓状态...")
            bp_positions = await self.backpack_api.get_positions()
            hl_positions = await self.hyperliquid_api.get_positions()

            # 计算当前持仓币种数量
            current_position_symbols = set()

            # 统计Backpack持仓币种
            for pos in bp_positions.values():
                if float(pos.get("quantity", 0)) != 0:
                    pos_symbol = pos.get("symbol", "").split("-")[0]
                    current_position_symbols.add(pos_symbol)

            # 统计Hyperliquid持仓币种
            for coin, pos in hl_positions.items():
                if pos.get("size", 0) != 0:
                    current_position_symbols.add(coin)

            # 获取max_positions_count配置
            max_positions_count = 5  # 默认值
            if "strategy" in self.config and isinstance(self.config["strategy"], dict):
                max_positions_count = self.config["strategy"].get(
                    "max_positions_count", max_positions_count)
            elif "max_positions_count" in self.config:
                max_positions_count = self.config.get(
                    "max_positions_count", max_positions_count)

            current_count = len(current_position_symbols)
            self.logger.info(
                f"当前已持有{current_count}个不同币种，配置限制为{max_positions_count}个")

            if current_count > 0:
                self.logger.info(
                    f"当前持仓币种: {', '.join(sorted(current_position_symbols))}")

            # 如果当前持仓数量接近或超过限制，发出警告
            if current_count >= max_positions_count:
                warning_msg = f"警告: 当前持仓数量({current_count})已达到或超过最大限制({max_positions_count})，无法开新仓位"
                self.logger.warning(warning_msg)
                print(warning_msg, file=sys.__stdout__)
            elif current_count >= max_positions_count - 1:
                warning_msg = f"注意: 当前持仓数量({current_count})接近最大限制({max_positions_count})，只能再开{max_positions_count-current_count}个新仓位"
                self.logger.warning(warning_msg)
                print(warning_msg, file=sys.__stdout__)

            # 初始化显示管理器
            print("正在初始化显示管理器...", file=sys.__stdout__)

            # 确保导入了DisplayManager
            try:
                from funding_arbitrage_bot.utils.display_manager import DisplayManager
            except ImportError:
                try:
                    from utils.display_manager import DisplayManager
                except ImportError:
                    print("无法导入DisplayManager", file=sys.__stdout__)
                    raise

            # 创建并启动显示管理器
            self.display_manager = DisplayManager(logger=self.logger)
            print("正在启动显示...", file=sys.__stdout__)
            self.display_manager.start()
            print("显示已启动", file=sys.__stdout__)

            # 如果当前持仓数量接近或超过限制，添加到显示管理器
            if current_count >= max_positions_count and self.display_manager:
                self.display_manager.add_order_message(
                    f"警告: 当前持仓数量({current_count})已达到或超过最大限制({max_positions_count})，无法开新仓位")
            elif current_count >= max_positions_count - 1 and self.display_manager:
                self.display_manager.add_order_message(
                    f"注意: 当前持仓数量({current_count})接近最大限制({max_positions_count})，只能再开{max_positions_count-current_count}个新仓位")

            # 启动数据流
            await self.data_manager.start_price_feeds()

            # 设置运行标志
            self.is_running = True

            # 开始主循环
            while self.is_running:
                try:
                    # 更新市场数据显示
                    market_data = self.data_manager.get_all_data()

                    # 获取当前持仓信息
                    bp_positions = await self.backpack_api.get_positions()
                    hl_positions = await self.hyperliquid_api.get_positions()

                    # 添加持仓信息到市场数据中，以便在表格中显示
                    for symbol in market_data:
                        bp_symbol = get_backpack_symbol(symbol)
                        has_position = (
                            bp_symbol in bp_positions) or (
                            symbol in hl_positions)
                        market_data[symbol]["position"] = has_position

                    # 更新持仓方向信息
                    market_data = self._update_position_direction_info(
                        market_data, bp_positions, hl_positions)

                    # 定期保存持仓和资金费率记录
                    current_time = time.time()
                    if current_time - self.last_sync_time >= self.position_sync_interval:
                        self.logger.info("定期保存持仓和资金费率记录...")
                        self._save_positions_and_records()
                        self.last_sync_time = current_time

                    # 使用直接的系统输出检查数据
                    print(f"更新市场数据: {len(market_data)}项", file=sys.__stdout__)

                    # 更新显示
                    if self.display_manager:
                        self.display_manager.update_market_data(market_data)
                    else:
                        print("显示管理器未初始化", file=sys.__stdout__)

                    # ===== 批量处理模式 =====
                    # 收集需要开仓和平仓的币种
                    open_candidates = []   # 存储满足开仓条件的币种信息
                    close_candidates = []  # 存储满足平仓条件的币种信息

                    # 检查每个交易对的套利机会，但不立即执行开仓/平仓
                    for symbol in self.config["strategy"]["symbols"]:
                        await self._collect_arbitrage_opportunity(
                            symbol,
                            open_candidates,
                            close_candidates,
                            bp_positions,
                            hl_positions
                        )

                    # 修改后的开仓逻辑: 每次只开一个资金费率差最大的仓位
                    if open_candidates:
                        self.logger.info(
                            f"收集到{len(open_candidates)}个符合开仓条件的币种")

                        # 按资金费率差的绝对值排序，取资金费率差最大的币种
                        sorted_candidates = sorted(open_candidates,
                                                 key=lambda x: abs(
                                                     x["funding_diff"]),
                                                 reverse=True)

                        # 记录排序后的候选币种
                        candidate_info = []
                        for c in sorted_candidates:
                            candidate_info.append(
                                f"{c['symbol']}({abs(c['funding_diff']):.6f})")
                        self.logger.info(f"按资金费率差排序后的候选币种: {candidate_info}")

                        # 获取最大持仓数量限制应该在循环外
                        max_positions_count = self.config.get(
                            "strategy", {}).get("max_positions_count", 5)

                        # 循环处理候选币种，直到达到最大持仓限制
                        for candidate in sorted_candidates:
                            # 每次开仓前重新获取当前持仓状态
                            bp_positions = await self.backpack_api.get_positions()
                            hl_positions = await self.hyperliquid_api.get_positions()

                            # 计算当前持仓数量
                            current_position_symbols = set()

                            # 统计Backpack持仓 (修复缩进)
                            for pos in bp_positions.values():
                                if float(pos.get("quantity", 0)) != 0:
                                    pos_symbol = pos.get(
                                        "symbol", "").split("-")[0]
                                    current_position_symbols.add(pos_symbol)

                            # 统计Hyperliquid持仓
                            for coin, pos in hl_positions.items():
                                if pos.get("size", 0) != 0:
                                    current_position_symbols.add(coin)

                            current_count = len(current_position_symbols)

                            # 检查当前持仓数量是否已达到最大限制
                            if current_count >= max_positions_count:
                                self.logger.warning(
                                    f"已达到最大持仓数量限制({max_positions_count})，停止开仓")
                                message = f"已达到最大持仓数量限制({max_positions_count})，当前持仓: {', '.join(sorted(current_position_symbols))}"
                                self._safe_add_condition_message(message)
                                break # break 在 for candidate 循环内，修正后linter应不再报错

                            # 检查当前候选币种是否已有持仓
                            if candidate["symbol"] in current_position_symbols:
                                self.logger.warning(
                                    f"{candidate['symbol']}已有持仓，跳过")
                                continue # continue 在 for candidate 循环内，修正后linter应不再报错

                            # 尝试开仓
                            self.logger.info(
                                f"尝试开仓资金费率差最大的币种: {candidate['symbol']}(差异: {abs(candidate['funding_diff']):.6f})")
                            try:
                                success = await self._open_position(
                                    candidate["symbol"],
                                    candidate["funding_diff"],
                                    candidate["bp_funding"],
                                    candidate["hl_funding"],
                                    candidate["available_size"]
                                )

                                if success:
                                    self.logger.info(
                                        f"{candidate['symbol']}开仓成功")
                                    # 开仓成功后等待5秒，确保API数据同步
                                    await asyncio.sleep(5)
                                    # 开一个仓位后退出循环，下次循环再考虑开下一个
                                    break # break 在 for candidate 循环内，修正后linter应不再报错
                                else:
                                    self.logger.warning(
                                        f"{candidate['symbol']}开仓失败")
                                    # 失败后等待3秒再尝试下一个币种
                                    await asyncio.sleep(3)
                            except Exception as e:
                                self.logger.error(
                                    f"开仓{candidate['symbol']}时出错: {e}")
                                await asyncio.sleep(3)

                    # 批量执行平仓操作 - 平仓逻辑保持不变 (修复缩进)
                    if close_candidates:
                        self.logger.info(
                            f"批量平仓: 共{len(close_candidates)}个币种符合平仓条件")
                        for candidate in close_candidates:
                            try:
                                await self._close_position(
                                    candidate["symbol"],
                                    candidate["position"]
                                )
                                # 每次平仓后添加短暂延迟，避免API限制但又不至于等待太久
                                await asyncio.sleep(3)  # 增加到3秒，确保API状态更新
                            except Exception as e:
                                self.logger.error(
                                    f"执行{candidate['symbol']}批量平仓时出错: {e}")
                        
                    # 等待下一次检查 (修复缩进)
                    await asyncio.sleep(self.config["strategy"]["check_interval"])
                    
                except Exception as e: # 修复缩进并添加 except (针对 try at 964)
                    self.logger.error(f"主循环发生错误: {e}") # 修复缩进
                    print(f"主循环错误: {e}", file=sys.__stdout__)
                    await asyncio.sleep(5)  # 发生错误时等待5秒
                    
        except Exception as e: # 修复缩进并添加 except (针对 try at 867)
            self.logger.error(f"启动套利引擎时发生错误: {e}") # 修复缩进
            print(f"启动引擎错误: {str(e)}", file=sys.__stdout__)
        finally: # 修复缩进
            # 停止显示
            if self.display_manager: # 修复缩进
                print("停止显示...", file=sys.__stdout__)
                self.display_manager.stop()
                print("显示已停止", file=sys.__stdout__)

    async def _check_open_conditions_without_execution(
        self,
        symbol,
        funding_diff,
        bp_funding,
        hl_funding,
        adjusted_hl_funding,  # 添加调整后的8小时资金费率参数
        available_size,
        bp_positions_for_opening,
        hl_positions_for_opening,
    ):
        """
        检查是否满足开仓条件但不执行开仓
        """
        # 从配置中获取最大持仓数量限制，确保正确使用嵌套结构读取
        # 确保无论配置如何变化都能正确读取
        max_positions_count = 5  # 默认保守值
        
        # 优先从config["strategy"]["max_positions_count"]读取
        if "strategy" in self.config and isinstance(self.config["strategy"], dict):
            max_positions_count = self.config["strategy"].get("max_positions_count", max_positions_count)
        # 向后兼容旧配置，如果直接放在根层级
        elif "max_positions_count" in self.config:
            max_positions_count = self.config.get("max_positions_count", max_positions_count)
            
        # 记录使用的max_positions_count值，便于调试
        self.logger.debug(f"使用最大持仓数量限制: {max_positions_count}")

        # 不再使用本地position_directions记录，而是仅使用API获取的实时数据
        api_positions = set()
        unique_position_symbols = set()

        # 收集Backpack持仓的不同币种
        for pos in bp_positions_for_opening.values():
            if float(pos.get("quantity", 0)) != 0:
                pos_symbol = pos.get("symbol", "").split("-")[0]
                unique_position_symbols.add(pos_symbol)
                api_positions.add(pos_symbol)

        # 收集Hyperliquid持仓的不同币种
        for coin, pos in hl_positions_for_opening.items():
            if pos.get("size", 0) != 0:
                unique_position_symbols.add(coin)
                api_positions.add(coin)

        # 同步开仓时间和资金费率记录与实际持仓
        self._sync_records_with_api_positions(api_positions)

        # 使用不同币种的总数，而不是不同持仓的总数
        current_positions_count = len(unique_position_symbols)

        # 增加日志详细度，记录当前持仓情况
        self.logger.info(
            f"当前持有{current_positions_count}个不同币种的仓位，最大允许{max_positions_count}个")
        self.logger.info(
            f"持仓币种列表: {', '.join(sorted(unique_position_symbols))}")

        # 检查该币种是否已有持仓（仅依赖API获取的实时数据）
        if symbol in unique_position_symbols:
            self.logger.warning(f"{symbol}已有持仓，不建议再次开仓")
            message = f"{symbol}已有持仓，不建议再次开仓"
            self._safe_add_condition_message(message)
            return False

        # 检查是否已经达到最大持仓数量限制
        if current_positions_count >= max_positions_count:
            self.logger.warning(
                f"已达到最大持仓数量限制({max_positions_count})，无法为{symbol}开仓")
            self.logger.warning(
                f"当前持仓币种: {', '.join(sorted(unique_position_symbols))}")
            message = f"已达到最大持仓数量限制({max_positions_count})，无法为{symbol}开仓"
            self._safe_add_condition_message(message)
            return False

        # 2. 获取交易对配置
        trading_pair_config = None
        for pair_config in self.config.get("trading_pairs", []):
            if pair_config.get("symbol") == symbol:
                trading_pair_config = pair_config
                break
        
        if trading_pair_config is None:
            self.logger.warning(f"未找到{symbol}的交易对配置")
            return False

        # 3. 检查最大持仓限制
        max_position_size = trading_pair_config.get("max_position_size", 0)
        if max_position_size <= 0:
            message = f"{symbol}未设置有效的最大持仓大小，无法开仓"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 4. 检查滑点前计算可交易大小
        # 先计算基础大小
        size = min(available_size, max_position_size)

        # 获取Backpack和Hyperliquid的价格和深度数据
        data = await self.data_manager.get_data(symbol)
        if not data:
            message = f"无法获取{symbol}的市场数据，无法开仓"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 检查深度数据是否存在，如果不存在则尝试获取
        bp_depth = data["backpack"].get("depth", {})
        hl_depth = data["hyperliquid"].get("depth", {})

        # 如果深度数据不存在，尝试从交易所API获取
        if not bp_depth or not hl_depth:
            try: # try at 1246
                self.logger.info(f"尝试获取{symbol}的深度数据...")

                # 获取Backpack订单深度数据
                bp_symbol = get_backpack_symbol(symbol)
                max_retries = 2
                bp_success = False

                for retry in range(max_retries):
                    try:
                        bp_orderbook = await self.backpack_api.get_orderbook(bp_symbol)
                        if bp_orderbook and bp_orderbook.get("bids") and bp_orderbook.get("asks"):
                            data["backpack"]["depth"] = bp_orderbook
                            bp_depth = bp_orderbook
                            self.logger.info(
                                f"成功获取{bp_symbol}的Backpack深度数据，尝试次数: {retry+1}")
                            bp_success = True
                            break
                        else:
                            self.logger.warning(
                                f"获取的{bp_symbol}的Backpack深度数据无效，尝试次数: {retry+1}")
                    except Exception as e:
                        self.logger.error(
                            f"获取{bp_symbol}的Backpack深度数据出错(尝试{retry+1}): {e}")
                        await asyncio.sleep(0.5)  # 延迟后重试

                if not bp_success:
                    self.logger.error(
                        f"无法获取{bp_symbol}的Backpack深度数据，已重试{max_retries}次")

                # 获取Hyperliquid订单深度数据
                hl_success = False
                for retry in range(max_retries):
                    try: # try at 1280
                        hl_orderbook = await self.hyperliquid_api.get_orderbook(symbol)
                        if hl_orderbook and hl_orderbook.get("bids") and hl_orderbook.get("asks"):
                            data["hyperliquid"]["depth"] = hl_orderbook
                            hl_depth = hl_orderbook
                            self.logger.info( # 修复缩进 (从原1285行开始)
                                f"成功获取{symbol}的Hyperliquid深度数据，尝试次数: {retry+1}")
                            hl_success = True # 修复缩进
                            break # break 在 for retry 循环内，修正后linter应不再报错
                        else: # 修复缩进
                            self.logger.warning( # 修复缩进
                                f"获取的{symbol}的Hyperliquid深度数据无效，尝试次数: {retry+1}")
                    except Exception as e: # 修复缩进并添加 except (针对 try at 1280)
                        self.logger.error( # 修复缩进
                            f"获取{symbol}的Hyperliquid深度数据出错(尝试{retry+1}): {e}")
                        await asyncio.sleep(0.5)  # 延迟后重试

                if not hl_success: # 修复缩进
                    self.logger.error(
                        f"无法获取{symbol}的Hyperliquid深度数据，已重试{max_retries}次")

                # 记录深度数据获取结果 (修复缩进)
                if bp_success and hl_success: # 修复缩进
                    self.logger.info(f"成功获取{symbol}的两个交易所深度数据")
                elif bp_success: # 修复缩进
                    self.logger.warning(
                        f"只获取了{symbol}的Backpack深度数据，Hyperliquid深度数据获取失败")
                elif hl_success: # 修复缩进
                    self.logger.warning(
                        f"只获取了{symbol}的Hyperliquid深度数据，Backpack深度数据获取失败")
                else: # 修复缩进
                    self.logger.error(f"两个交易所的{symbol}深度数据均获取失败")

            except Exception as e: # 修复缩进并添加 except (针对 try at 1246)
                self.logger.error(f"获取{symbol}深度数据时出错: {e}") # 修复缩进
                import traceback
                self.logger.error(f"详细错误信息: {traceback.format_exc()}")

        # 5. 副本版本的滑点计算 - 早期过滤
        # 获取滑点信息
        bp_price = data["backpack"]["price"]
        hl_price = data["hyperliquid"]["price"]

        if not bp_price or not hl_price:
            message = f"{symbol}无法获取有效价格，无法开仓"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 计算建议操作方向
        # 如果资金费率差小于0，建议在Backpack做多，在Hyperliquid做空
        # 如果资金费率差大于0，建议在Backpack做空，在Hyperliquid做多
        bp_side = "BUY" if funding_diff < 0 else "SELL"
        hl_side = "SELL" if funding_diff < 0 else "BUY"

        # 再次检查深度数据
        if not bp_depth or not hl_depth:
            message = f"{symbol}无法获取有效深度数据，无法计算滑点"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 获取买卖盘数据
        bp_bids = bp_depth.get("bids", [])
        bp_asks = bp_depth.get("asks", [])
        hl_bids = hl_depth.get("bids", [])
        hl_asks = hl_depth.get("asks", [])

        # 确保有足够的深度数据
        if len(bp_bids) < 3 or len(bp_asks) < 3 or len(hl_bids) < 3 or len(hl_asks) < 3:
            message = f"{symbol}深度数据不足，无法准确计算滑点"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 计算滑点
        bp_slippage = self._estimate_slippage(
            bp_side, size, bp_price, bp_bids, bp_asks)
        hl_slippage = self._estimate_slippage(
            hl_side, size, hl_price, hl_bids, hl_asks)

        # 计算百分比滑点
        bp_slippage_percent = (bp_slippage / bp_price) * 100 if bp_price else 0
        hl_slippage_percent = (hl_slippage / hl_price) * 100 if hl_price else 0

        # 记录滑点信息
        self.logger.info(
            f"{symbol} - BP滑点: {bp_slippage_percent:.4f}%, HL滑点: {hl_slippage_percent:.4f}%"
        )

        # 检查滑点是否超过阈值
        max_slippage_percent = self.config.get("strategy", {}).get(
            "open_conditions", {}).get("max_slippage_percent", 0.3)
        ignore_high_slippage = self.config.get("strategy", {}).get(
            "open_conditions", {}).get("ignore_high_slippage", False)

        # 只有在不忽略高滑点的情况下才检查滑点限制
        if not ignore_high_slippage and (bp_slippage_percent > max_slippage_percent or hl_slippage_percent > max_slippage_percent):
            message = (
                f"{symbol} - 滑点过大: BP={bp_slippage_percent:.4f}%, HL={hl_slippage_percent:.4f}%, "
                f"超过{max_slippage_percent}%的限制，不开仓"
            )
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 6. 检查价格条件
        min_price_diff_percent = self.config.get("min_price_diff_percent", 0.1)
        price_diff_percent = abs((bp_price - hl_price) / bp_price) * 100

        self.logger.info(
            f"{symbol} - 当前价差: {price_diff_percent:.4f}%, 最低要求: {min_price_diff_percent}%"
        )

        # 只有当价格差异较大时才发出警告
        if price_diff_percent > min_price_diff_percent:
            message = (
                f"{symbol} - 价格差异较大: {price_diff_percent:.4f}% > {min_price_diff_percent}%, "
                f"BP={bp_price}, HL={hl_price}"
            )
            self.logger.warning(message)
            self._safe_add_condition_message(message)

        # 7. 检查资金费率条件
        min_funding_diff = self.config.get("strategy", {}).get(
            "open_conditions", {}).get("min_funding_diff", 0.0005)
        
        # 使用bp_funding和adjusted_hl_funding计算8小时差值
        eight_hour_funding_diff = bp_funding - adjusted_hl_funding
        abs_eight_hour_funding_diff = abs(eight_hour_funding_diff)
        
        self.logger.info(
            f"{symbol} - 当前资金费率差(8小时): {abs_eight_hour_funding_diff:.6f}, 最低要求: {min_funding_diff}"
        )

        if abs_eight_hour_funding_diff < min_funding_diff:
            message = f"{symbol}资金费率差异过小(8小时): {abs_eight_hour_funding_diff:.6f} < {min_funding_diff}，不建议开仓"
            self.logger.warning(message)
            self._safe_add_condition_message(message)
            return False

        # 所有条件都满足
        self.logger.info(f"{symbol}满足所有开仓条件，建议开仓")

        direction = "BP做多HL做空" if eight_hour_funding_diff < 0 else "BP做空HL做多"
        message = (
            f"{symbol} 建议开仓 - {direction}, "
            f"资金费率差(8小时): {abs_eight_hour_funding_diff:.6f} "
            f"(BP: {bp_funding:.6f}, HL调整后: {adjusted_hl_funding:.6f})"
        )

        self._safe_add_condition_message(message)

        return True

    async def _check_close_conditions_without_execution(
        self,
        symbol: str,
        bp_position: dict,
        hl_position: dict,
        bp_price: float,
        hl_price: float,
        bp_funding: float,
        adjusted_hl_funding: float,
        price_diff_percent: float,
        funding_diff: float,
        funding_diff_sign: int
    ):
        """
        检查是否满足平仓条件，但不执行平仓
        
        Args:
            symbol: 基础币种，如 "BTC"
            bp_position: Backpack持仓信息
            hl_position: Hyperliquid持仓信息
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            bp_funding: Backpack资金费率
            adjusted_hl_funding: 调整后的Hyperliquid资金费率
            price_diff_percent: 价格差异（百分比）
            funding_diff: 资金费率差异
            funding_diff_sign: 资金费率差异符号

        Returns:
            tuple: (should_close, reason, position)
        """
        # 获取平仓条件配置
        close_conditions = self.config.get(
            "strategy", {}).get(
            "close_conditions", {})
        condition_type = close_conditions.get("condition_type", "any")
        
        # 检查持仓时间是否足够长
        min_position_time = close_conditions.get(
            "min_position_time", 600)  # 默认最小持仓10分钟
        current_time = time.time()
        open_time = self.position_open_times.get(symbol, 0)

        if open_time > 0:  # 如果有记录开仓时间
            position_duration = current_time - open_time
            if position_duration < min_position_time:
                self.logger.debug(
                    f"{symbol} - 持仓时间过短({position_duration:.0f}秒<{min_position_time}秒)，暂不平仓")
                return False, "持仓时间过短", None

        # 资金费率差异符号变化条件已弃用，新增方向反转检查
        # 计算当前市场状况下的建议开仓方向
        is_consistent, bp_current_side, hl_current_side = self.check_direction_consistency(
            symbol, bp_price, hl_price, bp_funding, adjusted_hl_funding)

        # 从持仓获取实际开仓方向
        bp_entry_side = bp_position.get("side", "UNKNOWN")
        hl_entry_side = hl_position.get("side", "UNKNOWN")

        # 判断方向是否反转（现在建议的方向与开仓方向相反）
        bp_direction_reversed = (bp_entry_side == "BUY" and bp_current_side == "SELL") or \
            (bp_entry_side == "SELL" and bp_current_side == "BUY")
        hl_direction_reversed = (hl_entry_side == "BUY" and hl_current_side == "SELL") or \
            (hl_entry_side == "SELL" and hl_current_side == "BUY")

        # 方向反转条件：双向反转即可，不需要方向一致
        direction_reversed = bp_direction_reversed and hl_direction_reversed

        self.logger.info(
            f"{symbol} - 方向反转检查: 持仓方向(BP={bp_entry_side}, HL={hl_entry_side})，"
            f"当前建议方向(BP={bp_current_side}, HL={hl_current_side})，"
            f"方向反转={direction_reversed}，方向一致={is_consistent}"
        )

        # 如果方向未反转，说明还未到平仓时机
        if not direction_reversed:
            return False, "方向未反转，不满足平仓条件", None

        # 保留原有价格差异检查
        price_diff_sign = 1 if price_diff_percent > 0 else -1
        
        # 资金费率差异最小值条件（当差异过小，无套利空间时平仓）
        min_funding_diff = close_conditions.get("min_funding_diff", 0.000005)
        
        # 价格差异条件（获利/止损）
        min_profit_percent = close_conditions.get("min_profit_percent", 0.1)
        max_loss_percent = close_conditions.get("max_loss_percent", 0.3)
        
        # 检查滑点条件
        max_close_slippage = close_conditions.get(
            "max_close_slippage_percent", 0.25)
        ignore_close_slippage = close_conditions.get(
            "ignore_close_slippage", False)

        # 获取市场数据中的滑点信息
        market_data = self.data_manager.get_all_data()
        total_slippage = market_data.get(
            symbol, {}).get(
            "total_slippage", 0.5)  # 默认为0.5%，较高值

        # 检查平仓滑点是否超过限制
        slippage_condition_met = ignore_close_slippage or total_slippage <= max_close_slippage

        if not slippage_condition_met:
            self.logger.debug(
                f"{symbol} - 预估平仓滑点({total_slippage:.4f}%)超过最大允许值({max_close_slippage:.4f}%)，暂不纳入平仓候选")
            return False, f"滑点过高({total_slippage:.4f}%)", None

        # 检查资金费率和价格差异条件
        funding_condition_met = abs(funding_diff) >= min_funding_diff
        price_condition_met = abs(price_diff_percent) >= min_profit_percent
        
        # 根据条件类型决定是否平仓
        should_close = False
        reason = ""
        
        if condition_type == "any":
            # 满足任一条件即可平仓（但必须满足方向反转）
            should_close = (funding_condition_met or price_condition_met)
            if funding_condition_met:
                reason = f"资金费率差异({abs(funding_diff):.6f})满足条件且方向已反转，确认盈利"
            elif price_condition_met:
                reason = f"价格差异({abs(price_diff_percent):.4f}%)满足条件且方向已反转，确认盈利"
        elif condition_type == "all":
            # 必须同时满足所有条件才能平仓
            should_close = funding_condition_met and price_condition_met
            reason = "同时满足资金费率差异和价格差异条件且方向已反转，确认盈利"
        elif condition_type == "funding_only":
            # 仅考虑资金费率条件
            should_close = funding_condition_met
            reason = f"资金费率差异({abs(funding_diff):.6f})满足条件且方向已反转，确认盈利"
        elif condition_type == "price_only":
            # 仅考虑价格差异条件
            should_close = price_condition_met
            reason = f"价格差异({abs(price_diff_percent):.4f}%)满足条件且方向已反转，确认盈利"
        
        # 记录条件判断结果
        self.logger.info(
            f"{symbol} - 平仓条件检查: 资金费率条件{'' if funding_condition_met else '未'}满足 "
            f"(差异: {abs(funding_diff):.6f}, 阈值: {min_funding_diff}), "
            f"价格条件{'' if price_condition_met else '未'}满足 "
            f"(差异: {abs(price_diff_percent):.4f}%, 阈值: {min_profit_percent}%), "
            f"方向反转条件{'' if direction_reversed else '未'}满足"
        )
            
            # 创建持仓对象 (修复缩进)
        position = {
                "bp_symbol": get_backpack_symbol(symbol), # 再次修正字典内缩进
                "hl_symbol": symbol, # 再次修正字典内缩进
                "bp_side": bp_position["side"], # 再次修正字典内缩进
                "hl_side": hl_position["side"], # 再次修正字典内缩进
                "bp_size": bp_position["size"], # 再次修正字典内缩进
                "hl_size": hl_position["size"], # 再次修正字典内缩进
                # 添加完整的持仓信息，以满足_close_position的需求
                "backpack": bp_position, # 再次修正字典内缩进
                "hyperliquid": hl_position # 再次修正字典内缩进
            }

        # 记录详细的持仓信息以便调试
        self.logger.debug(f"{symbol}平仓持仓信息详情: {position}")

        return should_close, reason, position

    async def _open_position(
            self,
            symbol: str,
            funding_diff: float,
            bp_funding: float,
            hl_funding: float,
            available_size: float = None):
        """
        开仓
        
        Args:
            symbol: 基础币种，如 "BTC"
            funding_diff: 资金费率差
            bp_funding: Backpack资金费率
            hl_funding: Hyperliquid资金费率
            available_size: 可用的剩余开仓量，如果为None则使用配置中的开仓数量
        """
        try:
            # 获取最新数据
            data = await self.data_manager.get_data(symbol)
            
            # 获取价格
            bp_price = data["backpack"]["price"]
            hl_price = data["hyperliquid"]["price"]
            
            if bp_price is None or hl_price is None:
                self.logger.error(f"{symbol}价格数据无效，无法开仓")
                return
            
            # 获取交易对配置
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair["symbol"] == symbol:
                    trading_pair_config = pair
                    break
            
            if not trading_pair_config:
                self.logger.error(f"未找到{symbol}的交易对配置")
                return
            
            # 获取最大持仓数量和最小交易量
            max_position_size = trading_pair_config.get("max_position_size")
            min_volume = trading_pair_config.get("min_volume")
            
            # 计算开仓数量
            bp_size = available_size if available_size is not None else self.position_sizes[
                symbol]
            hl_size = bp_size  # 两个交易所使用相同的开仓数量
            
            # 检查是否小于最小交易量
            if bp_size < min_volume:
                self.logger.warning(
                    f"{symbol}开仓数量({bp_size})小于最小交易量({min_volume})，已调整为最小交易量")
                bp_size = min_volume
                hl_size = min_volume
            
            # 检查是否超过最大持仓数量
            if max_position_size is not None and (bp_size > max_position_size):
                self.logger.warning(
                    f"{symbol}开仓数量({bp_size})超过最大持仓数量({max_position_size})，已调整为最大持仓数量")
                bp_size = max_position_size
                hl_size = max_position_size
            
            # 计算资金费率差
            funding_diff, funding_diff_sign = calculate_funding_diff(
                bp_funding, hl_funding)
            
            # 记录当前的资金费率符号用于后续平仓判断
            self.funding_diff_signs[symbol] = funding_diff_sign
            self.logger.debug(f"记录{symbol}开仓时的资金费率符号: {funding_diff_sign}")
            # 保存资金费率符号记录到文件
            self._save_funding_diff_signs()

            # 计算价格差异符号并记录
            price_diff_percent = (bp_price - hl_price) / hl_price * 100
            price_diff_sign = 1 if price_diff_percent > 0 else -1

            # 确保price_diff_signs字典存在
            if not hasattr(self, 'price_diff_signs'):
                self.price_diff_signs = {}

            # 记录当前的价格差符号用于后续平仓判断
            self.price_diff_signs[symbol] = price_diff_sign
            self.logger.debug(f"记录{symbol}开仓时的价格差符号: {price_diff_sign}")
            
            # 准备仓位数据
            bp_symbol = get_backpack_symbol(
                symbol)  # 使用正确的交易对格式，如 BTC_USDC_PERP
            hl_symbol = get_hyperliquid_symbol(symbol)

            # 检查是否存在已经计算好的开仓方向
            if hasattr(
                    self,
                    'preferred_sides') and symbol in self.preferred_sides:
                # 使用方向一致性检查时预先计算的方向
                bp_side = self.preferred_sides[symbol]["bp_side"]
                hl_side = self.preferred_sides[symbol]["hl_side"]
                self.logger.info(
                    f"{symbol} - 使用方向一致性检查确定的交易方向: BP={bp_side}, HL={hl_side}")

                # 使用完后清除，避免影响下次开仓
                del self.preferred_sides[symbol]
            else:
                # 修正资金费率套利逻辑
                # 1. 当两个交易所资金费率正负相反时：负的做多，正的做空
                # 2. 当两个交易所资金费率都为负时：绝对值大的做多，绝对值小的做空
                # 3. 当两个交易所资金费率都为正时：值大的做空，值小的做多
                if (bp_funding < 0 and hl_funding < 0):
                    # 两个交易所资金费率都为负
                    if abs(bp_funding) > abs(hl_funding):
                        # BP资金费率绝对值更大，BP做多，HL做空
                        bp_side = "BUY"
                        hl_side = "SELL"
                    else:
                        # HL资金费率绝对值更大，HL做多，BP做空
                        bp_side = "SELL"
                        hl_side = "BUY"
                elif (bp_funding > 0 and hl_funding > 0):
                    # 两个交易所资金费率都为正
                    if bp_funding > hl_funding:
                        # BP资金费率更大，BP做空，HL做多
                        bp_side = "SELL"
                        hl_side = "BUY"
                    else:
                        # HL资金费率更大，HL做空，BP做多
                        bp_side = "BUY"
                        hl_side = "SELL"
                else:
                    # 两个交易所资金费率正负相反，保持原有逻辑
                    bp_side = "SELL" if bp_funding > 0 else "BUY"
                    hl_side = "SELL" if hl_funding > 0 else "BUY"

            # 记录资金费率和交易方向
            self.logger.info(
                f"{symbol} - BP资金费率: {bp_funding:.6f}，方向: {bp_side}；HL资金费率: {hl_funding:.6f}，方向: {hl_side}")

            # 获取交易前的持仓状态
            self.logger.info(f"获取{symbol}开仓前的持仓状态")
            pre_bp_positions = await self.backpack_api.get_positions()
            pre_hl_positions = await self.hyperliquid_api.get_positions()

            # 记录开仓前的持仓状态
            pre_bp_position = None
            for pos in pre_bp_positions.values():
                if pos.get("symbol") == bp_symbol:
                    pre_bp_position = pos
                    break # 修复缩进

            pre_hl_position = None
            for pos in pre_hl_positions.values():
                if pos.get("symbol") == hl_symbol:
                    pre_hl_position = pos
                    break # 修复缩进
                                
            self.logger.info(
                f"开仓前持仓: BP {bp_symbol}={pre_bp_position}, HL {hl_symbol}={pre_hl_position}")

            # ===== 同时下单 =====
            self.logger.info(
                f"同时在两个交易所为{symbol}下单: BP {bp_side} {bp_size}, HL {hl_side} {hl_size}")

            # 获取价格精度和tick_size
            price_precision = trading_pair_config.get("price_precision", 3)
            tick_size = trading_pair_config.get("tick_size", 0.001)

            # 在Hyperliquid下限价单
            hl_price_adjuster = 1.005 if hl_side == "BUY" else 0.995
            hl_limit_price = hl_price * hl_price_adjuster

            # 使用正确的tick_size对限价单价格进行调整
            hl_limit_price = round(hl_limit_price / tick_size) * tick_size
            hl_limit_price = round(hl_limit_price, price_precision)

            self.logger.info(
                f"使用限价单开仓Hyperliquid: 价格={hl_limit_price}, 精度={price_precision}, tick_size={tick_size}")

            # 同时发送两个交易所的订单
            bp_order_task = asyncio.create_task(
                self.backpack_api.place_order(
                    symbol=bp_symbol,
                    side=bp_side,
                    size=bp_size,
                    price=None,  # 市价单不需要价格
                    order_type="MARKET"  # Backpack使用市价单
                )
            )

            hl_order_task = asyncio.create_task(
                self.hyperliquid_api.place_order(
                    symbol=hl_symbol,
                    side=hl_side,
                    size=hl_size,
                    price=None,  # 使用市价单简化订单处理
                    order_type="MARKET"
                )
            )

            # 等待订单结果
            bp_result, hl_result = await asyncio.gather(
                bp_order_task,
                hl_order_task,
                return_exceptions=True
            )

            # 检查两个交易所的订单结果
            bp_success = not isinstance(bp_result, Exception) and not (
                isinstance(bp_result, dict) and bp_result.get("error"))

            # 增强的Hyperliquid订单成功检查逻辑
            hl_success = False
            if not isinstance(hl_result, Exception):
                if isinstance(hl_result, dict):
                    # 检查直接的success标志
                    if hl_result.get("success", False):
                        hl_success = True
                        hl_order_id = hl_result.get("order_id", "未知")
                        self.logger.info(
                            f"Hyperliquid开仓订单成功，订单ID: {hl_order_id}")

                        # 检查订单是否已立即成交
                        if hl_result.get("status") == "filled":
                            self.logger.info(
                                f"Hyperliquid开仓订单已立即成交，均价: {hl_result.get('price', '未知')}")

                    # 检查是否包含filled状态
                    elif "raw_response" in hl_result:
                        raw_response = hl_result["raw_response"]
                        raw_str = json.dumps(raw_response)

                        if "filled" in raw_str:
                            self.logger.info("检测到开仓订单可能已成交")
                            hl_success = True

            # 记录订单结果
            if bp_success:
                self.logger.info(f"Backpack开仓订单成功: {bp_result}")
            else:
                self.logger.error(f"Backpack开仓订单失败: {bp_result}")

            if hl_success:
                self.logger.info(f"Hyperliquid开仓订单成功")
            else:
                self.logger.error(f"Hyperliquid开仓订单失败: {hl_result}")

            # ===== 添加单边重试逻辑 =====
            max_retries = 3  # 最大重试次数

            # 重试Backpack (如果失败且Hyperliquid成功)
            if not bp_success and hl_success:
                self.logger.info(f"Hyperliquid开仓成功但Backpack失败，开始重试Backpack下单")
                for retry in range(max_retries):
                    try:
                        # 先检查是否已有持仓（防止重复下单）
                        position = await self.backpack_api.get_position(bp_symbol)
                        if position:
                            self.logger.info(f"检测到已存在Backpack持仓，不再重试")
                            bp_success = True
                            break

                        # 重试下单
                        self.logger.info(
                            f"重试Backpack开仓 ({retry+1}/{max_retries}): {bp_symbol} {bp_side} {bp_size}")
                        bp_result = await self.backpack_api.place_order(
                            symbol=bp_symbol,
                            side=bp_side,
                            size=bp_size,
                            price=None,
                            order_type="MARKET"
                        )
                        bp_success = not isinstance(bp_result, Exception) and not (
                            isinstance(bp_result, dict) and bp_result.get("error"))

                        if bp_success:
                            self.logger.info(f"Backpack重试下单成功: {bp_result}")
                            break
                        else:
                            self.logger.error(f"Backpack重试下单失败: {bp_result}")
                    except Exception as e:
                        self.logger.error(f"Backpack重试失败: {e}")

                    # 重试间隔，避免过于频繁请求
                    await asyncio.sleep(1)

            # 重试Hyperliquid (如果失败且Backpack成功)
            if not hl_success and bp_success:
                self.logger.info(
                    f"Backpack开仓成功但Hyperliquid失败，开始重试Hyperliquid下单")
                for retry in range(max_retries):
                    try:
                        # 先检查是否已有持仓
                        position = await self.hyperliquid_api.get_position(hl_symbol)
                        if position:
                            self.logger.info(f"检测到已存在Hyperliquid持仓，不再重试")
                            hl_success = True
                            break

                        # 重试下单
                        self.logger.info(
                            f"重试Hyperliquid开仓 ({retry+1}/{max_retries}): {hl_symbol} {hl_side} {hl_size}")
                        hl_result = await self.hyperliquid_api.place_order(
                            symbol=hl_symbol,
                            side=hl_side,
                            size=hl_size,
                            price=None,
                            order_type="MARKET"
                        )

                        # 检查结果
                        hl_success = False
                        if isinstance(hl_result, dict):
                            if hl_result.get("success", False):
                                hl_success = True
                                hl_order_id = hl_result.get("order_id", "未知")
                                self.logger.info(
                                    f"Hyperliquid重试下单成功，订单ID: {hl_order_id}")
                            elif "raw_response" in hl_result:
                                raw_str = json.dumps(hl_result["raw_response"])
                                if "filled" in raw_str:
                                    self.logger.info("检测到重试订单可能已成交")
                                    hl_success = True

                        if hl_success:
                            break
                        else:
                            self.logger.error(
                                f"Hyperliquid重试下单失败: {hl_result}")
                    except Exception as e:
                        self.logger.error(f"Hyperliquid重试失败: {e}")

                    # 重试间隔
                    await asyncio.sleep(1)

            # ===== 验证持仓变化 =====
            # 等待3秒让交易所处理订单
            self.logger.info("等待3秒让交易所处理订单...")
            await asyncio.sleep(3)

            # 获取交易后的持仓状态
            self.logger.info(f"获取{symbol}开仓后的持仓状态")
            post_bp_positions = await self.backpack_api.get_positions()
            post_hl_positions = await self.hyperliquid_api.get_positions()

            # 记录开仓后的持仓状态
            post_bp_position = None
            for pos in post_bp_positions.values():
                if pos.get("symbol") == bp_symbol:
                    post_bp_position = pos
                    break

            post_hl_position = None
            for pos in post_hl_positions.values():
                if pos.get("symbol") == hl_symbol:
                    post_hl_position = pos
                    break

            self.logger.info(
                f"开仓后持仓: BP {bp_symbol}={post_bp_position}, HL {hl_symbol}={post_hl_position}")

            # 验证持仓变化
            bp_position_opened = False
            hl_position_opened = False

            # 检查Backpack持仓变化
            if pre_bp_position is None and post_bp_position is not None:
                # 新开仓位
                bp_position_opened = True
                self.logger.info(f"Backpack成功开仓{bp_symbol}")
            elif pre_bp_position is not None and post_bp_position is not None:
                # 检查持仓大小是否变化
                pre_size = float(pre_bp_position.get(
                    "quantity", pre_bp_position.get("size", 0)))
                post_size = float(post_bp_position.get(
                    "quantity", post_bp_position.get("size", 0)))

                if post_size > pre_size:
                    bp_position_opened = True
                    self.logger.info(
                        f"Backpack {bp_symbol}持仓量增加: {pre_size} -> {post_size}")

            # 检查Hyperliquid持仓变化
            if pre_hl_position is None and post_hl_position is not None:
                # 新开仓位
                hl_position_opened = True
                self.logger.info(f"Hyperliquid成功开仓{hl_symbol}")
            elif pre_hl_position is not None and post_hl_position is not None:
                # 检查持仓大小是否变化
                pre_size = float(pre_hl_position.get(
                    "size", pre_hl_position.get("quantity", 0)))
                post_size = float(post_hl_position.get(
                    "size", post_hl_position.get("quantity", 0)))
                if post_size > pre_size:
                    hl_position_opened = True
                    self.logger.info(
                        f"Hyperliquid {hl_symbol}持仓量增加: {pre_size} -> {post_size}")

            # 根据持仓变化情况判断开仓成功与否
            if bp_position_opened and hl_position_opened:
                # 两个交易所都成功开仓
                message = f"{symbol}开仓成功"
                self.logger.info(message)
                self.display_manager.add_order_message(message)
                # 更新订单统计
                self.display_manager.update_order_stats("open", True)

                # 记录开仓时间
                self.position_open_times[symbol] = time.time()

                # 记录开仓资金费率
                self.entry_funding_rates[symbol] = {
                    "bp_funding": bp_funding,
                    "hl_funding": hl_funding,
                    "diff": hl_funding - bp_funding
                }

                # 记录开仓价格
                self.entry_prices[symbol] = {
                    "bp_price": bp_price,
                    "hl_price": hl_price,
                    "diff_percent": price_diff_percent
                }

                # 保存开仓快照
                self._save_position_snapshot(
                    symbol=symbol,
                    action="open",
                    bp_position=post_bp_position,
                    hl_position=post_hl_position,
                    bp_price=bp_price,
                    hl_price=hl_price,
                    bp_funding=bp_funding,
                    hl_funding=hl_funding
                )

                # 保存开仓时间和资金费率记录
                self._save_positions_and_records()

                # 发送通知 (修复缩进)
                if self.alerter: # 修复缩进
                    self.alerter.send_order_notification(
                        symbol=symbol,
                        action="开仓",
                        quantity=bp_size,
                        price=bp_price,
                        side="多" if bp_side == "BUY" else "空",
                        exchange="Backpack"
                    )
                    # 同样应该发送Hyperliquid的通知
                    self.alerter.send_order_notification(
                        symbol=symbol,
                        action="开仓",
                        quantity=hl_size,
                        price=hl_price,
                        side="多" if hl_side == "BUY" else "空",
                        exchange="Hyperliquid"
                    )

                return True
            elif (not bp_position_opened and hl_position_opened) or (
                    bp_position_opened and not hl_position_opened):
                # 单边开仓成功，即使经过重试仍然只有一边成功
                exchange = "Hyperliquid" if hl_position_opened else "Backpack"
                self.logger.warning(f"{symbol}单边开仓成功，只在{exchange}开仓，需要手动处理")
                # 更新订单统计
                self.display_manager.update_order_stats("open", False)

                # 发送警报
                if self.alerter:
                    await self.alerter.send_alert(f"🚨 检测到单边头寸: {symbol} 只在{exchange}开仓成功，需要手动处理")

                return False
            else:
                # 两个交易所都未成功开仓
                self.logger.error(f"{symbol}在两个交易所均未成功开仓")
                # 更新订单统计
                self.display_manager.update_order_stats("open", False)
                return False
                
        except Exception as e:
            message = f"{symbol}开仓异常: {e}"
            self.logger.error(message)
            if self.display_manager:
                self.display_manager.add_order_message(message)
            return False

    async def _close_position(
        self,
        symbol: str,
        position: Dict[str, Any]
    ):
        """
        根据持仓信息平仓
        
        Args:
            symbol: 交易对基础币种，如"BTC"
            position: 持仓信息字典，包含两个交易所的持仓信息
        """
        self.logger.info(f"开始平仓 {symbol}")

        # 记录接收到的完整持仓信息
        self.logger.debug(f"平仓接收到的持仓信息: {json.dumps(position, default=str)}")

        if self.display_manager:
            self._safe_add_closing_process_message(f"正在平仓 {symbol}...")

        # 获取最新市场数据
        try:
            data = await self.data_manager.get_data(symbol)
            if not data:
                self.logger.error(f"无法获取{symbol}的市场数据，无法平仓")
                self._safe_add_closing_process_message(
                    f"{symbol}平仓失败: 无法获取市场数据")
                return False
        except Exception as e:
            self.logger.error(f"获取{symbol}市场数据时出错: {e}")
            self._safe_add_closing_process_message(f"{symbol}平仓失败: 获取市场数据出错")
            return False

        # 获取持仓信息
        bp_position = position.get("backpack")
        hl_position = position.get("hyperliquid")

        # 记录详细的持仓信息
        self.logger.info(f"{symbol}平仓 - Backpack持仓: {bp_position}")
        self.logger.info(f"{symbol}平仓 - Hyperliquid持仓: {hl_position}")

        # 初始化平仓结果变量
        bp_success = False
        hl_success = False
        bp_order_result = None
        hl_order_result = None

        # 检查是否有持仓
        if not bp_position and not hl_position:
            self.logger.warning(f"{symbol}没有找到任何持仓信息，无法平仓")
            self._safe_add_closing_process_message(f"{symbol}平仓失败: 未找到持仓信息")
            return False

        # 尝试重新获取持仓信息（如果需要）
        if (not bp_position or not hl_position):
            try:
                self.logger.info(f"尝试重新获取{symbol}的持仓信息...")
                bp_positions = await self.backpack_api.get_positions()
                hl_positions = await self.hyperliquid_api.get_positions()

                bp_symbol = get_backpack_symbol(symbol)
                bp_position_new = bp_positions.get(bp_symbol)
                hl_position_new = None
                for pos_symbol, pos in hl_positions.items():
                    if pos_symbol == symbol:
                        hl_position_new = pos
                        break

                if bp_position_new and not bp_position:
                    self.logger.info(f"成功获取{symbol}的Backpack持仓信息")
                    bp_position = bp_position_new

                if hl_position_new and not hl_position:
                    self.logger.info(f"成功获取{symbol}的Hyperliquid持仓信息")
                    hl_position = hl_position_new

            except Exception as e:
                self.logger.error(f"重新获取{symbol}持仓信息时出错: {e}")
                # 继续执行，尝试平仓可用的持仓

        # 获取价格数据
        bp_price = data["backpack"]["price"]
        hl_price = data["hyperliquid"]["price"]

        # 获取资金费率
        bp_funding = data["backpack"]["funding_rate"]
        hl_funding = data["hyperliquid"]["funding_rate"]

        # 调整Hyperliquid资金费率为8小时期
        hl_funding_8h = hl_funding * 8  # 由1小时调整为8小时

        # 获取单次交易数量限制
        # 使用和开仓时相同的单次交易数量限制，避免一次性平仓造成大的滑点
        single_trade_size = None
        if hasattr(self, "position_sizes") and symbol in self.position_sizes:
            single_trade_size = self.position_sizes[symbol]
        else:
            # 如果找不到配置的单次交易量，尝试从交易对配置中获取
            for pair_config in self.config.get("trading_pairs", []):
                if pair_config.get("symbol") == symbol:
                    single_trade_size = pair_config.get("max_position_size")
                    break
        
        if not single_trade_size:
            self.logger.warning(f"未找到{symbol}的单次交易数量配置，将一次性平仓")

        # 尝试平仓Backpack
        if bp_position:
            try:
                # 检查持仓数量
                try:
                    bp_quantity = float(bp_position.get(
                        "quantity", bp_position.get("size", 0)))
                    self.logger.info(f"{symbol}平仓 - Backpack数量: {bp_quantity}")

                    if bp_quantity == 0:
                        self.logger.warning(f"{symbol} Backpack持仓数量为0，无需平仓")
                        self._safe_add_closing_process_message(
                            f"{symbol} Backpack持仓数量为0，无需平仓")
                        bp_success = True  # 设置为True，因为无需平仓
                    else:
                        # 确定平仓方向
                        bp_side = "SELL" if bp_position.get(
                            "side") == "BUY" else "BUY"
                        self.logger.info(f"{symbol}平仓 - Backpack方向: {bp_side}")

                        # 构造正确的Backpack交易对符号
                        bp_symbol = get_backpack_symbol(
                            symbol)  # 使用工具函数获取完整的Backpack交易对

                        # 分批平仓
                        remaining_quantity = bp_quantity
                        bp_success = True  # 假设平仓成功，遇到错误时会设为False
                        
                        if single_trade_size and single_trade_size < bp_quantity:
                            self.logger.info(f"{symbol} Backpack采用分批平仓策略，单次交易量: {single_trade_size}")
                            
                            batch_count = math.ceil(bp_quantity / single_trade_size)
                            self.logger.info(f"{symbol} Backpack总计需要{batch_count}批次平仓")
                            
                            for batch in range(1, batch_count + 1):
                                # 计算当前批次的交易量
                                current_batch_size = min(single_trade_size, remaining_quantity)
                                current_batch_size = round(current_batch_size, 8)  # 避免浮点数精度问题
                                
                                if current_batch_size <= 0:
                                    break
                                
                                self.logger.info(
                                    f"正在Backpack下单平仓(批次{batch}/{batch_count}): {bp_symbol} {bp_side} {current_batch_size}")
                                
                                bp_order_result = await self.backpack_api.place_order(
                                    symbol=bp_symbol,
                                    side=bp_side,
                                    order_type="MARKET",
                                    size=current_batch_size,
                                    price=None
                                )
                                
                                # 检查结果
                                if bp_order_result and bp_order_result.get("status") == "Filled":
                                    self.logger.info(
                                        f"Backpack {symbol} 平仓批次{batch}/{batch_count}成功: {bp_order_result}")
                                    self._safe_add_closing_process_message(
                                        f"Backpack {symbol} 平仓批次{batch}/{batch_count}成功")
                                    
                                    # 更新剩余数量
                                    remaining_quantity -= current_batch_size
                                    
                                    # 添加短暂延迟，避免频繁请求
                                    await asyncio.sleep(1)
                                else:
                                    self.logger.error(
                                        f"Backpack {symbol} 平仓批次{batch}/{batch_count}失败: {bp_order_result}")
                                    self._safe_add_closing_process_message(
                                        f"Backpack {symbol} 平仓批次{batch}/{batch_count}失败")
                                    bp_success = False
                                    break
                        else:
                            # 如果单次交易量未配置或大于持仓量，直接一次性平仓
                            self.logger.info(
                                f"正在Backpack一次性下单平仓: {bp_symbol} {bp_side} {bp_quantity}")
                            
                            bp_order_result = await self.backpack_api.place_order(
                                symbol=bp_symbol,
                                side=bp_side,
                                order_type="MARKET",
                                size=bp_quantity,
                                price=None
                            )

                            # 检查结果
                            if bp_order_result and bp_order_result.get("status") == "Filled":
                                self.logger.info(
                                    f"Backpack {symbol} 平仓成功: {bp_order_result}")
                                self._safe_add_closing_process_message(
                                    f"Backpack {symbol} 平仓成功")
                            else:
                                self.logger.error(
                                    f"Backpack {symbol} 平仓失败: {bp_order_result}")
                                self._safe_add_closing_process_message(
                                    f"Backpack {symbol} 平仓失败: {bp_order_result}")
                                bp_success = False

                except (ValueError, TypeError) as e:
                    self.logger.error(f"{symbol} Backpack持仓数量解析错误: {e}")
                    self._safe_add_closing_process_message(
                        f"{symbol} Backpack平仓失败: 持仓数量解析错误")
                    bp_success = False

            except Exception as e:
                self.logger.error(f"{symbol} Backpack平仓过程中出错: {e}")
                # 记录详细的异常堆栈信息
                import traceback
                self.logger.error(
                    f"{symbol} Backpack平仓详细错误: {traceback.format_exc()}")
                self._safe_add_closing_process_message(
                    f"{symbol} Backpack平仓失败: {str(e)}")
                bp_success = False

        # 尝试平仓Hyperliquid - 无论Backpack平仓成功与否
        if hl_position:
            try:
                # 检查持仓数量
                try:
                    hl_quantity = float(hl_position.get(
                        "quantity", hl_position.get("size", 0)))
                    self.logger.info(
                        f"{symbol}平仓 - Hyperliquid数量: {hl_quantity}")

                    if hl_quantity == 0:
                        self.logger.warning(f"{symbol} Hyperliquid持仓数量为0，无需平仓")
                        self._safe_add_closing_process_message(
                            f"{symbol} Hyperliquid持仓数量为0，无需平仓")
                        hl_success = True  # 设置为True，因为无需平仓
                    else:
                        # 确定平仓方向
                        hl_side = "SELL" if hl_position.get(
                            "side") == "BUY" else "BUY"
                        self.logger.info(
                            f"{symbol}平仓 - Hyperliquid方向: {hl_side}")
                        
                        # 分批平仓
                        remaining_quantity = hl_quantity
                        hl_success = True  # 假设平仓成功，遇到错误时会设为False
                        
                        if single_trade_size and single_trade_size < hl_quantity:
                            self.logger.info(f"{symbol} Hyperliquid采用分批平仓策略，单次交易量: {single_trade_size}")
                            
                            batch_count = math.ceil(hl_quantity / single_trade_size)
                            self.logger.info(f"{symbol} Hyperliquid总计需要{batch_count}批次平仓")
                            
                            for batch in range(1, batch_count + 1):
                                # 计算当前批次的交易量
                                current_batch_size = min(single_trade_size, remaining_quantity)
                                current_batch_size = round(current_batch_size, 8)  # 避免浮点数精度问题
                                
                                if current_batch_size <= 0:
                                    break
                                
                                self.logger.info(
                                    f"正在Hyperliquid下单平仓(批次{batch}/{batch_count}): {symbol} {hl_side} {current_batch_size}")
                                self.logger.debug(
                                    f"Hyperliquid平仓请求参数: symbol={symbol}, side={hl_side}, size={current_batch_size}, price=None, order_type=MARKET")
                                
                                hl_order_result = await self.hyperliquid_api.place_order(
                                    symbol=symbol,
                                    side=hl_side,
                                    size=current_batch_size,
                                    price=None,
                                    order_type="MARKET"
                                )
                                
                                # 检查结果
                                if hl_order_result and hl_order_result.get("success", False):
                                    self.logger.info(
                                        f"Hyperliquid {symbol} 平仓批次{batch}/{batch_count}成功: {hl_order_result}")
                                    self._safe_add_closing_process_message(
                                        f"Hyperliquid {symbol} 平仓批次{batch}/{batch_count}成功")
                                    
                                    # 更新剩余数量
                                    remaining_quantity -= current_batch_size
                                    
                                    # 添加短暂延迟，避免频繁请求
                                    await asyncio.sleep(1)
                                else:
                                    self.logger.error(
                                        f"Hyperliquid {symbol} 平仓批次{batch}/{batch_count}失败: {hl_order_result}")
                                    self._safe_add_closing_process_message(
                                        f"Hyperliquid {symbol} 平仓批次{batch}/{batch_count}失败")
                                    hl_success = False
                                    break
                        else:
                            # 如果单次交易量未配置或大于持仓量，直接一次性平仓
                            self.logger.info(
                                f"正在Hyperliquid一次性下单平仓: {symbol} {hl_side} {hl_quantity}")
                            self.logger.debug(
                                f"Hyperliquid平仓请求参数: symbol={symbol}, side={hl_side}, size={hl_quantity}, price=None, order_type=MARKET")
                            
                            hl_order_result = await self.hyperliquid_api.place_order(
                                symbol=symbol,
                                side=hl_side,
                                size=hl_quantity,
                                price=None,
                                order_type="MARKET"
                            )

                            # 检查结果
                            if hl_order_result and hl_order_result.get("success", False):
                                self.logger.info(
                                    f"Hyperliquid {symbol} 平仓成功: {hl_order_result}")
                                self._safe_add_closing_process_message(
                                    f"Hyperliquid {symbol} 平仓成功")
                            elif "error" in hl_order_result:
                                self.logger.error(
                                    f"Hyperliquid {symbol} 平仓失败: {hl_order_result}")
                                self._safe_add_closing_process_message(
                                    f"Hyperliquid {symbol} 平仓失败: {hl_order_result}")
                                hl_success = False
                            else:
                                # 可能的成功情况，尝试从原始响应中判断
                                self.logger.warning(
                                    f"Hyperliquid {symbol} 平仓返回未明确状态: {hl_order_result}")
                                if "raw_response" in hl_order_result and "statuses" in str(hl_order_result["raw_response"]):
                                    self.logger.info(
                                        f"Hyperliquid {symbol} 平仓可能成功，检测到状态信息")
                                    self._safe_add_closing_process_message(
                                        f"Hyperliquid {symbol} 平仓可能成功")
                                else:
                                    hl_success = False

                except (ValueError, TypeError) as e:
                    self.logger.error(f"{symbol} Hyperliquid持仓数量解析错误: {e}")
                    self._safe_add_closing_process_message(
                        f"{symbol} Hyperliquid平仓失败: 持仓数量解析错误")
                    hl_success = False

            except Exception as e:
                self.logger.error(f"{symbol} Hyperliquid平仓过程中出错: {e}")
                # 记录详细的异常堆栈信息
                import traceback
                self.logger.error(
                    f"{symbol} Hyperliquid平仓详细错误: {traceback.format_exc()}")
                self._safe_add_closing_process_message(
                    f"{symbol} Hyperliquid平仓失败: {str(e)}")
                hl_success = False

        # 验证平仓结果
        try:
            self.logger.info(f"开始验证 {symbol} 平仓结果...")
            bp_positions = await self.backpack_api.get_positions()
            hl_positions = await self.hyperliquid_api.get_positions()

            # 记录所有持仓信息以便调试
            self.logger.debug(
                f"平仓后 Backpack 持仓: {json.dumps(bp_positions, default=str)}")
            self.logger.debug(
                f"平仓后 Hyperliquid 持仓: {json.dumps(hl_positions, default=str)}")

            # 检查Backpack是否已平仓
            bp_position_closed = True
            if bp_position:  # 只有在有Backpack持仓的情况下才检查
                bp_symbol = get_backpack_symbol(symbol)
                if bp_symbol in bp_positions and float(bp_positions[bp_symbol].get("quantity", bp_positions[bp_symbol].get("size", 0))) > 0:
                    bp_position_closed = False
                    self.logger.warning(
                        f"Backpack {symbol} ({bp_symbol}) 平仓后仍有持仓: {bp_positions[bp_symbol]}")
                    self._safe_add_closing_process_message(
                        f"Backpack {symbol} 平仓后仍有持仓，数量: {bp_positions[bp_symbol].get('quantity', bp_positions[bp_symbol].get('size', 0))}")
                else:
                    self.logger.info(f"Backpack {symbol} 平仓已确认成功")

            # 检查Hyperliquid是否已平仓
            hl_position_closed = True
            if hl_position:  # 只有在有Hyperliquid持仓的情况下才检查
                for pos_symbol, pos in hl_positions.items():
                    if pos_symbol == symbol and float(pos.get("size", 0)) > 0:
                        hl_position_closed = False
                        self.logger.warning(
                            f"Hyperliquid {symbol} 平仓后仍有持仓: {pos}")
                        self._safe_add_closing_process_message(
                            f"Hyperliquid {symbol} 平仓后仍有持仓，数量: {pos.get('size', 0)}")
                        break
                else:
                    self.logger.info(f"Hyperliquid {symbol} 平仓已确认成功")

            # 如果两个交易所都成功平仓或者各自成功平仓自己的持仓，则删除资金费率符号记录
            if (bp_position and hl_position and bp_position_closed and hl_position_closed) or \
               (bp_position and not hl_position and bp_position_closed) or \
               (not bp_position and hl_position and hl_position_closed):
                # 清理资金费率记录
                if symbol in self.position_open_times:
                    del self.position_open_times[symbol]
                if symbol in self.entry_prices:
                    del self.entry_prices[symbol]
                if symbol in self.entry_funding_rates:
                    del self.entry_funding_rates[symbol]

                self.logger.debug(f"已清理 {symbol} 的资金费率符号记录")

                # 保存快照记录
                self._save_position_snapshot(
                    symbol=symbol,
                    action="close",
                    bp_position=bp_position,
                    hl_position=hl_position,
                    bp_price=bp_price,
                    hl_price=hl_price,
                    bp_funding=bp_funding,
                    hl_funding=hl_funding_8h
                )

                # 保存更新后的资金费率符号 (修复缩进)
                self._save_funding_diff_signs() # 修复缩进
                
                # 手动保存更新后的持仓记录和资金费率记录
                self._save_positions_and_records()

                # 发送通知
                if self.alerter:
                    bp_msg = f"BP {bp_side} {bp_quantity} @ 市价 ({'成功' if bp_position_closed else '失败'})" if bp_position else "无BP持仓"
                    hl_msg = f"HL {hl_side} {hl_quantity} @ 市价 ({'成功' if hl_position_closed else '失败'})" if hl_position else "无HL持仓"

                    message = f"{symbol} 平仓结果: {bp_msg}, {hl_msg}"
                    await self.alerter.send_notification(message)

                self._safe_add_closing_process_message(f"{symbol} 平仓成功，已确认")
                self.logger.info(f"{symbol} 平仓成功")
                return True
            else:
                warning_msg = f"{symbol} 平仓部分成功: "
                if bp_position:
                    warning_msg += f"Backpack{'已' if bp_position_closed else '未'}平仓, "
                if hl_position:
                    warning_msg += f"Hyperliquid{'已' if hl_position_closed else '未'}平仓"

                self.logger.warning(warning_msg)
                self._safe_add_closing_process_message(warning_msg)
                return bp_position_closed or hl_position_closed  # 如果至少一个交易所平仓成功，返回True

        except Exception as e:
            self.logger.error(f"{symbol} 验证平仓结果时出错: {e}")
            # 记录详细的异常堆栈信息
            import traceback
            self.logger.error(f"{symbol} 验证平仓结果详细错误: {traceback.format_exc()}")
            self._safe_add_closing_process_message(
                f"{symbol} 验证平仓结果失败: {str(e)}")

            # 根据之前的下单结果返回
            return bp_success or hl_success  # 如果至少一个交易所平仓下单成功，返回True

    def _update_position_direction_info(
            self, market_data, bp_positions, hl_positions):
        """
        更新市场数据中的持仓方向信息
        
        Args:
            market_data: 市场数据字典
            bp_positions: Backpack持仓信息
            hl_positions: Hyperliquid持仓信息
            
        Returns:
            更新后的市场数据字典
        """
        self.logger.info(f"开始更新持仓方向信息，市场数据包含{len(market_data)}个交易对")
        self.logger.info(f"Backpack持仓数量: {len(bp_positions)}, Hyperliquid持仓数量: {len(hl_positions)}")
        
        # 收集API返回的所有实际持仓币种
        api_position_symbols = set()

        # 详细打印所有持仓数据，帮助调试
        self.logger.info("Backpack持仓原始数据详情:")
        for symbol, pos in bp_positions.items():
            self.logger.info(f"  {symbol}: {pos}")
            # 检查是否有实际持仓
            has_position = False
            for qty_field in ["size", "quantity", "netQuantity"]:
                if qty_field in pos:
                    try:
                        qty_value = float(pos.get(qty_field, 0))
                        if abs(qty_value) > 0:
                            has_position = True
                            break
                    except (ValueError, TypeError):
                        pass

            if has_position:
                try:
                    base_symbol = symbol.split("_")[0]
                    api_position_symbols.add(base_symbol)
                except:
                    self.logger.warning(f"无法解析Backpack符号: {symbol}")

        self.logger.info("Hyperliquid持仓原始数据详情:")
        for symbol, pos in hl_positions.items():
            self.logger.info(f"  {symbol}: {pos}")
            # 检查是否有实际持仓
            has_position = False
            for qty_field in ["size", "szi", "quantity"]:
                if qty_field in pos:
                    try:
                        qty_value = float(pos.get(qty_field, 0))
                        if abs(qty_value) > 0:
                            has_position = True
                            break
                    except (ValueError, TypeError):
                        pass

            if has_position:
                api_position_symbols.add(symbol)

        self.logger.info(f"API返回的实际持仓币种: {sorted(api_position_symbols)}")

        # 添加转换后的符号表
        bp_symbol_map = {}
        hl_symbol_map = {}

        # 预处理持仓数据，建立映射表
        for symbol in market_data:
            bp_symbol = get_backpack_symbol(symbol)
            hl_symbol = get_hyperliquid_symbol(symbol)

            bp_symbol_map[bp_symbol] = symbol
            hl_symbol_map[hl_symbol] = symbol

            self.logger.debug(
                f"符号映射: {symbol} -> BP:{bp_symbol}, HL:{hl_symbol}")

        # 打印完整的BP符号映射表以便调试
        self.logger.info(f"BP符号映射表: {bp_symbol_map}")

        # 为所有市场数据初始化持仓方向为None
        for symbol in market_data:
            market_data[symbol]["bp_position_side"] = None
            market_data[symbol]["hl_position_side"] = None

        # 处理Backpack持仓
        for bp_full_symbol, bp_pos in bp_positions.items():
            # 首先尝试直接从映射表中查找
            base_symbol = None
            if bp_full_symbol in bp_symbol_map:
                base_symbol = bp_symbol_map[bp_full_symbol]
                self.logger.info(
                    f"BP符号'{bp_full_symbol}'在映射表中找到匹配: {base_symbol}")
            else:
                # 如果映射表中没有找到，尝试从符号名称解析
                if "_USDC_PERP" in bp_full_symbol:
                    base_symbol = bp_full_symbol.split("_")[0]
                    self.logger.info(
                        f"BP符号'{bp_full_symbol}'不在映射表中，解析为基础符号: {base_symbol}")

            # 如果获取到了有效的基础符号且该符号在市场数据中存在
            if base_symbol and base_symbol in market_data:
                # 检查持仓量字段
                position_quantity = None
                for qty_field in ["size", "quantity", "netQuantity"]:
                    if qty_field in bp_pos:
                        try:
                            qty_value = float(bp_pos[qty_field])
                            if abs(qty_value) > 0:
                                position_quantity = qty_value
                                self.logger.info(
                                    f"BP持仓 {bp_full_symbol} (解析为{base_symbol}) 使用字段 {qty_field}={qty_value}")
                                break
                        except (ValueError, TypeError):
                            pass

                # 获取持仓方向
                position_side = None
                if position_quantity is not None:
                    if "side" in bp_pos:
                        position_side = bp_pos["side"]
                        self.logger.info(
                            f"BP持仓 {bp_full_symbol} 从side字段获取方向: {position_side}")
                    elif "position" in bp_pos:
                        position_side = bp_pos["position"]
                        self.logger.info(
                            f"BP持仓 {bp_full_symbol} 从position字段获取方向: {position_side}")
                    else:
                        position_side = "BUY" if position_quantity > 0 else "SELL"
                        self.logger.info(
                            f"BP持仓 {bp_full_symbol} 从数量推断方向: {position_side}")

                    # 标准化方向值 - 将LONG/SHORT转换为BUY/SELL
                    if position_side == "LONG":
                        position_side = "BUY"
                        self.logger.info(
                            f"{base_symbol}的Backpack方向从LONG标准化为BUY")
                    elif position_side == "SHORT":
                        position_side = "SELL"
                        self.logger.info(
                            f"{base_symbol}的Backpack方向从SHORT标准化为SELL")

                    self.logger.info(
                        f"找到{base_symbol}的Backpack持仓，最终方向: {position_side}")
                    market_data[base_symbol]["bp_position_side"] = position_side

        # 处理Hyperliquid持仓
        for hl_full_symbol, hl_pos in hl_positions.items():
            # 尝试获取基础符号
            if hl_full_symbol in hl_symbol_map:
                symbol = hl_symbol_map[hl_full_symbol]

                # 检查持仓量字段
                position_quantity = None
                for qty_field in ["size", "szi", "quantity"]:
                    if qty_field in hl_pos:
                        try:
                            qty_value = float(hl_pos[qty_field])
                            if abs(qty_value) > 0:
                                position_quantity = qty_value
                                self.logger.info(
                                    f"HL持仓 {hl_full_symbol} 使用字段 {qty_field}={qty_value}")
                                break
                        except (ValueError, TypeError):
                            pass

                # 获取持仓方向
                position_side = None
                if position_quantity is not None:
                    # 先检查是否有显式的side字段
                    if "side" in hl_pos:
                        position_side = hl_pos["side"]
                    else:
                        # 根据数量推断方向
                        position_side = "BUY" if position_quantity > 0 else "SELL"

                    self.logger.info(
                        f"找到{symbol}的Hyperliquid持仓，方向: {position_side}")
                    market_data[symbol]["hl_position_side"] = position_side
            elif hl_full_symbol in market_data:
                # 币种名直接匹配市场数据
                symbol = hl_full_symbol

                # 检查持仓量
                position_quantity = None
                for qty_field in ["size", "szi", "quantity"]:
                    if qty_field in hl_pos:
                        try:
                            qty_value = float(hl_pos[qty_field])
                            if abs(qty_value) > 0:
                                position_quantity = qty_value
                                self.logger.info(
                                    f"HL持仓 {hl_full_symbol} (直接匹配) 使用字段 {qty_field}={qty_value}")
                                break
                        except (ValueError, TypeError):
                            pass

                # 获取持仓方向
                position_side = None
                if position_quantity is not None:
                    if "side" in hl_pos:
                        position_side = hl_pos["side"]
                    else:
                        position_side = "BUY" if position_quantity > 0 else "SELL"

                    self.logger.info(
                        f"找到{symbol}的Hyperliquid持仓(直接匹配)，方向: {position_side}")
                    market_data[symbol]["hl_position_side"] = position_side

        # 总结所有交易对的持仓方向并添加开仓时间信息
        for symbol in market_data:
            bp_position_side = market_data[symbol]["bp_position_side"]
            hl_position_side = market_data[symbol]["hl_position_side"]

            # 总结该交易对的持仓方向
            self.logger.info(
                f"{symbol}的持仓方向: BP={bp_position_side or '-'}, HL={hl_position_side or '-'}")

            # 添加开仓时间信息
            if symbol in self.position_open_times:
                market_data[symbol]["open_time"] = self.position_open_times[symbol]
                self.logger.info(
                    f"{symbol}的开仓时间: {self.position_open_times[symbol]}")
            else:
                market_data[symbol]["open_time"] = None
                self.logger.info(f"{symbol}无开仓时间记录")

            # 如果检测到持仓但没有开仓时间记录，可能是手动开仓或程序重启，添加当前时间
            if (bp_position_side or hl_position_side) and symbol not in self.position_open_times:
                self.position_open_times[symbol] = time.time()
                market_data[symbol]["open_time"] = self.position_open_times[symbol]
                self.logger.info(
                    f"检测到{symbol}有持仓但无开仓时间记录，设置为当前时间: {self.position_open_times[symbol]}")

        # 清理过期持仓记录
        memory_positions = set(self.position_open_times.keys())
        expired_positions = memory_positions - api_position_symbols

        if expired_positions:
            self.logger.warning(
                f"发现内存中存在过期持仓时间记录，正在清理: {sorted(expired_positions)}")
            for expired_symbol in expired_positions:
                if expired_symbol in self.position_open_times:
                    self.logger.info(f"清理过期开仓时间记录: {expired_symbol}")
                    del self.position_open_times[expired_symbol]
                if expired_symbol in self.entry_prices:
                    del self.entry_prices[expired_symbol]
                if expired_symbol in self.entry_funding_rates:
                    del self.entry_funding_rates[expired_symbol]

            # 保存清理后的记录
            self._save_positions_and_records()

            # 不再需要更新内存中的方向记录
            # self.position_directions = {symbol: self.position_directions.get(symbol)
            #                            for symbol in api_position_symbols
            #                            if symbol in self.position_directions}

        self.logger.info("持仓方向信息更新完成")
        return market_data

    def check_direction_consistency(
            self,
            symbol,
            bp_price,
            hl_price,
            bp_funding,
            hl_funding):
        """
        检查基于价差和资金费率的开仓方向是否一致

        Args:
            symbol: 基础币种，如 "BTC"
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            bp_funding: Backpack资金费率
            hl_funding: Hyperliquid资金费率

        Returns:
            tuple: (is_consistent, funding_bp_side, funding_hl_side) 方向是否一致，以及基于资金费率的建议开仓方向
        """
        # 1. 计算基于价差的开仓方向
        price_diff = bp_price - hl_price
        if price_diff > 0:
            # BP价格高，价差套利应该BP做空，HL做多
            price_bp_side = "SELL"
            price_hl_side = "BUY"
        else:
            # HL价格高，价差套利应该BP做多，HL做空
            price_bp_side = "BUY"
            price_hl_side = "SELL"

        # 2. 计算基于资金费率的开仓方向
        if bp_funding < 0 and hl_funding < 0:
            # 两交易所资金费率都为负
            if abs(bp_funding) > abs(hl_funding):
                funding_bp_side = "BUY"    # BP绝对值大，做多
                funding_hl_side = "SELL"
            else:
                funding_bp_side = "SELL"
                funding_hl_side = "BUY"
        elif bp_funding > 0 and hl_funding > 0:
            # 两交易所资金费率都为正
            if bp_funding > hl_funding:
                funding_bp_side = "SELL"   # BP值大，做空
                funding_hl_side = "BUY"
            else:
                funding_bp_side = "BUY"
                funding_hl_side = "SELL"
        else:
            # 资金费率一正一负的情况
            # 经典资金费率套利：资金费率为正的交易所做空(支付资金费)，为负的交易所做多(收取资金费)
            funding_bp_side = "SELL" if bp_funding > 0 else "BUY"
            funding_hl_side = "SELL" if hl_funding > 0 else "BUY"

        # 3. 比较两种策略的开仓方向是否一致
        is_consistent = (
            price_bp_side == funding_bp_side and price_hl_side == funding_hl_side)

        self.logger.info(
            f"{symbol} - 方向一致性检查：价差套利方向(BP={price_bp_side}, HL={price_hl_side})，"
            f"资金费率套利方向(BP={funding_bp_side}, HL={funding_hl_side})，"
            f"{'一致' if is_consistent else '不一致'}")

        return is_consistent, funding_bp_side, funding_hl_side

    def _safe_add_condition_message(self, message: str):
        """
        安全地添加条件消息，避免未初始化的情况
        
        Args:
            message: 要添加的消息
        """
        try:
            # 确保message不是None
            if message is None:
                message = ""

            # 检查是否初始化了条件消息列表
            if not hasattr(self, 'condition_messages'):
                self.condition_messages = []

            # 避免重复添加相同的消息
            if message and message not in self.condition_messages:
                self.condition_messages.append(message)
                self.logger.info(f"添加订单消息: {message}")
            else:
                self.logger.info(f"订单消息(直接记录): {message}")
        except Exception as e:
            self.logger.error(f"添加订单消息失败: {e}")
            self.logger.error(f"异常类型: {type(e).__name__}, 消息内容: {message}")
            # 不影响主程序运行

    def _safe_add_order_message(self, message: str):
        """
        安全地添加订单消息，处理可能的异常

        Args:
            message: 要添加的消息
        """
        try:
            # 确保message不是None
            if message is None:
                message = ""

            if self.display_manager:
                try:
                    # 确保始终传递字符串参数
                    self.display_manager.add_order_message(message=message)
                except TypeError as type_error:
                    # 特别处理参数错误
                    self.logger.error(
                        f"调用add_order_message方法时参数错误: {type_error}")
                    # 尝试直接记录
                    self.logger.info(f"订单消息(TypeError后直接记录): {message}")
            else:
                self.logger.info(f"订单消息(直接记录): {message}")
        except Exception as e:
            self.logger.error(f"添加订单消息失败: {e}")
            self.logger.error(f"异常类型: {type(e).__name__}, 消息内容: {message}")
            # 不影响主程序运行

    def _safe_add_closing_process_message(self, message: str):
        """
        安全地添加平仓过程消息，处理可能的异常

        Args:
            message: 要添加的消息
        """
        try:
            # 确保message不是None
            if message is None:
                message = ""

            if self.display_manager:
                try:
                    # 确保始终传递字符串参数
                    self.display_manager.add_closing_process_message(
                        message=message)
                except TypeError as type_error:
                    # 特别处理参数错误
                    self.logger.error(
                        f"调用add_closing_process_message方法时参数错误: {type_error}")
                    # 尝试直接记录
                    self.logger.info(f"平仓过程(TypeError后直接记录): {message}")
            else:
                self.logger.info(f"平仓过程(直接记录): {message}")
        except Exception as e:
            self.logger.error(f"添加平仓过程消息失败: {e}")
            self.logger.error(f"异常类型: {type(e).__name__}, 消息内容: {message}")
            # 不影响主程序运行

    def _estimate_slippage(self, side, size, price, bids, asks):
        """
        估算给定交易大小的滑点

        Args:
            side: 交易方向 "BUY" 或 "SELL"
            size: 交易大小（以合约数量为单位）
            price: 当前中间价
            bids: 买单列表，格式为 [[price, size], ...]
            asks: 卖单列表，格式为 [[price, size], ...]

        Returns:
            估算的滑点（价格单位）
        """
        try:
            # 安全检查：确保输入参数有效
            if price is None or price <= 0:
                self.logger.warning(f"滑点计算中的价格无效: {price}")
                return 0.01 * price if price else 0.01  # 默认1%滑点

            if size is None or size <= 0:
                self.logger.warning(f"滑点计算中的交易大小无效: {size}")
                return 0.01 * price  # 默认1%滑点

            # 检查订单深度数据格式和有效性
            if not isinstance(bids, list) or not isinstance(asks, list):
                self.logger.warning(
                    f"深度数据格式无效: bids类型={type(bids)}, asks类型={type(asks)}")
                return 0.01 * price  # 默认1%滑点

            # 过滤掉无效的订单项
            valid_bids = []
            for bid in bids:
                if isinstance(bid, list) and len(bid) >= 2:
                    try:
                        bid_price = float(bid[0])
                        bid_size = float(bid[1])
                        if bid_price > 0 and bid_size > 0:
                            valid_bids.append([bid_price, bid_size])
                    except (ValueError, TypeError):
                        # 忽略无法转换为浮点数的项
                        pass

            valid_asks = []
            for ask in asks:
                if isinstance(ask, list) and len(ask) >= 2:
                    try:
                        ask_price = float(ask[0])
                        ask_size = float(ask[1])
                        if ask_price > 0 and ask_size > 0:
                            valid_asks.append([ask_price, ask_size])
                    except (ValueError, TypeError):
                        # 忽略无法转换为浮点数的项
                        pass

            if not valid_bids or not valid_asks:
                self.logger.warning(
                    f"有效深度数据不足: 有效买单={len(valid_bids)}, 有效卖单={len(valid_asks)}")
                return 0.01 * price  # 默认1%滑点

            # 按价格排序
            valid_bids.sort(key=lambda x: x[0], reverse=True)  # 买单降序
            valid_asks.sort(key=lambda x: x[0])  # 卖单升序

            # 计算实际成交价格
            if side.upper() == "BUY":
                # 买入使用卖单
                remaining_size = size
                weighted_avg_price = 0
                total_filled = 0

                for level in valid_asks:
                    level_price, level_size = level
                    if remaining_size <= 0:
                        break

                    fill_size = min(remaining_size, level_size)
                    weighted_avg_price += level_price * fill_size
                    total_filled += fill_size
                    remaining_size -= fill_size

                # 如果订单深度不足，使用最后一个价格填充剩余部分
                if remaining_size > 0 and valid_asks:
                    last_price = valid_asks[-1][0]
                    weighted_avg_price += last_price * remaining_size
                    total_filled += remaining_size

                if total_filled > 0:
                    avg_price = weighted_avg_price / total_filled
                    slippage = avg_price - price
                else:
                    # 如果无法填充，使用默认滑点
                    slippage = 0.01 * price
            else:
                # 卖出使用买单
                remaining_size = size
                weighted_avg_price = 0
                total_filled = 0

                for level in valid_bids:
                    level_price, level_size = level
                    if remaining_size <= 0:
                        break

                    fill_size = min(remaining_size, level_size)
                    weighted_avg_price += level_price * fill_size
                    total_filled += fill_size
                    remaining_size -= fill_size

                # 如果订单深度不足，使用最后一个价格填充剩余部分
                if remaining_size > 0 and valid_bids:
                    last_price = valid_bids[-1][0]
                    weighted_avg_price += last_price * remaining_size
                    total_filled += remaining_size

                if total_filled > 0:
                    avg_price = weighted_avg_price / total_filled
                    slippage = price - avg_price
                else:
                    # 如果无法填充，使用默认滑点
                    slippage = 0.01 * price

            # 确保滑点为正
            slippage = max(0, slippage)

            return slippage
        except Exception as e:
            self.logger.error(f"计算滑点时出错: {e}")
            import traceback
            self.logger.error(f"滑点计算详细错误: {traceback.format_exc()}")
            return 0.01 * price  # 异常情况下返回默认滑点

    def _display_condition_result(self, symbol: str, condition_result: bool, reason: str):
        """显示条件检查结果"""
        try:
            if condition_result:
                self.logger.info(f"{symbol} 条件满足: {reason}")
                self._safe_add_condition_message(f"{symbol} 条件满足: {reason}")
            else:
                self.logger.info(f"{symbol} 条件不满足: {reason}")
                self._safe_add_condition_message(f"{symbol} 条件不满足: {reason}")
        except Exception as e:
            self.logger.error(f"显示条件结果时出错: {e}")

    def _sync_records_with_api_positions(self, api_positions):
        """
        同步开仓时间记录和资金费率记录与API获取的实际持仓
        
        Args:
            api_positions: 从API获取的实际持仓币种集合
        """
        # 检查开仓时间记录中是否有不存在于API持仓中的记录，如有则删除
        expired_time_records = set(self.position_open_times.keys()) - api_positions
        if expired_time_records:
            self.logger.info(f"清理过期开仓时间记录: {expired_time_records}")
            for symbol in expired_time_records:
                if symbol in self.position_open_times:
                    del self.position_open_times[symbol]
        
        # 检查资金费率记录中是否有不存在于API持仓中的记录，如有则删除
        expired_funding_records = set(self.entry_funding_rates.keys()) - api_positions
        if expired_funding_records:
            self.logger.info(f"清理过期资金费率记录: {expired_funding_records}")
            for symbol in expired_funding_records:
                if symbol in self.entry_funding_rates:
                    del self.entry_funding_rates[symbol]
        
        # 检查开仓价格记录中是否有不存在于API持仓中的记录，如有则删除
        expired_price_records = set(self.entry_prices.keys()) - api_positions
        if expired_price_records:
            self.logger.info(f"清理过期开仓价格记录: {expired_price_records}")
            for symbol in expired_price_records:
                if symbol in self.entry_prices:
                    del self.entry_prices[symbol]
        
        # 如果有记录被清理，保存更新后的记录
        if expired_time_records or expired_funding_records or expired_price_records:
            self._save_positions_and_records()