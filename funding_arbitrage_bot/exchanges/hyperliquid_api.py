#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Hyperliquid交易所API封装

提供与Hyperliquid交易所交互的功能，包括获取价格、资金费率、下单、查询仓位等
使用官方SDK以及WebSocket API
"""

import json
import time
import asyncio
import math
from typing import Dict, List, Optional, Any, Tuple, Union
from decimal import Decimal
import logging
import traceback

import websockets
import httpx
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.exchange import Exchange
from eth_account import Account
from hyperliquid.utils.signing import OrderType
from hyperliquid.utils.types import Side


class HyperliquidAPI:
    """Hyperliquid交易所API封装类"""
    
    def __init__(
        self,
        api_key: str = None,
        api_secret: str = None,
        logger: Optional[logging.Logger] = None,
        config: Dict[str, Any] = None
    ):
        """
        初始化
        
        Args:
            api_key: API密钥，对应Hyperliquid的钱包地址
            api_secret: API密钥对应的私钥
            logger: 日志记录器
            config: 配置信息
        """
        self.logger = logger or logging.getLogger(__name__)
        self.hyperliquid_address = api_key
        self.hyperliquid_key = api_secret
        
        if api_key:
            self.logger.info(f"已配置Hyperliquid钱包地址: {api_key[:10]}...")
        
        # 加载配置
        self.config = config or {}
        
        # 尝试从配置中加载API密钥和私钥
        hyperliquid_config = config.get("hyperliquid", {}) if config else {}
        self.hyperliquid_address = self.hyperliquid_address or hyperliquid_config.get("api_key")
        self.hyperliquid_key = self.hyperliquid_key or hyperliquid_config.get("api_secret")
        
        # 如果仍未配置，尝试从exchanges部分获取
        if config and "exchanges" in config and "hyperliquid" in config["exchanges"]:
            hyperliquid_key = config["exchanges"]["hyperliquid"].get("api_key")
            hyperliquid_secret = config["exchanges"]["hyperliquid"].get("api_secret")
            
            self.hyperliquid_address = self.hyperliquid_address or hyperliquid_key
            self.hyperliquid_key = self.hyperliquid_key or hyperliquid_secret
            
        # 从配置中加载public_address（优先使用exchanges配置）
        if config and "exchanges" in config and "hyperliquid" in config["exchanges"]:
            self.public_address = config["exchanges"]["hyperliquid"].get("public_address")
            if self.public_address:
                self.logger.info(f"已加载Hyperliquid公共钱包地址: {self.public_address[:10]}...")
        
        # 日志记录、价格缓存和工具实例化
        self.price_cache = {}
        self.cache_expiry = {}
        self.cache_timeout = 5  # 价格缓存超时时间（秒）
        
        # 价格日志输出控制
        self.last_price_log = {}
        self.price_log_interval = 300  # 每5分钟记录一次价格
        
        # 添加最新价格缓存
        self.latest_prices = {}  # 币种 -> 最新价格
        self.latest_prices_timestamp = {}  # 币种 -> 时间戳
        
        # WebSocket相关
        self.ws = None
        self.ws_connected = False
        self.ws_task = None
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        
        # 使用配置设置连接
        self.funding_cache = {}
        self.prices = {}  # 币种 -> 价格
        self.orderbooks = {}  # 币种 -> 订单深度数据
        
        # 初始化HTTP客户端
        self.http_client = httpx.AsyncClient(timeout=10.0)
        self.base_url = "https://api.hyperliquid.xyz"
        
        # 初始化SDK客户端
        self._initialize_hyperliquid_client()
        
    def _initialize_hyperliquid_client(self):
        """初始化Hyperliquid官方SDK客户端"""
        self.hl_exchange = None
        
        if not self.hyperliquid_key or not self.hyperliquid_address:
            self.logger.warning("未配置Hyperliquid钱包地址或私钥，SDK初始化被跳过")
            return
            
        try:
            # 导入必要的库
            import eth_account
            from hyperliquid.exchange import Exchange
            
            # 创建钱包对象
            wallet = eth_account.Account.from_key(self.hyperliquid_key)
            
            # 确认钱包地址是否匹配
            if wallet.address.lower() != self.hyperliquid_address.lower():
                self.logger.warning(f"提供的钱包地址 {self.hyperliquid_address} 与私钥生成的地址 {wallet.address} 不匹配")
                return
                
            # 初始化Exchange对象
            self.hl_exchange = Exchange(wallet)
            self.logger.info("Hyperliquid SDK客户端初始化完成")
            
        except ImportError as e:
            self.logger.error(f"初始化Hyperliquid SDK失败: 缺少必要库 - {str(e)}")
        except Exception as e:
            self.logger.error(f"初始化Hyperliquid SDK失败: {str(e)}")
            
    async def start_websocket(self):
        """启动WebSocket连接以获取价格数据"""
        if self.ws_connected:
            self.logger.info("WebSocket已连接，无需重新连接")
            return
            
        try:
            self.logger.info(f"正在连接到Hyperliquid WebSocket: {self.ws_url}")
            self.ws = await websockets.connect(self.ws_url)
            self.ws_connected = True
            
            # 订阅价格数据
            subscription = {
                "method": "subscription",
                "subscription": {
                    "type": "allMids"
                }
            }
            await self.ws.send(json.dumps(subscription))
            self.logger.info("已订阅Hyperliquid价格数据")
            
            # 启动后台任务处理WebSocket消息
            self.ws_task = asyncio.create_task(self._handle_ws_messages())
            
        except Exception as e:
            self.logger.error(f"连接WebSocket失败: {str(e)}")
            self.ws_connected = False
            
    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            while self.ws_connected:
                message = await self.ws.recv()
                data = json.loads(message)
                
                # 处理价格更新
                if isinstance(data, dict) and data.get("channel") == "allMids":
                    for update in data.get("data", []):
                        if isinstance(update, list) and len(update) >= 2:
                            coin = update[0]
                            price = float(update[1])
                            self.prices[coin] = price
                            
                            # 避免过度日志记录
                            now = time.time()
                            if now - self.last_price_log.get(coin, 0) > self.price_log_interval:
                                self.logger.debug(f"价格更新: {coin} = {price}")
                                self.last_price_log[coin] = now
                                
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket连接已关闭")
            self.ws_connected = False
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            self.ws_connected = False
            
    async def close_websocket(self):
        """关闭WebSocket连接"""
        if self.ws_connected and self.ws:
            try:
                if self.ws_task:
                    self.ws_task.cancel()
                    
                await self.ws.close()
                self.ws_connected = False
                self.logger.info("WebSocket连接已关闭")
            except Exception as e:
                self.logger.error(f"关闭WebSocket连接时出错: {str(e)}")
                
    async def get_price(self, symbol):
        """获取币种的当前价格"""
        # 首先检查WebSocket价格缓存
        if symbol in self.prices:
            return self.prices[symbol]
            
        # 如果WebSocket中没有，尝试通过REST API获取
        try:
            url = f"{self.base_url}/info"
            response = await self.http_client.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                # 解析响应中的价格信息
                for meta in data[0].get("universe", []):
                    if meta.get("name") == symbol:
                        return float(meta.get("midPrice", 0))
                        
            self.logger.warning(f"无法从API获取{symbol}的价格")
            return None
            
        except Exception as e:
            self.logger.error(f"获取{symbol}价格时出错: {str(e)}")
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """
        获取指定币种的最新资金费率
        
        Args:
            symbol: 币种，如 "BTC"
            
        Returns:
            最新资金费率，如果无法获取则返回None
        """
        try:
            self.logger.info(f"尝试获取{symbol}的资金费率")
            
            # 直接使用REST API获取资金费率
            url = f"{self.base_url}/info"
            payload = {"type": "metaAndAssetCtxs"}
            
            try:
                response = await self.http_client.post(url, json=payload)
                
                if response.status_code != 200:
                    self.logger.error(f"获取资金费率HTTP错误: {response.status_code}")
                    return 0.0
                
                data = response.json()
                self.logger.debug(f"获取资金费率API响应: {type(data)}")
                
                # 解析API响应
                universe = []
                asset_ctxs = []
                
                # 检查数据是否为列表类型，且有两个元素
                if isinstance(data, list) and len(data) >= 2:
                    universe_data = data[0]
                    asset_ctxs = data[1]
                    
                    if isinstance(universe_data, dict) and "universe" in universe_data:
                        universe = universe_data["universe"]
                
                # 查找特定币种
                coin_idx = -1
                for i, coin_data in enumerate(universe):
                    if isinstance(coin_data, dict) and coin_data.get("name") == symbol:
                        coin_idx = i
                        break
                
                if coin_idx >= 0 and coin_idx < len(asset_ctxs):
                    coin_ctx = asset_ctxs[coin_idx]
                    
                    if "funding" in coin_ctx:
                        funding_rate = float(coin_ctx["funding"])
                        self.logger.info(f"获取到{symbol}的资金费率: {funding_rate}")
                        return funding_rate
                    else:
                        self.logger.warning(f"{symbol}的资产上下文中没有funding字段")
                else:
                    self.logger.warning(f"未找到{symbol}的资产上下文")
                    
                # 如果未找到资金费率，尝试查找其他可能的字段
                if coin_idx >= 0 and coin_idx < len(asset_ctxs):
                    self.logger.debug(f"{symbol}完整的资产上下文: {asset_ctxs[coin_idx]}")
                    
                    # 尝试查找fundingIndex字段
                    if "fundingIndex" in asset_ctxs[coin_idx]:
                        funding_index = float(asset_ctxs[coin_idx]["fundingIndex"])
                        self.logger.info(f"从fundingIndex字段获取到{symbol}的资金费率: {funding_index}")
                        return funding_index
                    
                return 0.0  # 如果无法找到资金费率，返回0.0
                
            except Exception as rest_error:
                self.logger.error(f"REST API获取资金费率出错: {rest_error}")
                return 0.0
                
        except Exception as e:
            self.logger.error(f"获取{symbol}资金费率出错: {e}")
            self.logger.debug(f"异常详情: {traceback.format_exc()}")
            return 0.0  # 返回0.0而不是None，表示没有资金费率

    async def get_all_funding_rates(self) -> Dict[str, float]:
        """
        获取所有币种的资金费率
        
        Returns:
            Dict[str, float]: 币种到资金费率的映射，如 {"BTC": 0.0001}
        """
        try:
            self.logger.info(f"尝试批量获取所有币种的资金费率")
            
            # 使用与单个币种相同的API端点，但一次返回所有数据
            url = f"{self.base_url}/info"
            payload = {"type": "metaAndAssetCtxs"}
            
            response = await self.http_client.post(url, json=payload)
            
            if response.status_code != 200:
                self.logger.error(f"批量获取资金费率HTTP错误: {response.status_code}")
                return {}
            
            data = response.json()
            
            # 结果集
            results = {}
            
            # 解析API响应
            if isinstance(data, list) and len(data) >= 2:
                universe_data = data[0]
                asset_ctxs = data[1]
                
                if isinstance(universe_data, dict) and "universe" in universe_data:
                    universe = universe_data["universe"]
                    
                    # 遍历所有币种
                    for i, coin_data in enumerate(universe):
                        if not isinstance(coin_data, dict) or "name" not in coin_data:
                            continue
                            
                        symbol = coin_data["name"]
                        
                        # 获取该币种的资金费率
                        if i < len(asset_ctxs):
                            coin_ctx = asset_ctxs[i]
                            
                            # 尝试从多个可能的字段获取资金费率
                            if "funding" in coin_ctx:
                                funding_rate = float(coin_ctx["funding"])
                                results[symbol] = funding_rate
                            elif "fundingIndex" in coin_ctx:
                                funding_rate = float(coin_ctx["fundingIndex"])
                                results[symbol] = funding_rate
            
            self.logger.info(f"成功批量获取 {len(results)} 个币种的资金费率")
            return results
            
        except Exception as e:
            self.logger.error(f"批量获取所有资金费率出错: {e}")
            self.logger.debug(f"异常详情: {traceback.format_exc()}")
            return {}

    async def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float] = None,
        order_type: str = "LIMIT",
        post_only: bool = False,
        reduce_only: bool = False
    ) -> Dict:
        """
        下单
        
        Args:
            symbol: 币种名称，如 "BTC"
            side: 订单方向，"BUY" 或 "SELL"
            size: 订单数量
            price: 订单价格，若为None，则使用当前市价的0.95倍(买入)或1.05倍(卖出)
            order_type: 订单类型，"LIMIT" 或 "MARKET"
            post_only: 是否仅做maker
            reduce_only: 是否仅减仓
            
        Returns:
            订单信息
        """
        if not self.hl_exchange:
            message = "未配置Hyperliquid钱包地址和私钥，无法下单"
            self.logger.error(message)
            return {"success": False, "error": message}
        
        try:
            # 检查参数
            if not symbol or not side or not size:
                message = f"下单参数不完整: symbol={symbol}, side={side}, size={size}"
                self.logger.error(message)
                return {"success": False, "error": message}
            
            # 标准化参数
            name = symbol.upper()
            side = side.upper()
            sz = abs(float(size))  # 确保数量为正数
            is_buy = side in ["BUY", "LONG"]
            
            # 检查下单方向和订单类型
            self.logger.info(f"Hyperliquid下单处理中: {name}, 方向: {'买入' if is_buy else '卖出'}, 数量: {sz}, 类型: {order_type}")
            
            # 获取市场价格
            if not price and order_type.upper() == "LIMIT":
                try:
                    current_price = await self.get_price(name)
                    if not current_price:
                        self.logger.warning(f"无法获取{name}的市场价格，尝试从行情API重新获取")
                        current_price = await self._get_market_price(name)
                    
                    if not current_price:
                        # 如果仍然无法获取价格，则获取深度数据
                        self.logger.warning(f"无法从行情API获取价格，尝试从orderbook获取")
                        orderbook = await self.get_orderbook(name)
                        if orderbook and orderbook.get("bids") and orderbook.get("asks"):
                            # 买单使用asks的最低价, 卖单使用bids的最高价
                            if is_buy:
                                current_price = float(orderbook["asks"][0][0])
                            else:
                                current_price = float(orderbook["bids"][0][0])
                    
                    # 计算下单价格: 买单降低价格，卖单提高价格
                    price_factor = 1.01 if is_buy else 0.99  # 降低买入价/提高卖出价，增加成交概率
                    
                    # 如果是市价单，price_factor的方向需要反向，以确保能够成交
                    if order_type.upper() == "MARKET":
                        price_factor = 1.01 if not is_buy else 0.99
                        
                    limit_px = current_price * price_factor
                    
                    self.logger.info(f"计算下单价格: 当前价格={current_price}, 下单价格={limit_px}, 方向={'买入' if is_buy else '卖出'}")
                except Exception as e:
                    self.logger.error(f"计算市场价格失败，将使用当前价格: {str(e)}")
                    limit_px = None
            else:
                limit_px = price

            # 处理市价单，如果是市价单，将order_type设置为MARKET
            is_market_order = order_type.upper() == "MARKET"
            if is_market_order:
                self.logger.info(f"处理市价单: {symbol} {side} {sz}")
                limit_px = None  # 市价单不需要指定价格

            # 使用异步线程池包装同步SDK调用
            def place_order_sync():
                try:
                    # 由于平仓时可能传入反向的size，需要调整
                    order_side = "B" if is_buy else "A"
                    order_size = abs(sz)
                    
                    self.logger.info(f"执行Hyperliquid订单: {name}, 方向: {order_side}, 数量: {order_size}, 价格: {limit_px if limit_px else '市价'}")
                    
                    # 根据订单类型调用不同方法
                    if is_market_order:
                        # 市价单
                        self.logger.info(f"下Hyperliquid市价单: {name}, {order_side}, {order_size}")
                        
                        # 获取当前价格以设定合适的限价
                        current_price = None
                        if name in self.prices:
                            current_price = self.prices[name]
                            self.logger.info(f"使用缓存价格: {current_price}")
                        
                        # 如果没有缓存价格，尝试获取当前价格
                        if not current_price:
                            try:
                                # 尝试从orderbook获取价格
                                temp_orderbook = self.orderbooks.get(name)
                                if temp_orderbook and temp_orderbook.get("bids") and temp_orderbook.get("asks"):
                                    if is_buy:
                                        current_price = float(temp_orderbook["asks"][0][0]) * 1.05  # 买入价高于最低卖价
                                    else:
                                        current_price = float(temp_orderbook["bids"][0][0]) * 0.95  # 卖出价低于最高买价
                                    self.logger.info(f"使用orderbook价格: {current_price}")
                            except Exception as e:
                                self.logger.error(f"获取orderbook价格失败: {e}")
                        
                        # 如果仍然无法获取价格，使用一个默认值
                        if not current_price:
                            current_price = 100.0
                            self.logger.warning(f"无法获取价格，使用默认价格: {current_price}")
                        
                        # 获取交易对配置信息
                        price_precision = 2  # 默认值
                        tick_size = 0.01     # 默认值
                        
                        # 从配置文件获取价格精度和tick_size
                        if self.config and "trading_pairs" in self.config:
                            for pair in self.config["trading_pairs"]:
                                if pair.get("symbol") == name:
                                    price_precision = pair.get("price_precision", 2)
                                    tick_size = pair.get("tick_size", 0.01)
                                    self.logger.info(f"找到{name}的交易对配置: 价格精度={price_precision}, tick_size={tick_size}")
                                    break
                        
                        # Hyperliquid不支持市价单，所以使用限价单模拟市价单效果
                        # 将价格设置为与当前价格相比更有利的价格，以确保成交
                        slippage_factor = 1.05 if is_buy else 0.95  # 买入价提高5%，卖出价降低5%
                        
                        # 计算调整后的价格
                        raw_price = current_price * slippage_factor
                        
                        # 按照tick_size调整价格
                        limit_price = round(raw_price / tick_size) * tick_size
                        
                        # 控制价格的小数位数
                        limit_price = round(limit_price, price_precision)
                        
                        self.logger.info(f"使用限价单模拟市价单: {name}, {order_side}, {order_size}, 原始价格: {raw_price}, 调整后价格: {limit_price}")
                        
                        return self.hl_exchange.order(
                            name=name,
                            is_buy=is_buy,
                            sz=order_size,
                            limit_px=limit_price,
                            order_type={"limit": {"tif": "Gtc"}}  # 使用限价单格式
                        )
                    else:
                        # 限价单
                        self.logger.info(f"下Hyperliquid限价单: {name}, {order_side}, {order_size}, {limit_px}")
                        
                        # 获取交易对配置信息
                        price_precision = 2  # 默认值
                        tick_size = 0.01     # 默认值
                        
                        # 从配置文件获取价格精度和tick_size
                        if self.config and "trading_pairs" in self.config:
                            for pair in self.config["trading_pairs"]:
                                if pair.get("symbol") == name:
                                    price_precision = pair.get("price_precision", 2)
                                    tick_size = pair.get("tick_size", 0.01)
                                    self.logger.info(f"找到{name}的限价单交易对配置: 价格精度={price_precision}, tick_size={tick_size}")
                                    break
                        
                        # 调整限价单价格，确保符合tick_size要求
                        adjusted_price = limit_px
                        if adjusted_price is not None:
                            # 按照tick_size调整价格
                            adjusted_price = round(adjusted_price / tick_size) * tick_size
                            # 控制价格的小数位数
                            adjusted_price = round(adjusted_price, price_precision)
                            
                            self.logger.info(f"调整后的限价单价格: {adjusted_price} (原价: {limit_px}, tick_size: {tick_size}, 精度: {price_precision})")
                        else:
                            self.logger.warning(f"限价单价格为None，将使用市场价格")
                            # 如果价格为None，获取市场价
                            if name in self.prices:
                                market_price = self.prices[name]
                                # 根据买卖方向调整价格，确保能够成交
                                market_factor = 1.01 if is_buy else 0.99
                                adjusted_price = market_price * market_factor
                                # 按照tick_size和precision调整
                                adjusted_price = round(adjusted_price / tick_size) * tick_size
                                adjusted_price = round(adjusted_price, price_precision)
                                self.logger.info(f"使用市场价格作为限价单价格: {adjusted_price}")
                            else:
                                self.logger.error(f"限价单价格为None且无法获取市场价格，无法下单")
                                raise ValueError("限价单价格为None且无法获取市场价格")
                        
                        return self.hl_exchange.order(
                            name=name,
                            is_buy=is_buy,
                            sz=order_size,
                            limit_px=adjusted_price,
                            order_type={"limit": {"tif": "Gtc"}}  # 使用限价单格式
                        )
                except Exception as e:
                    self.logger.error(f"下单时发生异常: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    raise e
                
            # 在异步函数中执行同步SDK调用
            loop = asyncio.get_event_loop()
            try:
                self.logger.info(f"开始执行Hyperliquid下单...")
                response = await loop.run_in_executor(None, place_order_sync)
                self.logger.info(f"Hyperliquid下单API响应: {json.dumps(response, default=str)}")
                
                # 解析响应
                success = False
                order_id = None
                status = "unknown"
                
                # 深入解析响应，尝试提取成功状态
                if isinstance(response, dict):
                    if "response" in response:
                        response_data = response["response"]
                        if isinstance(response_data, dict) and "data" in response_data:
                            data = response_data["data"]
                            if isinstance(data, dict) and "statuses" in data:
                                statuses = data["statuses"]
                                if statuses and len(statuses) > 0:
                                    status_entry = statuses[0]
                                    if "filled" in status_entry:
                                        # 订单成功执行
                                        order_id = status_entry["filled"].get("oid", "未知")
                                        status = "filled"
                                        success = True
                                        self.logger.info(f"Hyperliquid订单成功执行: id={order_id}")
                                    elif "resting" in status_entry:
                                        # 订单已挂出
                                        order_id = status_entry["resting"].get("oid", "未知")
                                        status = "resting"
                                        success = True
                                        self.logger.info(f"Hyperliquid订单已挂出: id={order_id}")
                                    else:
                                        # 检查是否有其他可能表示成功的状态
                                        status_keys = list(status_entry.keys())
                                        self.logger.warning(f"Hyperliquid订单状态未知，状态键: {status_keys}")
                                        
                                        if any(key in ["filled", "resting", "oid", "type", "status"] for key in status_keys):
                                            success = True
                                            status = "possible_success"
                                            self.logger.info(f"Hyperliquid订单可能成功: {status_entry}")
                
                # 如果前面的解析没有确定成功状态，尝试进一步判断
                if not success:
                    if "error" in str(response).lower():
                        error_message = str(response)
                        self.logger.error(f"Hyperliquid下单错误: {error_message}")
                    return {
                        "success": False,
                        "error": error_message,
                        "symbol": symbol,
                        "side": side,
                        "size": size,
                        "price": price,
                        "type": order_type,
                        "raw_response": response
                    }
                else:
                    # 尝试再次解析，检查是否有提示成功的关键字
                    response_str = json.dumps(response, default=str)
                    if any(key in response_str.lower() for key in ["success", "filled", "resting", "oid", "orderid"]):
                        success = True
                        status = "possible_success_by_keywords"
                        self.logger.info(f"Hyperliquid订单通过关键字判断可能成功: {response_str}")
                
                # 构建返回结果
                return {
                    "success": success,
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "price": price,
                    "type": order_type,
                    "status": status,
                        "raw_response": response
                    }
                
            except Exception as e:
                self.logger.error(f"调用Hyperliquid下单API失败: {str(e)}")
                self.logger.error(traceback.format_exc())
                return {
                    "success": False,
                    "error": f"调用Hyperliquid下单API失败: {str(e)}",
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "price": price,
                    "type": order_type
                }
                
        except Exception as e:
            self.logger.error(f"下单失败: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {
                "success": False, 
                "error": f"下单失败: {str(e)}",
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
                "type": order_type
            }

    async def get_order_status(self, order_id: str) -> Dict:
        """
        获取订单状态
        
        Args:
            order_id: 订单ID
            
        Returns:
            订单状态信息
        """
        if not self.hl_exchange:
            raise ValueError("未配置Hyperliquid钱包地址和私钥，无法查询订单")
        
        try:
            # 使用异步线程池包装同步SDK调用
            def get_order_status_sync():
                # 查询订单状态需要使用用户的所有订单并过滤
                orders = self.hl_exchange.order_status()
                for order in orders:
                    if order["oid"] == order_id:
                        return order
                return None
                
            # 在异步函数中执行同步SDK调用
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, get_order_status_sync)
            
            return result or {"error": "订单不存在"}
        except Exception as e:
            self.logger.error(f"获取Hyperliquid订单状态失败: {e}")
            raise

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """
        获取当前持仓
        
        Args:
            symbol: 币种，如 "BTC"
            
        Returns:
            持仓信息，如果无持仓则返回None
        """
        if not hasattr(self, "hl_exchange") or self.hl_exchange is None:
            if not self.hyperliquid_address or not self.hyperliquid_key:
                raise ValueError("未配置Hyperliquid钱包地址和私钥，无法查询仓位")
                
            try:
                # 初始化官方SDK
                from eth_account import Account
                from hyperliquid.exchange import Exchange
                
                # 创建钱包对象
                wallet = Account.from_key(self.hyperliquid_key)
                
                # 初始化Exchange
                self.hl_exchange = Exchange(wallet=wallet)
                self.logger.info("已初始化Hyperliquid官方SDK")
            except Exception as e:
                self.logger.error(f"初始化Hyperliquid官方SDK失败: {str(e)}")
                raise ValueError(f"初始化Hyperliquid官方SDK失败: {str(e)}")
        
        try:
            # 获取用户所有仓位
            positions = await self.get_positions()
            
            # 查找指定币种的持仓
            if symbol in positions:
                position_data = positions[symbol]
                
                # 获取最新价格
                current_price = await self.get_price(symbol)
                
                # 构建完整持仓信息
                return {
                    "symbol": symbol,
                    "side": position_data["side"],
                    "size": position_data["size"],
                    "entry_price": position_data.get("entry_price", 0),
                    "mark_price": current_price if current_price else 0.0,
                    "unrealized_pnl": position_data.get("unrealized_pnl", 0)
                }
            
            return None
        except Exception as e:
            self.logger.error(f"获取Hyperliquid {symbol}持仓信息失败: {str(e)}")
            return None

    async def close_position(self, symbol: str, size: Optional[float] = None) -> Dict:
        """
        平仓
        
        Args:
            symbol: 币种，如 "BTC"
            size: 平仓数量，如果为None则全部平仓
            
        Returns:
            订单信息
        """
        if not hasattr(self, "hl_exchange") or self.hl_exchange is None:
            raise ValueError("未配置Hyperliquid官方SDK，无法平仓")
        
        # 获取当前持仓
        position = await self.get_position(symbol)
        if not position:
            self.logger.warning(f"没有{symbol}的持仓，无法平仓")
            return {"error": "没有持仓"}
        
        # 确定平仓方向（与持仓方向相反）
        close_side = "SELL" if position["side"] == "BUY" else "BUY"
        
        # 确定平仓数量
        close_size = size if size is not None else position["size"]
        
        self.logger.info(f"正在Hyperliquid平仓: {symbol} {close_side} {close_size}")
        
        # 执行平仓订单
        return await self.place_order(symbol, close_side, close_size)

    async def start_ws_price_stream(self):
        """
        启动WebSocket价格数据流
        """
        if self.ws_task:
            return
            
        self.ws_task = asyncio.create_task(self._ws_price_listener())
        
    async def _ws_price_listener(self):
        """
        WebSocket价格数据监听器
        优化日志输出和批量处理
        """
        reconnect_delay = 5  # 初始重连延迟，秒
        max_reconnect_delay = 60  # 最大重连延迟，秒
        
        while True:
            try:
                self.logger.debug(f"尝试连接Hyperliquid WebSocket: {self.ws_url}")
                
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.ws_connected = True
                    self.logger.info("Hyperliquid WebSocket已连接")
                    
                    # 重置重连延迟
                    reconnect_delay = 5
                    
                    # 获取币种列表
                    coins = []
                    if hasattr(self, "price_coins") and self.price_coins:
                        coins = self.price_coins
                    else:
                        # 默认监控的币种列表
                        coins = ["BTC", "ETH", "SOL", "AVAX", "DOGE", "XRP", "ADA", "LINK", "BNB"]
                    
                    # 为每个币种创建订阅
                    subscription_count = 0
                    for coin in coins:
                        try:
                            subscribe_msg = {
                                "method": "subscribe",
                                "subscription": {
                                    "type": "l2Book",
                                    "coin": coin
                                }
                            }
                            
                            # 发送订阅请求
                            await ws.send(json.dumps(subscribe_msg))
                            subscription_count += 1
                        except Exception as e:
                            self.logger.error(f"发送{coin}订阅请求失败: {e}")
                    
                    self.logger.debug(f"已向Hyperliquid发送{subscription_count}个币种订阅请求")
                    
                    # 用于批量价格更新的临时存储
                    batch_price_updates = {}
                    last_batch_time = time.time()
                    
                    # 接收和处理消息
                    while True:
                        try:
                            message = await ws.recv()
                            data_json = json.loads(message)
                            
                            # 处理价格数据
                            if data_json.get("channel") == "l2Book":
                                book_data = data_json.get("data", {})
                                coin = book_data.get("coin")
                                
                                if coin and "levels" in book_data:
                                    levels = book_data["levels"]
                                    if levels and len(levels) >= 2 and len(levels[0]) > 0 and len(levels[1]) > 0:
                                        # 获取最佳买价和卖价
                                        bid = float(levels[0][0]["px"])
                                        ask = float(levels[1][0]["px"])
                                        
                                        # 计算中间价作为当前价格
                                        price = (bid + ask) / 2
                                        
                                        # 收集价格变化
                                        old_price = self.prices.get(coin)
                                        if old_price is not None:
                                            # 计算价格变化百分比
                                            pct_change = abs((price - old_price) / old_price)
                                            if pct_change > 0.005:  # 超过0.5%的变化
                                                batch_price_updates[coin] = (old_price, price, pct_change)
                                        
                                        # 更新当前价格
                                        self.prices[coin] = price
                                        
                                        # 更新订单深度数据
                                        self.orderbooks[coin] = {
                                            "timestamp": time.time(),
                                            "bids": levels[0],
                                            "asks": levels[1]
                                        }
                                        
                                        # 检查是否应该生成批量价格更新日志
                                        now = time.time()
                                        if (batch_price_updates and now - last_batch_time > 300) or len(batch_price_updates) >= 5:
                                            # 按变化幅度排序并取前5个显著变化
                                            if batch_price_updates:
                                                top_updates = sorted(
                                                    batch_price_updates.items(),
                                                    key=lambda x: x[1][2],
                                                    reverse=True
                                                )[:5]
                                                
                                                updates_text = []
                                                for coin, (old, new, pct) in top_updates:
                                                    updates_text.append(f"{coin}: {old:.2f}→{new:.2f} ({pct*100:.2f}%)")
                                                    
                                                # 记录批量价格更新
                                                self.logger.debug(f"Hyperliquid价格变化: {', '.join(updates_text)}")
                                            
                                            # 重置批量收集
                                            batch_price_updates = {}
                                            last_batch_time = now
                                        
                        except websockets.exceptions.ConnectionClosed:
                            self.logger.debug("Hyperliquid WebSocket连接已关闭，将重新连接")
                            break
                        except json.JSONDecodeError:
                            # 忽略无效JSON数据，不记录日志
                            pass
                        except Exception as e:
                            self.logger.error(f"处理WebSocket消息时出错: {e}")
                            continue
            except websockets.exceptions.ConnectionClosed:
                self.ws_connected = False
                self.logger.debug(f"Hyperliquid WebSocket连接关闭，{reconnect_delay}秒后重连")
                
            except Exception as e:
                self.ws_connected = False
                self.logger.error(f"Hyperliquid WebSocket错误: {e}")
            
            # 重连等待
            await asyncio.sleep(reconnect_delay)
            
            # 指数增长重连延迟，但不超过最大值
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    def set_price_coins(self, coins: List[str]):
        """
        设置需要通过WebSocket获取价格的币种列表
        
        Args:
            coins: 币种列表，如 ["BTC", "ETH", "SOL"]
        """
        self.price_coins = coins 

    async def get_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        获取当前持仓信息
        
        Returns:
            持仓信息字典，格式为 {"BTC": {"size": 0.001, "side": "BUY"}, ...}
        """
        try:
            # 确定要使用的钱包地址 - 优先使用公共钱包地址
            query_address = self.public_address if self.public_address else self.hyperliquid_address
            
            if not query_address:
                self.logger.error("未配置Hyperliquid钱包地址，无法获取持仓")
                return {}
                
            self.logger.info(f"使用钱包地址查询Hyperliquid持仓: {query_address[:10]}...")
            
            # 使用REST API获取持仓
            url = f"{self.base_url}/info"
            payload = {
                "type": "clearinghouseState",
                "user": query_address
            }
            
            # 添加更多的日志记录，便于调试
            self.logger.debug(f"Hyperliquid REST API请求URL: {url}")
            self.logger.debug(f"Hyperliquid REST API请求载荷: {payload}")
            
            try:
                response = await self.http_client.post(url, json=payload)
                self.logger.debug(f"Hyperliquid REST API响应状态码: {response.status_code}")
                
                if response.status_code != 200:
                    self.logger.error(f"获取用户状态HTTP错误: {response.status_code}, 响应内容: {response.text[:200]}")
                    return {}
                    
                user_data = response.json()
                self.logger.debug(f"Hyperliquid REST API响应: {user_data}")
                
                # 解析持仓数据
                positions = {}
                if "assetPositions" in user_data:
                    asset_positions = user_data["assetPositions"]
                    self.logger.debug(f"REST API持仓数据: {asset_positions}")
                    
                    for pos_item in asset_positions:
                        self.logger.debug(f"处理持仓项: {pos_item}")
                        
                        # 检查持仓项的格式
                        if "position" not in pos_item:
                            self.logger.debug(f"跳过无效持仓项: {pos_item}")
                            continue
                            
                        pos = pos_item["position"]
                        coin = pos.get("coin")
                        size = pos.get("szi")
                        
                        if not coin or size is None:
                            self.logger.debug(f"跳过无效持仓项: {pos}")
                            continue
                            
                        try:
                            size_value = float(size)
                            if size_value == 0:
                                self.logger.debug(f"跳过零持仓: {coin}")
                                continue
                            
                            side = "BUY" if size_value > 0 else "SELL"
                            abs_size = abs(size_value)
                            
                            positions[coin] = {
                                "side": side,
                                "size": abs_size,
                                "entry_price": float(pos.get("entryPx", 0)),
                                "unrealized_pnl": float(pos.get("unrealizedPnl", 0))
                            }
                            
                            self.logger.info(f"Hyperliquid持仓: {coin}, 方向: {side}, 数量: {abs_size}")
                        except (ValueError, TypeError) as e:
                            self.logger.error(f"解析{coin}持仓数据时出错: {str(e)}")
                
                self.logger.info(f"通过REST API获取到{len(positions)}个Hyperliquid持仓")
                return positions
                
            except Exception as rest_error:
                self.logger.error(f"REST API获取持仓信息时出错: {str(rest_error)}")
                self.logger.exception(rest_error)
                return {}
                
        except Exception as e:
            self.logger.error(f"获取持仓信息时出错: {str(e)}")
            self.logger.exception(e)
            return {}

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        """
        取消订单
        
        Args:
            symbol: 币种，如 "BTC"
            order_id: 订单ID
            
        Returns:
            取消订单的响应
        """
        try:
            if not self.hl_exchange:
                self.logger.error("未配置Hyperliquid钱包地址和私钥，无法取消订单")
                return {"success": False, "error": "未配置Hyperliquid API"}
                
            self.logger.info(f"正在取消Hyperliquid订单: {symbol} {order_id}")
            
            # Hyperliquid的SDK可能返回boolean或其他类型，确保不直接await这个结果
            def cancel_order_sync():
                try:
                    # 执行SDK的取消订单操作
                    # 注意：Hyperliquid SDK的cancel_order方法可能不存在或不同名
                    # 使用正确的SDK方法来取消订单
                    result = self.hl_exchange.cancel_orders(name=name, oid=int(order_id))
                    self.logger.debug(f"Hyperliquid取消订单结果: {result}")
                    return {"success": True, "data": result}
                except Exception as e:
                    self.logger.error(f"Hyperliquid取消订单SDK错误: {e}")
                    return {"success": False, "error": str(e)}
            
            # 使用线程池执行同步操作
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, cancel_order_sync)
            
            if result.get("success"):
                self.logger.info(f"成功取消Hyperliquid订单: {order_id}")
            else:
                self.logger.error(f"取消Hyperliquid订单失败: {result.get('error')}")
                
            return result
            
        except Exception as e:
            self.logger.error(f"取消Hyperliquid订单时出错: {e}")
            return {"success": False, "error": str(e)}

    async def close(self):
        """关闭所有连接"""
        # 关闭WebSocket
        await self.close_websocket()
        
        # 关闭HTTP客户端
        if self.http_client:
            await self.http_client.aclose()
            
        self.logger.info("已关闭所有Hyperliquid连接") 

    async def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        获取指定币种的订单深度数据
        
        Args:
            symbol: 币种，如 "BTC"
            
        Returns:
            订单深度数据，格式为 {"bids": [...], "asks": [...], "timestamp": ...}
            如果无法获取则返回None
        """
        # 先检查WebSocket中是否已有数据 (WebSocket数据比REST数据更实时)
        orderbook = self.orderbooks.get(symbol)
        if orderbook and time.time() - orderbook.get("timestamp", 0) < 10:  # 10秒内的数据视为有效
            self.logger.debug(f"使用WebSocket缓存的{symbol}订单簿数据，时间戳: {orderbook['timestamp']}")
            
            # 确保数据格式统一，转换为标准格式 [[price, size], ...]
            standardized_orderbook = {
                "timestamp": orderbook["timestamp"],
                "bids": [],
                "asks": []
            }
            
            # 转换买单格式
            if "bids" in orderbook and orderbook["bids"]:
                for item in orderbook["bids"]:
                    if isinstance(item, dict) and "px" in item and "sz" in item:
                        standardized_orderbook["bids"].append([float(item["px"]), float(item["sz"])])
            
            # 转换卖单格式
            if "asks" in orderbook and orderbook["asks"]:
                for item in orderbook["asks"]:
                    if isinstance(item, dict) and "px" in item and "sz" in item:
                        standardized_orderbook["asks"].append([float(item["px"]), float(item["sz"])])
            
            # 检查转换后的数据有效性
            if standardized_orderbook["bids"] and standardized_orderbook["asks"]:
                self.logger.debug(f"WebSocket {symbol}订单簿有效: {len(standardized_orderbook['bids'])}买单, {len(standardized_orderbook['asks'])}卖单")
                return standardized_orderbook
            else:
                self.logger.warning(f"WebSocket {symbol}订单簿数据无效，将尝试REST API")
        else:
            # 如果没有WebSocket数据或数据已过期
            if not orderbook:
                self.logger.debug(f"未找到{symbol}的WebSocket订单簿数据，将尝试REST API")
            else:
                self.logger.debug(f"{symbol}的WebSocket订单簿数据已过期 ({time.time() - orderbook.get('timestamp', 0):.1f}秒前)，将尝试REST API")
            
        # 如果WebSocket中没有数据或数据无效，通过REST API获取
        try:
            # 添加调试日志
            self.logger.debug(f"通过REST API获取Hyperliquid订单簿: {symbol}")
            
            url = f"{self.base_url}/info"
            payload = {
                "type": "l2Book",
                "coin": symbol
            }
            
            self.logger.debug(f"请求URL: {url}, 数据: {payload}")
            
            # 发送请求
            response = await self.http_client.post(url, json=payload)
            
            # 检查响应状态
            if response.status_code != 200:
                self.logger.error(f"获取订单深度HTTP错误: {response.status_code}, {response.text}")
                return None
                
            # 解析响应数据
            data = response.json()
            
            # 添加调试信息
            self.logger.debug(f"Hyperliquid订单簿原始数据: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            
            if isinstance(data, dict) and "levels" in data:
                levels = data["levels"]
                
                # 记录原始数据样本
                if len(levels) > 0 and levels[0]:
                    self.logger.debug(f"Hyperliquid订单簿原始bid样本: {levels[0][0] if levels[0] else None}")
                if len(levels) > 1 and levels[1]:
                    self.logger.debug(f"Hyperliquid订单簿原始ask样本: {levels[1][0] if levels[1] else None}")
                
                # 转换Hyperliquid格式: [{"px": price, "sz": size}, ...] 为统一格式: [price, size]
                standardized_orderbook = {
                    "timestamp": time.time(),
                    "bids": [],
                    "asks": []
                }
                
                # 处理买单
                if len(levels) > 0:
                    for item in levels[0]:
                        if isinstance(item, dict) and "px" in item and "sz" in item:
                            standardized_orderbook["bids"].append([float(item["px"]), float(item["sz"])])
                
                # 处理卖单
                if len(levels) > 1:
                    for item in levels[1]:
                        if isinstance(item, dict) and "px" in item and "sz" in item:
                            standardized_orderbook["asks"].append([float(item["px"]), float(item["sz"])])
                
                # 记录转换后的数据样本
                if standardized_orderbook["bids"]:
                    self.logger.debug(f"Hyperliquid订单簿转换后bid样本: {standardized_orderbook['bids'][0]}")
                if standardized_orderbook["asks"]:
                    self.logger.debug(f"Hyperliquid订单簿转换后ask样本: {standardized_orderbook['asks'][0]}")
                
                # 检查数据有效性
                if not standardized_orderbook["bids"] or not standardized_orderbook["asks"]:
                    self.logger.error(f"{symbol}订单簿数据无效: 买单: {len(standardized_orderbook['bids'])}, 卖单: {len(standardized_orderbook['asks'])}")
                    return None
                
                # 同时更新WebSocket缓存，以便下次使用更新的数据
                self.orderbooks[symbol] = {
                    "timestamp": standardized_orderbook["timestamp"],
                    "bids": [{"px": str(bid[0]), "sz": str(bid[1])} for bid in standardized_orderbook["bids"]],
                    "asks": [{"px": str(ask[0]), "sz": str(ask[1])} for ask in standardized_orderbook["asks"]]
                }
                
                return standardized_orderbook
            else:
                self.logger.error(f"订单深度数据格式异常: {data}")
                return None
                
        except Exception as e:
            self.logger.error(f"获取订单深度出错: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None 