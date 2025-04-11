#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backpack交易所API封装

提供与Backpack交易所交互的功能，包括获取价格、资金费率、下单、查询仓位等
"""

import time
import json
import hmac
import hashlib
import base64
import asyncio
import uuid
from typing import Dict, List, Optional, Any, Tuple, Union
from decimal import Decimal
import math
import traceback

import httpx
import websockets
import logging
import nacl.signing  # 添加PyNaCl库依赖
import urllib.parse


class BackpackAPI:
    """Backpack交易所API封装类"""
    
    def __init__(
        self, 
        api_key: str, 
        api_secret: str, 
        base_url: str = "https://api.backpack.exchange",
        ws_url: str = "wss://ws.backpack.exchange",
        logger: Optional[logging.Logger] = None,
        config: Optional[Dict] = None
    ):
        """
        初始化Backpack API
        
        Args:
            api_key: API密钥
            api_secret: API密钥对应的密钥
            base_url: REST API基础URL
            ws_url: WebSocket API URL
            logger: 日志记录器，如果为None则使用默认日志记录器
            config: 配置信息，包含交易对精度等设置
        """
        self.logger = logger or logging.getLogger(__name__)
        self.api_key = api_key.strip()
        
        # 保存原始的API密钥
        self.api_secret = api_secret.strip()
        
        # 尝试解码
        try:
            self.api_secret_bytes = base64.b64decode(self.api_secret)
            self.logger.info("成功解码API密钥")
            
            # 如果密钥长度大于32字节，仅取前32字节
            if len(self.api_secret_bytes) > 32:
                self.api_secret_bytes = self.api_secret_bytes[:32]
                self.logger.info(f"API密钥长度为{len(self.api_secret_bytes)}字节，截取前32字节")
        except Exception as e:
            self.logger.warning(f"无法解码API密钥，可能导致签名问题: {e}")
            self.api_secret_bytes = self.api_secret.encode()
            
        self.base_url = base_url
        self.ws_url = ws_url
        
        # 保存配置信息
        self.config = config or {}
        
        # 价格和资金费率缓存
        self.prices = {}
        self.funding_rates = {}
        
        # HTTP客户端
        self.http_client = httpx.AsyncClient(timeout=10.0)
        
        # WebSocket连接
        self.ws = None
        self.ws_connected = False
        self.ws_task = None
        
        # 默认请求超时时间
        self.default_window = 5000  # 5秒
        
    async def close(self):
        """
        关闭所有连接
        """
        try:
            if self.http_client:
                await self.http_client.aclose()
                self.logger.debug("已关闭Backpack HTTP客户端")
            
            if self.ws_task and not self.ws_task.done():
                self.ws_task.cancel()
                try:
                    await self.ws_task
                except asyncio.CancelledError:
                    pass
                self.logger.debug("已取消Backpack WebSocket任务")
            
            if self.ws:
                await self.ws.close()
                self.ws = None
                self.logger.debug("已关闭Backpack WebSocket连接")
            
            self.logger.info("已关闭所有Backpack连接")
        except Exception as e:
            self.logger.error(f"关闭Backpack连接时出错: {e}")
    
    def _generate_ed25519_signature(self, params: Dict, instruction: str, timestamp: int, window: int = None) -> str:
        """
        使用ED25519算法生成API请求签名
        
        Args:
            params: 请求参数
            instruction: API指令
            timestamp: 时间戳（毫秒）
            window: 请求有效窗口时间（毫秒）
            
        Returns:
            Base64编码的签名
        """
        if window is None:
            window = self.default_window
            
        try:
            # 创建参数的副本以避免修改原始数据
            params_copy = params.copy()
            
            # 按字母顺序排序参数并转换为查询字符串
            def sort_params(params):
                if not params:
                    return ""
                
                # 确保所有参数都是字符串
                converted_params = {}
                for k, v in params.items():
                    # 简单直接地转换所有值为字符串，即使是整数也要转为字符串
                    converted_params[k] = str(v)
                
                sorted_params = sorted(converted_params.items(), key=lambda x: x[0])
                return "&".join([f"{k}={v}" for k, v in sorted_params])
            
            # 构建参数字符串
            param_str = sort_params(params_copy)
            
            # 构建头部信息
            header_params = {
                "timestamp": timestamp,
                "window": window
            }
            header_str = sort_params(header_params)
            
            # 构建完整的消息
            message_to_sign = "instruction=" + instruction
            if param_str:
                message_to_sign += "&" + param_str
            message_to_sign += "&" + header_str
            
            self.logger.debug(f"待签名消息: {message_to_sign}")
            
            # 使用ED25519算法签名
            signing_key = nacl.signing.SigningKey(self.api_secret_bytes)
            signed_message = signing_key.sign(message_to_sign.encode())
            signature = base64.b64encode(signed_message.signature).decode()
            
            self.logger.debug(f"ED25519签名: {signature}")
            return signature
        except Exception as e:
            self.logger.error(f"生成ED25519签名错误: {e}")
            raise
    
    async def _make_signed_request(
        self, 
        instruction: str,
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None, 
        data: Optional[Dict] = None
    ) -> Dict:
        """
        发送带签名的API请求
        
        Args:
            instruction: API指令名称
            method: HTTP方法（GET、POST等）
            endpoint: API端点（不包含基础URL）
            params: URL参数
            data: 请求体数据
            
        Returns:
            API响应数据
        """
        # 准备请求参数
        request_params = {}
        if params:
            # 确保参数值是字符串
            request_params = {k: str(v) for k, v in params.items()}
            
        # 如果是POST或DELETE请求，使用请求体参数
        if data and (method == "POST" or method == "DELETE"):
            # 确保数据值是字符串
            for k, v in data.items():
                request_params[k] = str(v)
        
        # 准备URL
        url = f"{self.base_url}{endpoint}"
        if method == "GET" and params:
            # 对于GET请求，将参数添加到URL
            query_string = "&".join([f"{k}={v}" for k, v in sorted(request_params.items())])
            url = f"{url}?{query_string}"
        
        # 准备请求体
        body = None
        if method in ["POST", "DELETE"] and data:
            body = json.dumps(data, separators=(',', ':'))
            
        # 生成签名
        timestamp = int(time.time() * 1000)
        signature = self._generate_ed25519_signature(request_params, instruction, timestamp)
        
        # 准备请求头
        headers = {
            "X-API-KEY": self.api_key,
            "X-SIGNATURE": signature,
            "X-TIMESTAMP": str(timestamp),
            "X-WINDOW": str(self.default_window),
            "Content-Type": "application/json"
        }
        
        self.logger.debug(f"请求URL: {url}")
        if body:
            self.logger.debug(f"请求体: {body}")
        self.logger.debug(f"请求头: {headers}")
        
        # 发送请求
        try:
            if method == "GET":
                response = await self.http_client.get(url, headers=headers)
            elif method == "POST":
                response = await self.http_client.post(url, headers=headers, content=body)
            elif method == "DELETE":
                response = await self.http_client.delete(url, headers=headers, content=body)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")
            
            # 检查响应
            self.logger.debug(f"响应状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text}")
            
            if response.status_code != 200:
                self.logger.error(f"API请求失败: {response.status_code} - {response.text}")
                
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP错误: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            self.logger.error(f"请求错误: {str(e)}")
            raise
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """
        获取指定交易对的最新价格
        
        Args:
            symbol: 交易对，如 "BTC_USDC_PERP"
            
        Returns:
            最新价格，如果无法获取则返回None
        """
        # 如果已经通过WebSocket获取了价格，直接返回
        if symbol in self.prices:
            return self.prices.get(symbol)
        
        # 否则通过REST API获取
        try:
            response = await self.http_client.get(f"{self.base_url}/api/v1/ticker/24hr?symbol={symbol}")
            if response.status_code == 200:
                data = response.json()
                if "lastPrice" in data:
                    price = float(data["lastPrice"])
                    self.prices[symbol] = price
                    return price
            return None
        except Exception as e:
            self.logger.error(f"获取{symbol}价格失败: {e}")
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """
        获取指定交易对的资金费率
        
        Args:
            symbol: 交易对，如 "BTC_USDC_PERP"
            
        Returns:
            资金费率，如果无法获取则返回None
        """
        try:
            # 构建请求URL
            url = f"{self.base_url}/api/v1/fundingRates?symbol={symbol}"
            self.logger.debug(f"请求URL: {url}")
            
            # 发送请求
            response = await self.http_client.get(url)
            
            # 打印响应状态码和内容（用于调试）
            self.logger.debug(f"响应状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:200]}")
            
            # 检查响应状态
            if response.status_code != 200:
                self.logger.error(f"获取资金费率失败，状态码: {response.status_code}")
                return None
            
            # 解析响应
            data = response.json()
            
            # 检查数据有效性
            if not data or not isinstance(data, list) or len(data) == 0:
                self.logger.warning(f"未找到{symbol}的资金费率数据")
                return None
            
            # 获取最新的资金费率
            latest_funding = data[0]
            
            if "fundingRate" in latest_funding:
                funding_rate = float(latest_funding["fundingRate"])
                self.logger.info(f"获取到{symbol}资金费率: {funding_rate}")
                return funding_rate
            else:
                self.logger.warning(f"资金费率数据格式异常: {latest_funding}")
                return None
                
        except Exception as e:
            self.logger.error(f"获取资金费率时出错: {e}")
            return None

    async def get_all_funding_rates(self) -> Dict[str, float]:
        """
        获取所有交易对的资金费率
        
        Returns:
            Dict[str, float]: 交易对到资金费率的映射，如 {"BTC_USDC_PERP": 0.0001}
        """
        try:
            # 获取所有可用交易对列表
            trading_pairs = [f"{pair.get('symbol')}_USDC_PERP" for pair in self.config.get("trading_pairs", [])]
            self.logger.info(f"准备获取 {len(trading_pairs)} 个交易对的资金费率")
            
            # 定义单个交易对资金费率获取函数
            async def get_single_funding_rate(symbol: str) -> Tuple[str, Optional[float]]:
                try:
                    url = f"{self.base_url}/api/v1/fundingRates?symbol={symbol}"
                    response = await self.http_client.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, list) and len(data) > 0:
                            # 取最新的资金费率（第一个元素）
                            funding_rate = float(data[0].get("fundingRate", 0))
                            # 记录成功
                            self.logger.debug(f"成功获取 {symbol} 资金费率: {funding_rate}")
                            return symbol, funding_rate
                    
                    self.logger.warning(f"获取 {symbol} 资金费率失败，状态码: {response.status_code}")
                    return symbol, None
                except Exception as e:
                    self.logger.error(f"获取 {symbol} 资金费率出错: {e}")
                    return symbol, None
            
            # 并行请求所有交易对的资金费率
            tasks = []
            for symbol in trading_pairs:
                tasks.append(get_single_funding_rate(symbol))
            
            self.logger.info(f"正在并行请求 {len(tasks)} 个交易对的资金费率")
            results = await asyncio.gather(*tasks)
            
            # 整理结果
            funding_rates = {}
            success_count = 0
            for symbol, rate in results:
                if rate is not None:
                    funding_rates[symbol] = rate
                    success_count += 1
            
            self.logger.info(f"成功获取 {success_count}/{len(tasks)} 个交易对的资金费率")
            return funding_rates
            
        except Exception as e:
            self.logger.error(f"批量获取资金费率出错: {e}")
            return {}

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str = "MARKET",
        size: float = None,
        price: float = None
    ) -> Dict:
        """
        下单接口
        
        Args:
            symbol: 交易对，如 "BTC_USDC_PERP"
            side: 方向，"BUY" 或 "SELL"
            order_type: 订单类型，"MARKET" 或 "LIMIT"
            size: 数量
            price: 价格，MARKET类型可以为None
            
        Returns:
            订单信息
        """
        if size is None or size <= 0:
            raise ValueError("下单数量必须大于0")
            
        try:
            # 验证参数
            if order_type not in ["MARKET", "LIMIT"]:
                raise ValueError("订单类型必须是 MARKET 或 LIMIT")
            if side not in ["BUY", "SELL"]:
                raise ValueError("交易方向必须是 BUY 或 SELL")
                
            # 从配置中获取交易对信息
            trading_pair_config = None
            symbol_base = symbol.split("_")[0]  # 提取币种名称，如从"BTC_USDC_PERP"提取"BTC"
            for pair in self.config.get("trading_pairs", []):
                if pair.get("symbol") == symbol_base:
                    trading_pair_config = pair
                    break
            
            # 根据交易对确定数量精度和tick_size
            quantity_precision = 3  # 默认使用3位精度
            tick_size = 0.01     # 默认tick_size
            if trading_pair_config:
                quantity_precision = int(trading_pair_config.get("size_precision", 3))
                tick_size = float(trading_pair_config.get("tick_size", 0.01))
            
            # 调整数量精度
            step_size = 10 ** -quantity_precision
            adjusted_size = math.floor(float(size) / step_size) * step_size
            
            # 确保数量不会被格式化为0
            if adjusted_size < step_size:
                adjusted_size = step_size
            
            formatted_size = "{:.{}f}".format(adjusted_size, quantity_precision)
            
            # 生成clientId (使用时间戳的低32位)
            client_id = int(time.time() * 1000) & 0xFFFFFFFF
            
            # 准备订单数据
            order_data = {
                "symbol": symbol,  # 使用正确的交易对格式，如 BTC_USDC_PERP
                "side": "Bid" if side == "BUY" else "Ask",  # 使用正确的Side枚举值
                "orderType": "Market" if order_type == "MARKET" else "Limit",
                "quantity": formatted_size,  # 已经是字符串
                "timeInForce": "GTC",
                "clientId": client_id
            }
            
            # 如果是限价单，添加价格
            if order_type == "LIMIT":
                if price is None or price <= 0:
                    raise ValueError("限价单必须指定有效的价格")
                
                # 从配置中获取交易对信息
                trading_pair_config = None
                for pair in self.config.get("trading_pairs", []):
                    if pair.get("symbol") == symbol_base:
                        trading_pair_config = pair
                        break
                
                # 根据交易对确定价格精度和tick_size
                price_precision = 2  # 默认精度
                if trading_pair_config:
                    price_precision = int(trading_pair_config.get("price_precision", 2))
                    tick_size = float(trading_pair_config.get("tick_size", 0.01))
                
                # 确保价格是浮点数
                price = float(price)
                
                # 调整价格为tick_size的倍数
                price = round(price / tick_size) * tick_size
                
                # 控制小数位数，确保不超过配置的精度
                price = round(price, price_precision)
                
                # 格式化价格，确保精度符合要求
                formatted_price = "{:.{}f}".format(price, price_precision)
                
                # 去除尾部多余的0
                formatted_price = formatted_price.rstrip('0').rstrip('.') if '.' in formatted_price else formatted_price
                
                self.logger.info(f"价格处理: 原始价格={price}, 格式化后={formatted_price}, 精度={price_precision}, tick_size={tick_size}")
                
                order_data["price"] = formatted_price
            
            self.logger.info(f"正在Backpack下单: {order_data}")
            
            # 生成签名
            timestamp = int(time.time() * 1000)
            signature = self._generate_ed25519_signature(order_data, "orderExecute", timestamp)
            
            # 发送请求
            headers = {
                "X-API-KEY": self.api_key,
                "X-TIMESTAMP": str(timestamp),
                "X-SIGNATURE": signature,
                "Content-Type": "application/json"
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/v1/order",
                    headers=headers,
                    json=order_data
                )
                
                if response.status_code != 200:
                    error_text = response.text
                    self.logger.error(f"Backpack下单失败: {error_text}")
                    raise ValueError(f"Backpack下单失败: {error_text}")
                    
                result = response.json()
                self.logger.info(f"Backpack下单成功: {result}")
                return result
                
        except Exception as e:
            self.logger.error(f"Backpack下单失败: {e}")
            raise

    async def get_order_status(self, order_id: str) -> Dict:
        """
        获取订单状态
        
        Args:
            order_id: 订单ID
            
        Returns:
            订单状态信息
        """
        response = await self._make_signed_request("GET", f"/api/v1/order/{order_id}")
        return response

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """
        获取当前持仓
        
        Args:
            symbol: 交易对，如 "BTC_USDC_PERP"
            
        Returns:
            持仓信息，如果无持仓则返回None
        """
        try:
            response = await self._make_signed_request("GET", "/api/v1/positions")
            
            if not response:
                return None
                
            # 查找指定交易对的持仓
            for position in response:
                if position.get("symbol") == symbol:
                    # 确保数据有效
                    if "size" in position:
                        size = float(position.get("size", 0))
                        if size != 0:
                            # 确定持仓方向
                            side = "BUY" if size > 0 else "SELL"
                            abs_size = abs(size)
                            
                            return {
                                "symbol": symbol,
                                "side": side,
                                "size": abs_size,
                                "entry_price": float(position.get("entryPrice", 0)),
                                "mark_price": float(position.get("markPrice", 0)),
                                "unrealized_pnl": float(position.get("unrealizedPnl", 0))
                            }
            
            return None
        except Exception as e:
            self.logger.error(f"获取{symbol}持仓信息失败: {e}")
            return None

    async def close_position(self, symbol: str, size: Optional[float] = None) -> Dict:
        """
        平仓
        
        Args:
            symbol: 交易对，如 "BTC_USDC_PERP"
            size: 平仓数量，如果为None则全部平仓
            
        Returns:
            订单信息
        """
        # 获取当前持仓
        position = await self.get_position(symbol)
        if not position:
            self.logger.warning(f"没有{symbol}的持仓，无法平仓")
            return {"error": "没有持仓"}
        
        # 确定平仓方向（与持仓方向相反）
        close_side = "SELL" if position["side"] == "BUY" else "BUY"
        
        # 确定平仓数量
        close_size = size if size is not None else position["size"]
        
        # 执行平仓订单
        return await self.place_order(symbol, close_side, "MARKET", close_size)

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
        """
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.ws_connected = True
                    self.logger.info("Backpack WebSocket已连接")
                    
                    # 获取永续合约交易对列表
                    symbols = await self._get_perp_symbols()
                    
                    # 构建订阅参数
                    channels = [f"ticker.{symbol}" for symbol in symbols]
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": channels,
                        "id": 1
                    }
                    
                    # 发送订阅请求
                    await ws.send(json.dumps(subscribe_msg))
                    self.logger.info(f"已向Backpack发送订阅请求，共 {len(channels)} 个交易对")
                    
                    # 接收和处理消息
                    while True:
                        message = await ws.recv()
                        data_json = json.loads(message)
                        
                        # 处理确认消息
                        if "result" in data_json and data_json.get("id") == 1:
                            self.logger.info("Backpack订阅成功")
                            continue
                        
                        # 处理价格数据
                        if "data" in data_json and "stream" in data_json:
                            stream = data_json["stream"]
                            if stream.startswith("ticker."):
                                symbol = stream.replace("ticker.", "")
                                price_data = data_json["data"]
                                
                                # 确保数据中包含最新价格和交易对
                                if "c" in price_data and "s" in price_data:
                                    self.prices[symbol] = float(price_data["c"])
                                    
                                    # 检查是否包含资金费率数据(有些交易所在ticker中包含资金费率)
                                    if "fr" in price_data:
                                        self.funding_rates[symbol] = float(price_data["fr"])
            except Exception as e:
                self.ws_connected = False
                self.logger.error(f"Backpack WebSocket错误: {e}")
                # 等待5秒后重连
                await asyncio.sleep(5)
    
    async def _get_perp_symbols(self) -> List[str]:
        """
        获取所有永续合约交易对
        
        Returns:
            交易对列表
        """
        # 默认永续合约列表
        default_symbols = [
            "BTC_USDC_PERP", "ETH_USDC_PERP", "SOL_USDC_PERP", 
            "AVAX_USDC_PERP", "DOGE_USDC_PERP", "XRP_USDC_PERP",
            "ADA_USDC_PERP", "LINK_USDC_PERP", "BNB_USDC_PERP",
            "HYPE_USDC_PERP", "WIF_USDC_PERP", "BERA_USDC_PERP",
            "LTC_USDC_PERP", "SUI_USDC_PERP", "JUP_USDC_PERP",
            "S_USDC_PERP", "IP_USDC_PERP", "TRUMP_USDC_PERP"
        ]
        
        try:
            # 尝试从API获取交易对列表
            response = await self.http_client.get(f"{self.base_url}/api/v1/tickers")
            if response.status_code == 200:
                tickers = response.json()
                symbols = []
                for ticker in tickers:
                    symbol = ticker.get("symbol")
                    if symbol and symbol.endswith("_USDC_PERP"):
                        symbols.append(symbol)
                if symbols:
                    return symbols
        except Exception as e:
            self.logger.error(f"获取Backpack交易对列表失败: {e}")
        
        return default_symbols

    async def get_positions(self, url_path: str = "/api/v1/position") -> Dict[str, Dict[str, Any]]:
        """
        获取当前所有持仓
        
        Args:
            url_path: API端点路径，默认为"/api/v1/position"
            
        Returns:
            持仓信息，格式为: {symbol: position_data}
        """
        try:
            self.logger.info(f"获取持仓信息，使用端点: {url_path}")
            url = f"{self.base_url}{url_path}"
            
            # 获取当前时间戳
            timestamp = int(time.time() * 1000)
            
            # 使用instruction方式签名，这是已知有效的方法
            instruction = "positionQuery"
            params = {}
            signature = self._generate_ed25519_signature(params, instruction, timestamp)
            
            headers = {
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": str(timestamp),
                "X-WINDOW": str(self.default_window),
                "Content-Type": "application/json"
            }
            
            self.logger.debug(f"instruction方式签名请求头: {headers}")
            
            # 获取所有交易对
            positions = {}
            
            # 发送请求获取持仓信息
            response = await self.http_client.get(url, headers=headers)
            
            # 检查响应状态
            if response.status_code == 200:
                # 解析响应
                data = response.json()
                self.logger.debug(f"使用instruction方式签名的响应: {data}")
                
                # 解析持仓数据
                if isinstance(data, list):
                    for position in data:
                        # 使用netQuantity字段替代size字段
                        position_size = position.get("netQuantity", "0")
                        
                        if not position_size or float(position_size) == 0:
                            self.logger.debug(f"跳过零持仓: {position.get('symbol')}")
                            continue  # 跳过空仓位
                            
                        symbol = position.get("symbol")
                        size = float(position_size)
                        side = "BUY" if size > 0 else "SELL"
                        size = abs(size)
                        
                        positions[symbol] = {
                            "side": side,
                            "size": size,
                            "entry_price": float(position.get("entryPrice", 0)),
                            "mark_price": float(position.get("markPrice", 0)),
                            "pnl": float(position.get("pnlUnrealized", position.get("unrealizedPnl", 0)))
                        }
                        
                        self.logger.info(f"Backpack持仓: {symbol}, 方向: {side}, 数量: {size}")
                    
                    self.logger.info(f"使用instruction方式获取到{len(positions)}个Backpack持仓")
                    return positions
                else:
                    self.logger.warning(f"意外的响应格式，期望列表但收到: {type(data)}")
                    return {}
            else:
                self.logger.error(f"获取持仓信息失败，状态码: {response.status_code}, 响应: {response.text}")
                
                # 尝试按交易对单独查询（如果请求所有持仓失败）
                if hasattr(self, 'config') and self.config and 'trading_pairs' in self.config:
                    for pair in self.config['trading_pairs']:
                        symbol = pair.get('symbol')
                        quote = pair.get('quote', 'USDC')
                        suffix = "_PERP" if pair.get('is_perp', True) else ""
                        full_symbol = f"{symbol}_{quote}{suffix}"
                        
                        self.logger.info(f"查询交易对 {full_symbol} 的持仓...")
                        
                        # 构建查询参数
                        query_params = {"symbol": full_symbol}
                        query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
                        
                        # 获取新的时间戳
                        timestamp = int(time.time() * 1000)
                        
                        # 使用instruction方式生成签名
                        instruction = "positionQuery"
                        params = query_params.copy()
                        signature = self._generate_ed25519_signature(params, instruction, timestamp)
                        
                        headers = {
                            "X-API-KEY": self.api_key,
                            "X-SIGNATURE": signature,
                            "X-TIMESTAMP": str(timestamp),
                            "X-WINDOW": str(self.default_window),
                            "Content-Type": "application/json"
                        }
                        
                        # 发送请求
                        symbol_url = f"{url}?{query_string}"
                        
                        try:
                            symbol_response = await self.http_client.get(symbol_url, headers=headers)
                            
                            if symbol_response.status_code == 200:
                                symbol_data = symbol_response.json()
                                self.logger.debug(f"{full_symbol} 持仓响应: {symbol_data}")
                                
                                # 处理单个交易对的持仓数据
                                if symbol_data and isinstance(symbol_data, dict):
                                    # 使用netQuantity字段替代size字段
                                    position_size = symbol_data.get("netQuantity", "0")
                                    
                                    if position_size and float(position_size) != 0:
                                        size = float(position_size)
                                        side = "BUY" if size > 0 else "SELL"
                                        size = abs(size)
                                        
                                        positions[full_symbol] = {
                                            "side": side,
                                            "size": size,
                                            "entry_price": float(symbol_data.get("entryPrice", 0)),
                                            "mark_price": float(symbol_data.get("markPrice", 0)),
                                            "pnl": float(symbol_data.get("pnlUnrealized", symbol_data.get("unrealizedPnl", 0)))
                                        }
                                        
                                        self.logger.info(f"Backpack持仓: {full_symbol}, 方向: {side}, 数量: {size}")
                            elif symbol_response.status_code == 404:
                                self.logger.warning(f"交易对 {full_symbol} 不存在或无持仓")
                            else:
                                self.logger.warning(f"获取交易对 {full_symbol} 持仓失败: {symbol_response.status_code} - {symbol_response.text}")
                        except Exception as symbol_e:
                            self.logger.error(f"查询交易对 {full_symbol} 持仓出错: {symbol_e}")
                
                self.logger.info(f"按交易对查询后获取到{len(positions)}个Backpack持仓")
                return positions
                
        except Exception as e:
            self.logger.error(f"获取持仓信息时出错: {e}")
            self.logger.debug(f"异常详情: {traceback.format_exc()}")
            return {}

    def _generate_auth_headers(self, method: str, url_path: str, body: str, timestamp: int) -> Dict[str, str]:
        """
        生成API认证头
        
        Args:
            method: HTTP方法（GET、POST等）
            url_path: URL路径部分
            body: 请求体（如果有）
            timestamp: 时间戳（毫秒）
            
        Returns:
            请求头字典
        """
        try:
            # 构建签名字符串
            signature_payload = f"{timestamp}{method}{url_path}{body}"
            self.logger.debug(f"签名负载: {signature_payload}")
            
            # 使用ED25519算法签名
            signing_key = nacl.signing.SigningKey(self.api_secret_bytes)
            signed_message = signing_key.sign(signature_payload.encode())
            signature = base64.b64encode(signed_message.signature).decode()
            
            # 构建请求头 - 确保头部字段与Backpack API要求一致
            headers = {
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": str(timestamp),
                "X-WINDOW": str(self.default_window),
                "Content-Type": "application/json"
            }
            
            self.logger.debug(f"生成认证头: {headers}")
            return headers
        except Exception as e:
            self.logger.error(f"生成认证头失败: {e}")
            raise 

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        """
        取消订单
        
        Args:
            symbol: 交易对，如 BTC_USDC_PERP
            order_id: 订单ID
            
        Returns:
            取消订单响应
        """
        try:
            endpoint = "/api/v1/order"
            timestamp = int(time.time() * 1000)
            
            # 创建取消订单请求体
            payload = {
                "symbol": symbol,
                "orderId": order_id
            }
            
            # 为DELETE请求生成签名
            payload_str = json.dumps(payload)
            signature_payload = f"{timestamp}DELETE{endpoint}{payload_str}"
            self.logger.debug(f"签名负载: {signature_payload}")
            
            # 创建签名
            signature = self._generate_ed25519_signature({}, "DELETE", timestamp)
            
            # 创建认证头
            headers = self._generate_auth_headers("DELETE", endpoint, payload_str, timestamp)
            
            # 构建完整URL
            url = f"{self.base_url}{endpoint}"
            self.logger.debug(f"取消订单请求URL: {url}")
            self.logger.debug(f"取消订单请求载荷: {payload}")
            
            # 检查使用的HTTP客户端对象
            if hasattr(self, 'http_client'):
                response = await self.http_client.delete(url, headers=headers, json=payload)
                status = response.status_code
                if status == 200:
                    data = response.json()
                    return data
                else:
                    error_text = response.text
                    self.logger.error(f"取消订单失败: HTTP {status}, {error_text}")
                    return {"error": f"HTTP {status}: {error_text}"}
            else:
                # 假设使用aiohttp.ClientSession
                async with self.session.delete(url, headers=headers, json=payload) as response:
                    status = response.status
                    if status == 200:
                        data = await response.json()
                        return data
                    else:
                        error_text = await response.text()
                        self.logger.error(f"取消订单失败: HTTP {status}, {error_text}")
                        return {"error": f"HTTP {status}: {error_text}"}
        except Exception as e:
            self.logger.error(f"取消订单时出错: {e}")
            return {"error": str(e)} 

    async def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        获取指定币种的订单深度数据
        
        Args:
            symbol: 币种，如 "BTC_USDC_PERP"
            
        Returns:
            订单深度数据，格式为 {"bids": [...], "asks": [...], "timestamp": ...}
            如果无法获取则返回None
        """
        try:
            # 添加调试日志
            self.logger.debug(f"获取Backpack订单簿: {symbol}")
            
            # 检查格式
            if "_PERP" not in symbol:
                # 如果是简单币种名称，如"BTC"，转换为正确格式
                if "_USDC_PERP" not in symbol:
                    symbol = f"{symbol}_USDC_PERP"
                    self.logger.debug(f"自动转换为标准格式: {symbol}")
            
            # 构建API请求URL - 更正为正确的API端点
            url = f"{self.base_url}/api/v1/depth?symbol={symbol}"
            self.logger.debug(f"请求URL: {url}")
            
            # 发送请求
            response = await self.http_client.get(url)
            
            # 检查响应状态
            if response.status_code != 200:
                self.logger.error(f"获取订单深度HTTP错误: {response.status_code}, {response.text}")
                return None
                
            # 解析响应数据
            data = response.json()
            
            # 添加调试信息
            self.logger.debug(f"Backpack订单簿原始数据: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            
            # 处理响应数据
            if isinstance(data, dict) and "bids" in data and "asks" in data:
                # 记录原始数据样本，便于调试
                if data.get("bids"):
                    self.logger.debug(f"Backpack订单簿原始bid样本: {data['bids'][0]}")
                if data.get("asks"):
                    self.logger.debug(f"Backpack订单簿原始ask样本: {data['asks'][0]}")
                
                # 转换数据格式为统一格式：[price, size]
                bids = [[float(bid[0]), float(bid[1])] for bid in data.get("bids", [])]
                asks = [[float(ask[0]), float(ask[1])] for ask in data.get("asks", [])]
                
                # 记录转换后的数据样本
                if bids:
                    self.logger.debug(f"Backpack订单簿转换后bid样本: {bids[0]}")
                if asks:
                    self.logger.debug(f"Backpack订单簿转换后ask样本: {asks[0]}")
                
                # 返回统一格式的订单簿
                orderbook = {
                    "timestamp": time.time(),
                    "bids": bids,
                    "asks": asks
                }
                return orderbook
            else:
                self.logger.error(f"订单深度数据格式异常: {data}")
                return None
                
        except Exception as e:
            self.logger.error(f"获取Backpack订单深度出错: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None 