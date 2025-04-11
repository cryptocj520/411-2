"""
Hyperliquid SDK包装器

提供与Hyperliquid API交互的简化方法
"""

import json
import httpx
import time
from typing import Dict, List, Tuple, Any, Optional
from eth_account import Account
from eth_account.messages import encode_defunct

class HyperliquidBase:
    """Hyperliquid基础连接类"""
    
    def __init__(self):
        self.base_url = "https://api.hyperliquid.xyz"
        self.http_client = httpx.AsyncClient(timeout=10.0)
        
    async def close(self):
        """关闭HTTP连接"""
        await self.http_client.aclose()
    
    async def request(self, endpoint: str, method: str = "GET", data: Optional[Dict] = None) -> Dict:
        """
        发送API请求
        
        Args:
            endpoint: API端点
            method: HTTP方法
            data: 请求数据
        
        Returns:
            响应数据
        """
        url = f"{self.base_url}/{endpoint}"
        print(f"HTTP Request: {method} {url} with data: {json.dumps(data, default=str)}")
        
        if method.upper() == "GET":
            response = await self.http_client.get(url, json=data)
        else:
            response = await self.http_client.post(url, json=data)
        
        print(f"HTTP Response: {response.status_code} - {response.text[:200]}...")
        
        if response.status_code != 200:
            raise Exception(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")
        
        return response.json()

class HyperliquidInfo:
    """Hyperliquid市场信息类"""
    
    def __init__(self, connection: HyperliquidBase):
        self.connection = connection
    
    async def meta_and_asset_ctxs(self) -> Tuple[Dict, List]:
        """
        获取市场元数据和资产上下文
        
        Returns:
            元数据和资产上下文的元组
        """
        response = await self.connection.request("info", method="POST", data={"type": "metaAndAssetCtxs"})
        
        # 处理不同格式的响应
        universe = []
        asset_contexts = []
        
        # 检查数据是否为字典类型，有universe和assetCtxs键
        if isinstance(response, dict) and "universe" in response and "assetCtxs" in response:
            universe = response["universe"]
            asset_contexts = response["assetCtxs"]
        # 检查数据是否为列表类型，且有两个元素
        elif isinstance(response, list) and len(response) >= 2:
            # 假设第一个元素是universe，第二个元素是assetCtxs
            if isinstance(response[0], list):
                universe = response[0]
            if len(response) > 1 and isinstance(response[1], list):
                asset_contexts = response[1]
        
        meta_data = {
            "universe": universe
        }
        
        return meta_data, asset_contexts

class HyperliquidMarketData:
    """Hyperliquid市场数据类"""
    
    def __init__(self, connection: HyperliquidBase):
        self.connection = connection
    
    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """
        获取资金费率
        
        Args:
            symbol: 币种
        
        Returns:
            资金费率，如果无法获取则返回None
        """
        try:
            response = await self.connection.request(
                "info", method="POST", data={"type": "metaAndAssetCtxs"}
            )
            
            # 处理不同格式的响应
            universe = []
            asset_contexts = []
            
            # 检查数据是否为字典类型，有universe和assetCtxs键
            if isinstance(response, dict) and "universe" in response and "assetCtxs" in response:
                universe = response["universe"]
                asset_contexts = response["assetCtxs"]
            # 检查数据是否为列表类型，且有两个元素
            elif isinstance(response, list) and len(response) >= 2:
                # 假设第一个元素是universe，第二个元素是assetCtxs
                if isinstance(response[0], list):
                    universe = response[0]
                if len(response) > 1 and isinstance(response[1], list):
                    asset_contexts = response[1]
            
            # 查找指定币种的索引
            idx = None
            for i, coin in enumerate(universe):
                coin_name = None
                if isinstance(coin, dict) and "name" in coin:
                    coin_name = coin.get("name")
                elif isinstance(coin, str):
                    coin_name = coin
                
                if coin_name == symbol:
                    idx = i
                    break
            
            if idx is not None and idx < len(asset_contexts):
                asset_ctx = asset_contexts[idx]
                if isinstance(asset_ctx, dict) and "funding" in asset_ctx:
                    return float(asset_ctx["funding"])
            
            return None
        except Exception:
            return None

class HyperliquidUser:
    """Hyperliquid用户类"""
    
    def __init__(self, connection: HyperliquidBase, wallet_address: str, wallet_secret: str):
        self.connection = connection
        self.wallet_address = wallet_address
        self.wallet_secret = wallet_secret
        # 创建钱包对象
        self.wallet = Account.from_key(wallet_secret)
    
    def sign_request(self, action_type: str, data: Dict) -> Dict:
        """
        签名请求
        
        Args:
            action_type: 操作类型
            data: 请求数据
        
        Returns:
            完整的签名请求
        """
        # 创建时间戳
        ts = int(time.time())
        
        # 创建待签名的消息（根据Hyperliquid文档格式)
        message = f"hyperliquid\n{action_type}\n{self.wallet_address.lower()}\n{ts}"
        
        # 编码消息
        message_hash = encode_defunct(text=message)
        
        # 签名
        signed_message = self.wallet.sign_message(message_hash)
        
        # 构建完整请求
        payload = {
            "action": {
                "type": action_type,
                "data": data
            },
            "signature": {
                "r": signed_message.r,
                "s": signed_message.s,
                "v": signed_message.v
            },
            "nonce": ts,
            "agent": "sdk",
            "wallet": self.wallet_address.lower()
        }
        
        return payload

class HyperliquidExchange:
    """Hyperliquid交易类"""
    
    def __init__(self, connection: HyperliquidBase, user: HyperliquidUser):
        self.connection = connection
        self.user = user
    
    async def order(self, name: str, is_buy: bool, sz: float, limit_px: float, order_type):
        """
        下单方法
        
        参数:
            name: 币种名称，如"BTC"
            is_buy: 是否买入
            sz: 数量
            limit_px: 限价
            order_type: 订单类型参数，可以是字符串("Limit")或字典格式({"limit": {"tif": "Gtc"}})或OrderType类
            
        返回:
            订单结果
        """
        # 处理order_type参数，支持多种格式
        processed_order_type = order_type
        
        # 如果是字符串类型，转换为适当的格式
        if isinstance(order_type, str):
            if order_type.lower() == "limit":
                processed_order_type = {"limit": {"tif": "Gtc"}}
                
        # 创建订单数据
        order_data = {
            "coin": name,
            "is_buy": is_buy,
            "sz": sz,
            "limit_px": limit_px,
            "order_type": processed_order_type,
            "reduce_only": False,
            "cloid": int(time.time() * 1000)  # 客户端订单ID
        }
        
        # 签名请求
        signed_request = self.user.sign_request("order", order_data)
        
        # 发送请求
        return await self.connection.request("exchange", method="POST", data=signed_request) 