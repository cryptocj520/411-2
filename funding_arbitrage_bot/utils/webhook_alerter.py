"""
WebhookAlerter - 用于通过Webhook发送资金费率套利机器人的交易通知
"""

import json
import logging
import requests
from typing import Dict, Any, Optional

class WebhookAlerter:
    """
    用于通过Webhook发送资金费率套利机器人的交易通知
    """
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        初始化Webhook通知器
        
        Args:
            webhook_url: Webhook URL，如果为None则禁用通知
        """
        self.webhook_url = webhook_url
        self.logger = logging.getLogger('funding_arbitrage')
        
    def send_notification(self, title: str, message: str, data: Dict[str, Any] = None) -> bool:
        """
        发送通知消息
        
        Args:
            title: 通知标题
            message: 通知内容
            data: 附加数据（可选）
            
        Returns:
            bool: 发送是否成功
        """
        if not self.webhook_url:
            return False
            
        try:
            payload = {
                "title": title,
                "message": message
            }
            
            if data:
                payload["data"] = data
                
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                self.logger.debug(f"通知发送成功: {title}")
                return True
            else:
                self.logger.warning(f"通知发送失败，状态码: {response.status_code}, 响应: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"发送通知时出错: {str(e)}")
            return False
            
    def send_order_notification(self, symbol: str, action: str, quantity: float, 
                               price: float, side: str, exchange: str) -> bool:
        """
        发送订单通知
        
        Args:
            symbol: 交易对
            action: 动作 (开仓/平仓)
            quantity: 数量
            price: 价格
            side: 方向 (多/空)
            exchange: 交易所
            
        Returns:
            bool: 发送是否成功
        """
        title = f"{action}通知 - {symbol}"
        message = f"{exchange}交易所{action}{side}单: {quantity} {symbol} @ {price}"
        
        data = {
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "price": price,
            "side": side,
            "exchange": exchange
        }
        
        return self.send_notification(title, message, data)
        
    def send_funding_notification(self, symbol: str, funding_rate: float, 
                                 funding_diff: float, exchanges: list) -> bool:
        """
        发送资金费率通知
        
        Args:
            symbol: 交易对
            funding_rate: 资金费率
            funding_diff: 资金费率差异
            exchanges: 交易所列表
            
        Returns:
            bool: 发送是否成功
        """
        title = f"资金费率通知 - {symbol}"
        message = f"{symbol}资金费率差异: {funding_diff:.6f}% ({exchanges[0]}: {funding_rate:.6f}%)"
        
        data = {
            "symbol": symbol,
            "funding_rate": funding_rate,
            "funding_diff": funding_diff,
            "exchanges": exchanges
        }
        
        return self.send_notification(title, message, data) 