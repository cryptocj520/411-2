# Backpack和Hyperliquid交易所API对比文档

本文档详细对比了Backpack和Hyperliquid两个交易所在资金费率套利机器人中的API实现方式，包括价格获取、资金费率查询、持仓管理和订单创建等关键功能。

## 价格获取方式

### Backpack
1. **主要方式**：WebSocket订阅实时价格
   ```python
   # 如果已经通过WebSocket获取了价格，直接返回
   if symbol in self.prices:
       return self.prices.get(symbol)
   ```

2. **备用方式**：REST API请求
   ```python
   # 如果没有WebSocket缓存，通过REST API获取
   response = await self.http_client.get(f"{self.base_url}/api/v1/ticker/24hr?symbol={symbol}")
   if response.status_code == 200:
       data = response.json()
       if "lastPrice" in data:
           price = float(data["lastPrice"])
           self.prices[symbol] = price
           return price
   ```

3. **API端点**：`/api/v1/ticker/24hr?symbol={symbol}`

### Hyperliquid
1. **主要方式**：WebSocket订阅实时价格
   ```python
   # 首先检查WebSocket价格缓存
   if symbol in self.prices:
       return self.prices[symbol]
   ```

2. **备用方式**：REST API请求
   ```python
   # 如果WebSocket中没有，尝试通过REST API获取
   url = f"{self.base_url}/info"
   response = await self.http_client.get(url)
   
   if response.status_code == 200:
       data = response.json()
       
       # 解析响应中的价格信息
       for meta in data[0].get("universe", []):
           if meta.get("name") == symbol:
               return float(meta.get("midPrice", 0))
   ```

3. **API端点**：`/info`

## 资金费率获取方式

### Backpack
1. **方式**：REST API请求
   ```python
   url = f"{self.base_url}/api/v1/fundingRates?symbol={symbol}"
   response = await self.http_client.get(url)
   
   # 解析响应
   data = response.json()
   latest_funding = data[0]
   funding_rate = float(latest_funding["fundingRate"])
   ```

2. **API端点**：`/api/v1/fundingRates?symbol={symbol}`

### Hyperliquid
1. **方式**：REST API请求
   ```python
   url = f"{self.base_url}/info"
   payload = {"type": "metaAndAssetCtxs"}
   
   response = await self.http_client.post(url, json=payload)
   data = response.json()
   
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
           return funding_rate
   ```

2. **API端点**：`/info` 使用POST方法和`{"type": "metaAndAssetCtxs"}`载荷

3. **响应示例**：
   ```
   2025-04-06 00:35:24,200 - INFO - BTC的资产上下文: {
       'funding': '0.0000125', 
       'openInterest': '10719.24896', 
       'prevDayPx': '83114.0', 
       'dayNtlVlm': '2190927944.0337491035', 
       'premium': '-0.0002900547', 
       'oraclePx': '82743.0', 
       'markPx': '82713.0', 
       'midPx': '82718.5', 
       'impactPxs': ['82718.0', '82719.0'], 
       'dayBaseVlm': '26185.4109'
   }
   2025-04-06 00:35:24,200 - INFO - BTC 资金费率(小时): 1.25e-05, 调整后(8小时): 0.0001
   ```

## 持仓查询方式

### Backpack
1. **方式**：签名的REST API请求
   ```python
   response = await self._make_signed_request("GET", "/api/v1/positions")
   
   # 解析响应找到特定交易对的持仓
   for position in response:
       if position.get("symbol") == symbol:
           # 处理持仓数据
           size = float(position.get("size", 0))
           side = "BUY" if size > 0 else "SELL"
           abs_size = abs(size)
   ```

2. **API端点**：`/api/v1/positions`
3. **认证方式**：ED25519签名算法

### Hyperliquid
1. **方式**：REST API请求
   ```python
   url = f"{self.base_url}/info"
   payload = {
       "type": "clearinghouseState",
       "user": self.hyperliquid_address
   }
   
   response = await self.http_client.post(url, json=payload)
   user_data = response.json()
   
   # 解析持仓数据
   positions = {}
   if "assetPositions" in user_data:
       asset_positions = user_data["assetPositions"]
       
       for pos_item in asset_positions:
           # 处理每个持仓数据
           pos = pos_item["position"]
           coin = pos.get("coin")
           size = pos.get("szi")
           
           side = "BUY" if size_value > 0 else "SELL"
           abs_size = abs(size_value)
   ```

2. **API端点**：`/info` 使用POST方法和`{"type": "clearinghouseState", "user": wallet_address}`载荷
3. **认证方式**：通过钱包地址查询

## 订单创建方式

### Backpack
1. **方式**：签名的REST API请求
   ```python
   # 准备订单数据
   order_data = {
       "symbol": symbol,
       "side": "Bid" if side == "BUY" else "Ask",
       "orderType": "Market" if order_type == "MARKET" else "Limit",
       "quantity": formatted_size,
       "timeInForce": "GTC",
       "clientId": client_id
   }
   
   # 如果是限价单，添加价格
   if order_type == "LIMIT":
       order_data["price"] = formatted_price
   
   # 生成签名
   timestamp = int(time.time() * 1000)
   signature = self._generate_ed25519_signature(order_data, "orderExecute", timestamp)
   
   # 发送请求
   response = await client.post(
       f"{self.base_url}/api/v1/order",
       headers=headers,
       json=order_data
   )
   ```

2. **API端点**：`/api/v1/order`
3. **订单类型**：支持限价单(LIMIT)和市价单(MARKET)
4. **认证方式**：ED25519签名算法

### Hyperliquid
1. **方式**：使用官方SDK或REST API
   ```python
   # 使用SDK下单
   if order_type.upper() == "LIMIT":
       # 限价单
       response = self.hl_exchange.order(
           coin=coin,
           is_buy=is_buy,
           sz=sz,
           limit_px=limit_px,
           order_type="Limit"
       )
   ```

2. **SDK方法**：`hl_exchange.order()`
3. **订单类型**：只支持限价单(Limit)，市价单通过调整价格的限价单模拟
4. **认证方式**：通过钱包私钥创建的wallet对象进行签名

## 总结对比

| 功能         | Backpack                                            | Hyperliquid                                           |
|--------------|-----------------------------------------------------|-------------------------------------------------------|
| 价格获取     | 1. WebSocket实时订阅<br>2. REST API `/api/v1/ticker/24hr` | 1. WebSocket实时订阅<br>2. REST API `/info` |
| 资金费率获取 | REST API `/api/v1/fundingRates`                     | REST API `/info` POST `{"type": "metaAndAssetCtxs"}`   |
| 持仓查询     | 签名的REST API `/api/v1/positions`                  | REST API `/info` POST `{"type": "clearinghouseState"}` |
| 订单创建     | 签名的REST API `/api/v1/order`                      | 官方SDK `hl_exchange.order()`                          |
| 认证方式     | ED25519签名算法                                     | 基于以太坊钱包私钥的签名                               |
| 订单类型     | 支持限价单和市价单                                  | 仅支持限价单(通过调整价格模拟市价单)                   |
| API格式      | REST API，参数通过URL或JSON传递                     | REST API + SDK，大部分操作使用JSON载荷                 |
| 币种格式     | 交易对格式如 `BTC_USDC_PERP`                        | 基础币种格式如 `BTC`                                   |

## 资金费率数据示例

下面是Hyperliquid资金费率的测试输出示例：

```
2025-04-06 00:35:23,554 - INFO - 直接使用REST API获取资金费率
2025-04-06 00:35:24,199 - INFO - API响应类型: <class 'list'>
2025-04-06 00:35:24,200 - INFO - 找到187个币种
2025-04-06 00:35:24,200 - INFO - 币种0: BTC
2025-04-06 00:35:24,200 - INFO - BTC的资产上下文: {'funding': '0.0000125', 'openInterest': '10719.24896', 'prevDayPx': '83114.0', 'dayNtlVlm': '2190927944.0337491035', 'premium': '-0.0002900547', 'oraclePx': '82743.0', 'markPx': '82713.0', 'midPx': '82718.5', 'impactPxs': ['82718.0', '82719.0'], 'dayBaseVlm': '26185.4109'}
2025-04-06 00:35:24,200 - INFO - BTC 资金费率(小时): 1.25e-05, 调整后(8小时): 0.0001
2025-04-06 00:35:24,200 - INFO - 币种1: ETH
2025-04-06 00:35:24,200 - INFO - ETH的资产上下文: {'funding': '-0.0000066154', 'openInterest': '353244.807', 'prevDayPx': '1789.2', 'dayNtlVlm': '439389975.3573400974', 'premium': '-0.0005595971', 'oraclePx': '1787.0', 'markPx': '1785.9', 'midPx': '1785.95', 'impactPxs': ['1785.9', '1786.0'], 'dayBaseVlm': '243368.3274'}
2025-04-06 00:35:24,200 - INFO - ETH 资金费率(小时): -6.6154e-06, 调整后(8小时): -5.29232e-05
2025-04-06 00:35:24,200 - INFO - 币种2: ATOM
2025-04-06 00:35:24,200 - INFO - ATOM的资产上下文: {'funding': '-0.0000135665', 'openInterest': '340191.82', 'prevDayPx': '5.0226', 'dayNtlVlm': '2219603.6365310005', 'premium': '-0.0005309915', 'oraclePx': '4.8965', 'markPx': '4.8922', 'midPx': '4.8932', 'impactPxs': ['4.891', '4.8939'], 'dayBaseVlm': '445576.73'}
2025-04-06 00:35:24,200 - INFO - ATOM 资金费率(小时): -1.35665e-05, 调整后(8小时): -0.000108532
2025-04-06 00:35:24,200 - INFO - 单独查询 - BTC 资金费率: 1.25e-05
2025-04-06 00:35:24,200 - INFO - 单独查询 - ETH 资金费率: -6.6154e-06
2025-04-06 00:35:24,200 - INFO - 单独查询 - SOL 资金费率: -2.39842e-05
```

## 注意事项与实现提示

1. **币种格式转换**：在两个交易所之间进行套利时，需要注意币种格式转换:
   - Backpack: `BTC_USDC_PERP`
   - Hyperliquid: `BTC`

2. **资金费率比较**：
   - Hyperliquid的资金费率为每小时费率
   - 比较时可能需要转换为8小时费率以保持一致性

3. **订单下单限制**：
   - Hyperliquid只支持限价单，市价单需要通过调整价格实现
   - Backpack支持多种订单类型

4. **认证方式**：
   - Backpack: 需要API Key和Secret，使用ED25519签名
   - Hyperliquid: 需要以太坊钱包私钥和地址，使用区块链钱包认证

5. **API设计差异**：
   - Backpack: 每个功能有专门的API端点
   - Hyperliquid: 多个功能通过同一个端点（`/info`）实现，通过不同的payload区分

以上对比可帮助开发者理解两个交易所API的实现方式，为套利策略开发提供参考。 