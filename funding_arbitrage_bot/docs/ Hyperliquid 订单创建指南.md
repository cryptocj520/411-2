# Hyperliquid 订单创建指南

## 目录
1. [基本设置](#基本设置)
2. [订单类型](#订单类型)
3. [参数说明](#参数说明)
4. [注意事项](#注意事项)
5. [示例代码](#示例代码)
6. [常见错误](#常见错误)
7. [测试结果](#测试结果)
8. [主脚本修改](#主脚本修改)

## 基本设置

### 1. 环境准备
```python
from hyperliquid.exchange import Exchange
from eth_account import Account
```

### 2. 初始化交易所
```python
# 创建钱包对象
wallet = Account.from_key(private_key)

# 初始化交易所
exchange = Exchange(wallet=wallet)
```

## 订单类型

根据我们的测试结果，Hyperliquid 目前**只支持限价单（Limit Order）**，**不支持市价单（Market Order）**。

### 限价单格式
```python
order_type = {
    "limit": {
        "tif": "Gtc"  # Good Till Cancel
    }
}
```

### 市价单测试结果
我们尝试了多种市价单格式，包括：
1. 使用 IOC (Immediate or Cancel) 格式 `{"ioc": {}}`
2. 使用 FOK (Fill or Kill) 格式 `{"fok": {}}`
3. 使用 Post Only 格式 `{"post_only": {}}`

所有市价单格式都返回了 `Invalid order type` 错误，表明 Hyperliquid API 目前不支持市价单。

## 参数说明

### 1. 必需参数
- `name`: 交易对名称（字符串），例如 "SOL"
- `is_buy`: 交易方向（布尔值）
  - `True`: 买入/做多
  - `False`: 卖出/做空
- `sz`: 交易数量（浮点数），例如 0.1
- `limit_px`: 限价（浮点数），例如 127.0
- `order_type`: 订单类型（字典）

### 2. 参数格式要求
- 所有数值参数（`sz` 和 `limit_px`）必须是浮点数类型，不能是字符串
- 订单类型必须是字典格式，包含 `limit` 和 `tif` 字段

## 注意事项

1. **数值类型**
   - 所有数值必须使用浮点数类型
   - 不要使用字符串类型的数值，例如 `"0.1"` 或 `"127.0"`

2. **订单类型**
   - 目前只支持限价单
   - 不支持市价单
   - 必须指定 `tif` 参数

3. **价格精度**
   - 确保价格符合交易所的精度要求
   - 建议使用合理的价格，避免价格过高或过低

4. **数量精度**
   - 确保数量符合交易所的最小交易单位
   - 建议使用合理的数量，避免数量过小

## 示例代码

### 限价单示例
```python
# 创建限价单
order_result = exchange.order(
    name="SOL",           # 交易对
    is_buy=True,          # 买入/做多
    sz=0.1,              # 数量（浮点数）
    limit_px=127.0,      # 限价（浮点数）
    order_type={         # 订单类型
        "limit": {
            "tif": "Gtc"
        }
    }
)
```

## 常见错误

1. **Invalid order type**
   - 原因：订单类型格式不正确
   - 解决：使用正确的限价单格式 `{"limit": {"tif": "Gtc"}}`

2. **Unknown format code 'f' for object of type 'str'**
   - 原因：数值参数使用了字符串类型
   - 解决：将字符串类型的数值改为浮点数类型

3. **Order size too small**
   - 原因：订单数量小于最小交易单位
   - 解决：增加订单数量到最小交易单位以上

4. **Invalid price**
   - 原因：价格不符合交易所要求
   - 解决：使用合理的价格，确保符合交易所的精度要求

## 最佳实践

1. **错误处理**
   ```python
   try:
       order_result = exchange.order(...)
       logger.info(f"下单结果: {order_result}")
   except Exception as e:
       logger.error(f"下单失败: {e}", exc_info=True)
   ```

2. **日志记录**
   ```python
   logger.info(f"正在下单: {symbol} {'买入' if is_buy else '卖出'} {size}，限价: {limit_price}")
   ```

3. **参数验证**
   - 在下单前验证所有参数
   - 确保数值类型正确
   - 确保价格和数量在合理范围内

4. **订单状态检查**
   - 下单后检查订单状态
   - 记录订单ID以便后续查询
   - 监控订单是否成交

## 测试结果

### 限价单测试
限价单测试成功，返回结果：
```
{'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [{'resting': {'oid': 83400267467}}]}}}
```

这表明：
1. 订单状态为 `ok`，表示下单成功
2. 订单类型为 `order`
3. 订单状态为 `resting`，表示订单已经提交但尚未成交
4. 订单ID为 `83400267467`

### 市价单测试
我们尝试了以下市价单格式：
1. IOC (Immediate or Cancel): `{"ioc": {}}`
2. FOK (Fill or Kill): `{"fok": {}}`
3. Post Only: `{"post_only": {}}`

所有市价单格式都返回了 `Invalid order type` 错误，表明 Hyperliquid API 目前不支持市价单。

## 主脚本修改

根据测试结果，我们对主脚本进行了以下修改：

### 1. 修改 `hyperliquid_api.py` 中的 `place_order` 方法

```python
# 修改前
if order_type.upper() == "MARKET":
    # 市价单
    price_adjuster = 1.01 if is_buy else 0.99
    market_price = str(round(current_price * price_adjuster, price_precision))
    
    order_result = self.exchange.order(
        name=name,
        is_buy=is_buy,
        sz=formatted_size,  # 已经是字符串
        limit_px=market_price,  # 市价单也需要提供参考价格
        order_type={'market': {'tif': 'Gtc'}}  # 使用错误的订单类型格式
    )
else:
    # 限价单
    price_adjuster = 1.005 if is_buy else 0.995
    limit_price = str(round(current_price * price_adjuster, price_precision))
    
    order_result = self.exchange.order(
        name=name,
        is_buy=is_buy,
        sz=formatted_size,  # 已经是字符串
        limit_px=limit_price,  # 已经是字符串
        order_type={'limit': {'tif': 'Gtc'}}  # 使用正确的订单类型格式
    )

# 修改后
# 根据方向调整价格，确保订单能够快速成交
price_adjuster = 1.01 if is_buy else 0.99  # 买入时价格略高，卖出时价格略低
limit_price = float(round(current_price * price_adjuster, price_precision))  # 使用浮点数类型

# 使用限价单格式（Hyperliquid不支持市价单）
order_result = self.exchange.order(
    name=name,
    is_buy=is_buy,
    sz=formatted_size,  # 使用浮点数类型
    limit_px=limit_price,  # 使用浮点数类型
    order_type={"limit": {"tif": "Gtc"}}  # 使用正确的限价单格式
)
```

### 2. 修改 `arbitrage_engine.py` 中的开仓和平仓逻辑

```python
# 修改前
bp_order, hl_order = await asyncio.gather(
    self.backpack_api.place_order(
        position_data["bp_symbol"],
        position_data["bp_side"],
        "MARKET",  # 使用市价单
        bp_size,
        None  # 市价单不需要价格
    ),
    self.hyperliquid_api.place_order(
        position_data["hl_symbol"],
        position_data["hl_side"],
        "MARKET",  # 使用市价单
        hl_size,
        None  # 市价单不需要价格
    )
)

# 修改后
bp_order, hl_order = await asyncio.gather(
    self.backpack_api.place_order(
        position_data["bp_symbol"],
        position_data["bp_side"],
        "MARKET",  # Backpack使用市价单
        bp_size,
        None  # 市价单不需要价格
    ),
    self.hyperliquid_api.place_order(
        position_data["hl_symbol"],
        position_data["hl_side"],
        "LIMIT",  # Hyperliquid使用限价单
        hl_size,
        None  # 价格会在API内部计算
    )
)
```

### 3. 主要修改点

1. **订单类型**：
   - 将 Hyperliquid 的市价单改为限价单
   - 使用正确的限价单格式 `{"limit": {"tif": "Gtc"}}`

2. **参数类型**：
   - 将字符串类型的数值改为浮点数类型
   - 确保 `sz` 和 `limit_px` 参数使用浮点数类型

3. **价格调整**：
   - 买入时价格略高（+1%），卖出时价格略低（-1%）
   - 确保订单能够快速成交

这些修改应该能够解决 Hyperliquid 无法成功下单的问题。 