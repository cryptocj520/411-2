# 资金费率套利机器人

自动监控Backpack和Hyperliquid交易所的资金费率差异，执行跨交易所资金费率套利策略。

## 功能特点

- 实时监控多个交易对的资金费率和价格
- 自动执行资金费率套利策略：在资金费率高的交易所做多，在资金费率低的交易所做空
- 当资金费率差异符号反转时自动平仓获利
- 支持自定义开仓阈值和仓位大小
- 完整的日志记录和仓位管理
- 异常处理和自动重连机制

## 安装

1. 克隆仓库
```bash
git clone <repository-url>
cd funding-arbitrage-bot
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 配置交易所API密钥
编辑`config.yaml`文件，设置你的API密钥：
```yaml
exchanges:
  backpack:
    api_key: "YOUR_BACKPACK_API_KEY"
    api_secret: "YOUR_BACKPACK_API_SECRET"
  hyperliquid:
    api_key: "YOUR_HYPERLIQUID_API_KEY_OR_ADDRESS" 
    api_secret: "YOUR_HYPERLIQUID_API_SECRET_OR_PRIVATE_KEY"
```

## 使用方法

1. 启动机器人
```bash
python main.py
```

2. 使用自定义配置文件
```bash
python main.py --config your_config.yaml
```

3. 模拟运行模式（不实际下单）
```bash
python main.py --dry-run
```

## 配置选项

在`config.yaml`中配置策略参数：

```yaml
strategy:
  symbols: ["BTC", "ETH", "SOL"] # 监控的基础币种列表
  max_positions_count: 5 # 全局持仓数量限制，最多同时持有的不同币种数量
  min_funding_diff: 0.000001 # 最小资金费率差阈值
  min_price_diff_percent: 0.000001 # 最小价格差阈值（百分比）
  position_sizes: # 每个币种的单次开仓数量
    BTC: 0.001  # BTC开仓数量
    ETH: 0.01   # ETH开仓数量
    SOL: 0.1    # SOL开仓数量
  max_total_position_usd: 150 # 所有币种的总开仓金额上限（美元）
  check_interval: 5 # 检查套利机会的时间间隔 (秒)
  funding_update_interval: 60 # 更新资金费率的时间间隔 (秒)

# 交易对配置
trading_pairs:
  - symbol: "BTC"
    min_volume: 0.001
    price_precision: 1 # BTC价格精度：1位小数
    size_precision: 3
    max_position_size: 0.001  # BTC最大持仓数量
  - symbol: "ETH"
    min_volume: 0.01
    price_precision: 2
    size_precision: 2 # ETH数量精度：2位小数
    max_position_size: 0.01  # ETH最大持仓数量
  - symbol: "SOL"
    min_volume: 0.1
    price_precision: 3
    size_precision: 2 # SOL数量精度：2位小数
    max_position_size: 0.1  # SOL最大持仓数量
```

## 注意事项

- 请使用小额资金进行测试
- 确保网络稳定，因为套利策略需要同时在两个交易所下单
- 请了解资金费率套利的风险，包括但不限于价格波动风险、流动性风险等

## 风险控制

机器人内置了多项风险控制机制，帮助您限制潜在的风险：

1. **全局持仓数量限制**：通过`strategy.max_positions_count`配置项限制同时最多持有的不同币种数量。例如，设置为5时，即使更多币种满足开仓条件，也最多只持有5个不同币种的仓位。这有助于：
   - 防止资金过度分散
   - 控制整体风险敞口
   - 在市场波动时限制潜在的亏损

2. **单币种持仓限制**：每个币种都有`max_position_size`设置，限制单一币种的最大持仓量。

3. **价格差异过滤**：通过`min_price_diff_percent`和`max_price_diff_percent`过滤异常的价格数据，避免在错误数据基础上开仓。

合理设置这些参数可以有效控制套利策略的风险。

## 项目结构

```
funding_arbitrage_bot/
├── main.py                  # 主程序入口
├── config.yaml              # 配置文件
├── requirements.txt         # Python依赖
├── README.md                # 项目说明文档
├── exchanges/               # 交易所API封装
│   ├── backpack_api.py      # Backpack交易所API
│   └── hyperliquid_api.py   # Hyperliquid交易所API
├── core/                    # 核心逻辑
│   ├── arbitrage_engine.py  # 套利引擎
│   ├── data_manager.py      # 数据管理
│   └── position_manager.py  # 仓位管理
└── utils/                   # 工具函数
    ├── logger.py            # 日志工具
    └── helpers.py           # 辅助函数
```

# Hyperliquid API 修复说明

## 问题

在使用自建的Hyperliquid SDK时，发现以下问题：

1. API请求格式不正确
2. 身份验证签名方式不正确
3. 参数类型问题（浮点数vs字符串）
4. 资金费率获取失败，显示为0

## 解决方案

我们采用了以下解决方案：

1. 使用官方SDK代替自己实现的SDK
2. 修改了API代码，使其正确调用官方SDK
3. 确保所有参数使用正确的格式和类型
4. 直接使用REST API获取资金费率数据

## 具体修改

1. 添加了对官方SDK的支持：
   ```python
   from hyperliquid.exchange import Exchange
   from eth_account import Account
   
   # 创建钱包对象
   wallet = Account.from_key(private_key)
   
   # 初始化Exchange
   exchange = Exchange(wallet=wallet)
   
   # 下单
   order_response = exchange.order(
       coin=symbol,
       is_buy=is_buy,
       sz=size,
       limit_px=price,
       order_type="Limit"  # 限价单
   )
   ```

2. 修复了REST API的使用方式：
   ```python
   # 获取持仓信息
   url = f"{self.base_url}/info"
   payload = {
       "type": "clearinghouseState",
       "user": self.hyperliquid_address
   }
   response = await self.http_client.post(url, json=payload)
   ```

3. 修复资金费率获取：
   ```python
   # 获取资金费率
   url = f"{self.base_url}/info"
   payload = {"type": "metaAndAssetCtxs"}
   
   response = await self.http_client.post(url, json=payload)
   data = response.json()
   
   # 解析资金费率
   if isinstance(data, list) and len(data) >= 2:
       universe_data = data[0]
       asset_ctxs = data[1]
       
       # 查找特定币种
       for i, coin_data in enumerate(universe_data.get("universe", [])):
           if coin_data.get("name") == symbol:
               coin_ctx = asset_ctxs[i]
               if "funding" in coin_ctx:
                   funding_rate = float(coin_ctx["funding"])
                   return funding_rate
   ```

4. 创建了测试脚本验证功能：
   - `test_order.py`: 使用官方SDK直接下单测试
   - `test_hl_api.py`: 通过自己的API接口下单测试
   - `test_funding_rate.py`: 验证资金费率获取功能

## 注意事项

1. Hyperliquid只支持限价单，不支持市价单
2. 下单时需要注意价格精度和数量精度
3. 钱包地址必须与私钥匹配
4. 为了避免意外成交，测试时将价格设置为市场价格的95%（买入）或105%（卖出）
5. 资金费率通过直接调用REST API获取，避免SDK中可能存在的问题

## API使用方式

```python
# 初始化API
api = HyperliquidAPI(
    api_key=wallet_address,   # 钱包地址
    api_secret=private_key,   # 钱包私钥
    logger=logger,
    config=config
)

# 下限价单
order_result = await api.place_order(
    symbol="SOL",
    side="BUY",
    size=0.01,
    price=current_price*0.95,  # 买入价格比市场价低5%
    order_type="LIMIT"
)

# 获取持仓
positions = await api.get_positions()

# 获取资金费率
funding_rate = await api.get_funding_rate("BTC")

# 平仓
await api.close_position(symbol="SOL")
```

# 资金费率获取修复总结

我们已经成功修复了Hyperliquid资金费率获取功能，现在可以正确获取各个交易对的资金费率数据。

## 问题原因

1. 原代码尝试使用官方SDK的`meta_and_asset_ctx`方法获取资金费率，但该方法在当前版本的SDK中不存在
2. REST API请求格式不正确，导致无法获取资金费率数据
3. 对资金费率数据的解析逻辑存在问题

## 解决方案

1. 移除对不存在的SDK方法的调用
2. 采用直接的REST API请求获取资金费率数据
3. 正确解析API响应，提取资金费率信息
4. 增加详细的错误处理和日志记录

## 修复验证

我们创建了专门的测试脚本（`test_funding_rate.py`和`test_detailed_funding.py`）来验证资金费率获取功能：

1. 直接使用REST API获取资金费率
2. 通过修改后的`HyperliquidAPI`类获取资金费率
3. 对比两种方法获取的结果，确保一致性

测试结果显示，修复后的代码可以正确获取Hyperliquid的资金费率数据，不再出现获取失败或返回0的情况。现在资金费率可以用于套利策略的计算。

## 示例资金费率数据

以下是部分交易对当前的资金费率数据（测试时间：2025-04-06）：

| 币种 | 每小时费率 | 8小时费率 | 24小时费率 |
|------|-----------|-----------|-----------|
| BTC  | 0.001250% | 0.010000% | 0.030000% |
| ETH  | -0.000611% | -0.004888% | -0.014664% |
| SOL  | -0.002420% | -0.019357% | -0.058072% |
| AVAX | -0.004687% | -0.037495% | -0.112484% |
| LINK | -0.001299% | -0.010394% | -0.031181% |

请注意，资金费率为正值表示做多需要支付资金费用，做空可以获得资金费用；为负值表示做空需要支付费用，做多可以获得资金费用。套利策略应利用这些差异进行交易。

# 盈亏状态计算修复 (2025-04-11)

## 问题描述

终端界面上持仓的盈亏状态始终显示为"亏损"，即使资金费率数据显示应该是"盈利"状态。日志中显示盈亏计算时使用的资金费率数值均为0，导致计算结果总是为0，被判断为"亏损"状态。

## 问题原因

1. 在向 `check_position_profit_status` 函数传递参数时，使用了可能已经过时的资金费率数据
2. 资金费率数据没有从最新的 market_data 中获取，而是使用了函数调用时的参数值
3. 部分环节中存在空值处理不当，导致 '<' 比较操作时出现 NoneType 错误

## 修复方案

1. 在 `_update_position_direction_info` 方法中，直接从 market_data 中获取最新的资金费率数据进行盈亏计算
2. 添加防御性编程代码，对可能为 None 的值进行检查和处理，避免 NoneType 错误
3. 当资金费率接近0时，将盈亏状态显示为"持平"而不是"亏损"，更准确反映实际情况

## 修复效果

1. 终端界面现在能够正确显示持仓的盈亏状态（"盈利"、"亏损"或"持平"）
2. 日志中记录的资金费率盈亏计算使用了准确的最新资金费率数据
3. 程序运行更加稳定，不再出现NoneType比较错误

这一修复确保了盈亏状态的计算与显示能够准确反映实际的资金费率情况，帮助用户做出更准确的交易决策。 