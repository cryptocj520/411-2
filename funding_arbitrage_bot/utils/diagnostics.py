#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Hyperliquid API诊断工具

用于测试Hyperliquid API连接和资金费率获取，帮助用户诊断显示"na"的问题
"""

import sys
import asyncio
import argparse
import logging
import yaml
import json
import traceback
from pathlib import Path

# 设置日志格式
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('hl_diagnostics')

async def test_hyperliquid_api(config_path: str, symbol: str = "BTC"):
    """
    测试Hyperliquid API连接和资金费率获取
    
    Args:
        config_path: 配置文件路径
        symbol: 要测试的币种
    """
    # 打印诊断头部
    logger.info("=" * 50)
    logger.info("Hyperliquid API连接和资金费率获取诊断工具")
    logger.info("=" * 50)
    
    # 加载配置
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"已成功加载配置文件: {config_path}")
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return
    
    # 提取Hyperliquid API凭证
    hl_api_key = config.get('exchanges', {}).get('hyperliquid', {}).get('api_key')
    hl_api_secret = config.get('exchanges', {}).get('hyperliquid', {}).get('api_secret')
    
    if not hl_api_key or not hl_api_secret:
        logger.error("配置文件中未找到Hyperliquid API凭证")
        return
    
    logger.info(f"已找到Hyperliquid API凭证，钱包地址前缀: {hl_api_key[:10]}...")
    
    # 导入Hyperliquid API
    try:
        from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
        logger.info("已成功导入HyperliquidAPI类")
    except ImportError:
        logger.error("导入HyperliquidAPI类失败，请确保您在正确的目录中运行此脚本")
        import traceback
        logger.debug(f"导入错误详情: {traceback.format_exc()}")
        return
    
    # 创建API实例
    try:
        api = HyperliquidAPI(
            api_key=hl_api_key,
            api_secret=hl_api_secret,
            logger=logger,
            config=config
        )
        logger.info("已成功创建HyperliquidAPI实例")
    except Exception as e:
        logger.error(f"创建HyperliquidAPI实例失败: {e}")
        return
    
    # 测试直接REST API调用
    try:
        import httpx
        import traceback
        client = httpx.AsyncClient(timeout=10.0)
        
        logger.info("尝试直接调用Hyperliquid REST API获取资金费率...")
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "metaAndAssetCtxs"}
        
        response = await client.post(url, json=payload)
        
        if response.status_code == 200:
            logger.info(f"REST API请求成功，状态码: {response.status_code}")
            
            # 解析响应并查找特定币种
            data = response.json()
            
            # 记录响应格式
            logger.debug(f"响应数据类型: {type(data)}")
            if isinstance(data, dict):
                logger.debug(f"响应数据键: {data.keys()}")
            elif isinstance(data, list):
                logger.debug(f"响应数据是列表，长度: {len(data)}")
                if len(data) > 0:
                    logger.debug(f"第一项类型: {type(data[0])}")
                    if isinstance(data[0], dict):
                        logger.debug(f"第一项键: {data[0].keys()}")
            
            # 尝试以不同格式解析
            universe = []
            asset_ctxs = []
            
            # 检查数据是否为字典类型，有universe和assetCtxs键
            if isinstance(data, dict) and "universe" in data and "assetCtxs" in data:
                universe = data["universe"]
                asset_ctxs = data["assetCtxs"]
                logger.info("使用字典格式解析API响应")
            # 检查数据是否为列表类型，且有两个元素
            elif isinstance(data, list) and len(data) >= 2:
                # 假设第一个元素是universe，第二个元素是assetCtxs
                if isinstance(data[0], list):
                    universe = data[0]
                if len(data) > 1 and isinstance(data[1], list):
                    asset_ctxs = data[1]
                logger.info("使用列表格式解析API响应")
            
            logger.info(f"获取到{len(universe)}个币种信息")
            
            # 查找特定币种
            symbol_idx = None
            for i, coin in enumerate(universe):
                coin_name = None
                if isinstance(coin, dict) and "name" in coin:
                    coin_name = coin.get("name")
                elif isinstance(coin, str):
                    coin_name = coin
                
                if coin_name == symbol:
                    symbol_idx = i
                    break
            
            if symbol_idx is not None and symbol_idx < len(asset_ctxs):
                asset_ctx = asset_ctxs[symbol_idx]
                logger.info(f"找到{symbol}的资产上下文: {json.dumps(asset_ctx, indent=2)}")
                
                # 检查是否存在资金费率字段
                if isinstance(asset_ctx, dict) and "funding" in asset_ctx:
                    logger.info(f"{symbol}的资金费率: {asset_ctx['funding']}")
                else:
                    logger.warning(f"{symbol}的资产上下文中不存在资金费率字段")
            else:
                logger.warning(f"在API响应中未找到{symbol}的资产上下文")
                
            # 打印所有币种名称以供参考
            all_coins = []
            for coin in universe:
                if isinstance(coin, dict) and "name" in coin:
                    all_coins.append(coin.get("name"))
                elif isinstance(coin, str):
                    all_coins.append(coin)
            
            logger.info(f"所有可用币种: {', '.join(all_coins)}")
        else:
            logger.error(f"REST API请求失败，状态码: {response.status_code}")
        
        await client.aclose()
    except Exception as e:
        logger.error(f"直接调用REST API失败: {e}")
        import traceback
        logger.debug(f"错误详情: {traceback.format_exc()}")
    
    # 测试SDK方法
    try:
        logger.info(f"尝试通过SDK获取{symbol}的资金费率...")
        
        funding_rate = await api.get_funding_rate(symbol)
        
        if funding_rate is not None:
            logger.info(f"成功获取到{symbol}的资金费率: {funding_rate}")
        else:
            logger.warning(f"获取{symbol}的资金费率失败，返回None")
    except Exception as e:
        logger.error(f"通过SDK获取资金费率失败: {e}")
        logger.debug(f"错误详情: {traceback.format_exc()}")
    
    # 测试获取价格
    try:
        logger.info(f"尝试获取{symbol}的价格...")
        
        # 先启动WebSocket连接
        logger.info("启动WebSocket价格连接...")
        await api.start_ws_price_stream()
        
        # 等待一段时间让WebSocket连接建立并接收数据
        logger.info("等待5秒以接收价格数据...")
        await asyncio.sleep(5)
        
        # 获取价格
        price = await api.get_price(symbol)
        
        if price is not None:
            logger.info(f"成功获取到{symbol}的价格: {price}")
        else:
            logger.warning(f"获取{symbol}的价格失败，返回None")
    except Exception as e:
        logger.error(f"获取价格失败: {e}")
        logger.debug(f"错误详情: {traceback.format_exc()}")
    
    # 清理资源
    try:
        await api.close()
    except Exception as e:
        logger.error(f"关闭API连接失败: {e}")
    
    logger.info("=" * 50)
    logger.info("诊断完成")
    logger.info("=" * 50)

def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='Hyperliquid API诊断工具')
    parser.add_argument('--config', type=str, default='funding_arbitrage_bot/config.yaml',
                        help='配置文件路径')
    parser.add_argument('--symbol', type=str, default='BTC',
                        help='要测试的币种')
    
    args = parser.parse_args()
    
    # 检查配置文件是否存在
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"错误: 配置文件 {args.config} 不存在")
        sys.exit(1)
    
    # 运行异步测试函数
    asyncio.run(test_hyperliquid_api(args.config, args.symbol))

if __name__ == '__main__':
    main() 