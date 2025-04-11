#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
资金费率符号记录管理工具

用于查看、添加、修改或删除资金费率符号记录
"""

import os
import json
import argparse
from typing import Dict, Optional, List

class FundingSignsManager:
    """资金费率符号记录管理器"""
    
    def __init__(self, file_path: Optional[str] = None):
        """
        初始化管理器
        
        Args:
            file_path: 资金费率符号记录文件路径，如果为None则使用默认路径
        """
        if file_path is None:
            # 获取项目根目录
            root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.file_path = os.path.join(root_dir, 'data', 'funding_diff_signs.json')
        else:
            self.file_path = file_path
            
        # 确保data目录存在
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
    def load_signs(self) -> Dict[str, int]:
        """
        加载资金费率符号记录
        
        Returns:
            Dict[str, int]: 资金费率符号记录字典
        """
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r') as f:
                    signs_data = json.load(f)
                    # 确保符号值是整数类型
                    return {symbol: int(sign) for symbol, sign in signs_data.items()}
            return {}
        except Exception as e:
            print(f"加载资金费率符号记录文件失败: {e}")
            return {}
            
    def save_signs(self, signs: Dict[str, int]) -> bool:
        """
        保存资金费率符号记录
        
        Args:
            signs: 资金费率符号记录字典
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with open(self.file_path, 'w') as f:
                json.dump(signs, f, indent=2)
            print(f"资金费率符号记录已保存到文件: {self.file_path}")
            return True
        except Exception as e:
            print(f"保存资金费率符号记录到文件失败: {e}")
            return False
            
    def list_signs(self) -> None:
        """列出所有资金费率符号记录"""
        signs = self.load_signs()
        if not signs:
            print("没有资金费率符号记录")
            return
            
        print("\n当前资金费率符号记录:")
        print("-" * 30)
        print(f"{'币种':<10} {'符号':<10} {'含义':<10}")
        print("-" * 30)
        
        for symbol, sign in signs.items():
            meaning = "正差异(BP>HL)" if sign == 1 else "负差异(BP<HL)"
            print(f"{symbol:<10} {sign:<10} {meaning:<10}")
            
        print("-" * 30)
        
    def add_sign(self, symbol: str, sign: int) -> bool:
        """
        添加或修改资金费率符号记录
        
        Args:
            symbol: 币种符号，如 "BTC"
            sign: 符号值，1表示正差异，-1表示负差异
            
        Returns:
            bool: 操作是否成功
        """
        if sign not in (1, -1):
            print("错误: 符号值必须为1或-1")
            return False
            
        signs = self.load_signs()
        action = "修改" if symbol in signs else "添加"
        signs[symbol] = sign
        
        if self.save_signs(signs):
            print(f"成功{action}{symbol}的资金费率符号为: {sign}")
            return True
        return False
        
    def delete_sign(self, symbol: str) -> bool:
        """
        删除资金费率符号记录
        
        Args:
            symbol: 币种符号，如 "BTC"
            
        Returns:
            bool: 操作是否成功
        """
        signs = self.load_signs()
        if symbol not in signs:
            print(f"警告: {symbol}不存在于资金费率符号记录中")
            return False
            
        del signs[symbol]
        
        if self.save_signs(signs):
            print(f"成功删除{symbol}的资金费率符号记录")
            return True
        return False
        
    def clear_signs(self) -> bool:
        """
        清空所有资金费率符号记录
        
        Returns:
            bool: 操作是否成功
        """
        return self.save_signs({})
        
def main():
    """命令行工具主函数"""
    parser = argparse.ArgumentParser(description="资金费率符号记录管理工具")
    
    # 创建子命令
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # 列出所有记录
    list_parser = subparsers.add_parser("list", help="列出所有资金费率符号记录")
    
    # 添加或修改记录
    add_parser = subparsers.add_parser("add", help="添加或修改资金费率符号记录")
    add_parser.add_argument("symbol", help="币种符号，如 BTC")
    add_parser.add_argument("sign", type=int, choices=[1, -1], help="符号值: 1表示正差异，-1表示负差异")
    
    # 删除记录
    delete_parser = subparsers.add_parser("delete", help="删除资金费率符号记录")
    delete_parser.add_argument("symbol", help="币种符号，如 BTC")
    
    # 清空所有记录
    clear_parser = subparsers.add_parser("clear", help="清空所有资金费率符号记录")
    
    # 文件路径参数（可选）
    parser.add_argument("--file", "-f", help="资金费率符号记录文件路径")
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = FundingSignsManager(args.file)
    
    # 执行对应命令
    if args.command == "list":
        manager.list_signs()
    elif args.command == "add":
        manager.add_sign(args.symbol, args.sign)
    elif args.command == "delete":
        manager.delete_sign(args.symbol)
    elif args.command == "clear":
        confirm = input("确定要清空所有资金费率符号记录吗？(y/n): ")
        if confirm.lower() == 'y':
            manager.clear_signs()
    else:
        parser.print_help()
        
if __name__ == "__main__":
    main() 