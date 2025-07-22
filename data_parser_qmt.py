#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票DAT文件解析器 - 精简版
只保留核心字段：code, date, open, high, low, close, volume, amount, pct_change
"""

import os
import struct
import pandas as pd
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class DATParser:
    """DAT文件解析器 - 精简版"""
    
    def __init__(self):
        self.record_size = 32  # 每条记录32字节
        
    def parse_date(self, date_int, include_time=False):
        """
        解析Unix时间戳为日期时间格式

        Args:
            date_int: Unix时间戳
            include_time: 是否包含时间信息（用于分钟级数据）

        Returns:
            str: 格式化的日期时间字符串
        """
        try:
            if date_int > 1500000000 and date_int < 2000000000:  # Unix时间戳
                target_date = datetime.fromtimestamp(date_int)
                if include_time:
                    # 分钟级数据需要精确到分钟
                    return target_date.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # 日线数据只需要日期
                    return target_date.strftime('%Y-%m-%d')
            else:
                return str(date_int)
        except:
            return str(date_int)
    
    def _detect_data_type(self, file_path):
        """检测数据类型（日线或分钟线）"""
        path_str = str(file_path).lower()
        # 根据路径判断：包含5m、min等关键字的是分钟数据
        return '5m' in path_str or 'min' in path_str

    def parse_dat_file(self, file_path):
        """
        解析单个DAT文件（自动检测日线或分钟线）

        Args:
            file_path: DAT文件路径

        Returns:
            pandas.DataFrame: 包含股票数据的DataFrame
        """
        try:
            with open(file_path, 'rb') as f:
                data = f.read()

            record_count = len(data) // self.record_size
            if record_count == 0:
                return pd.DataFrame()

            # 检测数据类型
            is_intraday = self._detect_data_type(file_path)

            # 解析数据 - 每两条记录为一组（价格记录 + 成交量记录）
            records = []
            for i in range(0, record_count - 1, 2):
                # 价格记录（奇数记录）
                price_pos = i * self.record_size
                price_data = data[price_pos:price_pos + self.record_size]

                # 成交量记录（偶数记录）
                volume_pos = (i + 1) * self.record_size
                volume_data = data[volume_pos:volume_pos + self.record_size]

                if len(price_data) < self.record_size or len(volume_data) < self.record_size:
                    break

                try:
                    # 解包价格记录和成交量记录
                    price_unpacked = struct.unpack('<8I', price_data)
                    volume_unpacked = struct.unpack('<8I', volume_data)

                    # 检查价格记录是否有效（有时间戳）
                    if price_unpacked[2] > 1500000000 and price_unpacked[2] < 2000000000:
                        date_int = price_unpacked[2]

                        # 价格数据在位置3-6，需要除以1000
                        price_limit = 10000000 if is_intraday else 1000000  # 分钟数据价格范围更大
                        if len([x for x in price_unpacked[3:7] if x > 0 and x < price_limit]) >= 3:
                            open_price = price_unpacked[3] / 1000.0
                            high_price = price_unpacked[4] / 1000.0
                            low_price = price_unpacked[5] / 1000.0
                            close_price = price_unpacked[6] / 1000.0

                            # 从成交量记录中提取成交量和成交金额
                            if is_intraday:
                                # 分钟数据：成交量不乘100，成交金额可能需要除以100
                                volume = volume_unpacked[0]
                                raw_amount = volume_unpacked[2]
                                amount = raw_amount / 100.0 if raw_amount > 1000000 else raw_amount
                            else:
                                # 日线数据：成交量乘100
                                volume = volume_unpacked[0] * 100
                                raw_amount = volume_unpacked[2]
                                amount = raw_amount

                            # 数据质量检查
                            if (all(p == 0 for p in [open_price, high_price, low_price, close_price]) or
                                any(p < 0 for p in [open_price, high_price, low_price, close_price])):
                                continue

                            # 价格逻辑检查
                            if (high_price < low_price or
                                high_price < max(open_price, close_price) or
                                low_price > min(open_price, close_price)):
                                continue

                            # 根据数据类型选择时间字段名和格式
                            time_field = 'datetime' if is_intraday else 'date'
                            records.append({
                                time_field: self.parse_date(date_int, include_time=is_intraday),
                                'open': round(open_price, 2),
                                'high': round(high_price, 2),
                                'low': round(low_price, 2),
                                'close': round(close_price, 2),
                                'volume': volume,
                                'amount': round(amount, 2)
                            })

                except struct.error:
                    continue

            if not records:
                return pd.DataFrame()

            # 创建DataFrame
            df = pd.DataFrame(records)

            # 获取股票代码（从文件名提取）
            stock_code = Path(file_path).stem
            df['code'] = stock_code

            # 按时间排序
            time_field = 'datetime' if is_intraday else 'date'
            df = df.sort_values(time_field).reset_index(drop=True)

            # 计算涨跌幅
            df['pct_change'] = df['close'].pct_change() * 100

            # 重新排列列顺序
            df = df[['code', time_field, 'open', 'high', 'low', 'close', 'volume', 'amount', 'pct_change']]

            return df

        except Exception as e:
            print(f"解析文件 {file_path} 时出错: {e}")
            return pd.DataFrame()
    
    def parse_directory(self, directory_path, output_dir='output'):
        """
        解析目录中的所有DAT文件
        
        Args:
            directory_path: 包含DAT文件的目录路径
            output_dir: 输出目录
        """
        directory_path = Path(directory_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        dat_files = list(directory_path.glob('*.DAT'))
        
        if not dat_files:
            print(f"在目录 {directory_path} 中未找到DAT文件")
            return pd.DataFrame()
        
        print(f"找到 {len(dat_files)} 个DAT文件")
        
        all_data = []
        successful_count = 0
        
        for i, dat_file in enumerate(dat_files, 1):
            if i % 100 == 0:  # 每100个文件显示进度
                print(f"正在处理 ({i}/{len(dat_files)}): {dat_file.name}")
            
            df = self.parse_dat_file(dat_file)
            
            if not df.empty:
                all_data.append(df)
                successful_count += 1
        
        print(f"成功解析 {successful_count} 个文件")
        
        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 保存合并后的数据
            combined_file = output_dir / "all_stocks_data.csv"
            combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
            
            print(f"合并数据已保存到: {combined_file}")
            print(f"总共包含 {len(combined_df)} 条记录，{combined_df['code'].nunique()} 只股票")
            
            return combined_df
        
        return pd.DataFrame()


def parse_single_stock(dat_file_path, output_file=None):
    """
    解析单个股票文件的便捷函数
    
    Args:
        dat_file_path: DAT文件路径
        output_file: 输出CSV文件路径（可选）
    
    Returns:
        pandas.DataFrame: 股票数据
    """
    parser = DATParser()
    df = parser.parse_dat_file(dat_file_path)
    
    if not df.empty and output_file:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"数据已保存到: {output_file}")
    
    return df


def parse_all_stocks(data_dir='data', output_dir='output'):
    """
    解析所有股票数据的便捷函数

    Args:
        data_dir: 数据目录
        output_dir: 输出目录
    """
    parser = DATParser()

    # 解析深圳日线股票
    sz_dir = Path(data_dir) / 'szdayK'
    if sz_dir.exists():
        print("解析深圳日线股票数据...")
        parser.parse_directory(sz_dir, Path(output_dir) / 'shenzhen_daily')

    # 解析上海日线股票
    sh_dir = Path(data_dir) / 'shdayK'
    if sh_dir.exists():
        print("解析上海日线股票数据...")
        parser.parse_directory(sh_dir, Path(output_dir) / 'shanghai_daily')

    # 解析5分钟K线数据
    sh5m_dir = Path(data_dir) / 'sh5mK'
    if sh5m_dir.exists():
        print("解析上海5分钟K线数据...")
        parser.parse_directory(sh5m_dir, Path(output_dir) / 'shanghai_5min')


if __name__ == "__main__":
    # 使用示例
    
    # 1. 解析单个股票
    print("解析单个股票示例:")
    df = parse_single_stock('data/szdayK/000001.DAT', 'output/000001_final.csv')
    if not df.empty:
        print(f"解析成功: {len(df)} 条记录")
        print("数据预览:")
        print(df.head())
        print(f"\n数据字段: {list(df.columns)}")
    
    # 2. 解析所有股票（可选，取消注释运行）
    # print("\n解析所有股票:")
    # parse_all_stocks()
