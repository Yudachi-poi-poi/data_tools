# data_tools
script for parser data

QMT DAT数据解析脚本
# DAT文件解析器使用说明

## 📁 文件说明

**主要文件**: `dat_parser_final.py` - 精简版DAT解析器

## 📊 输出字段

解析后的CSV文件包含以下9个字段：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | str | 股票代码 | "000001" |
| date | str | 交易日期 | "2018-11-16" |
| open | float | 开盘价(元) | 10.61 |
| high | float | 最高价(元) | 10.72 |
| low | float | 最低价(元) | 10.49 |
| close | float | 收盘价(元) | 10.57 |
| volume | int | 成交量(股) | 1059592 |
| amount | float | 成交金额(元) | 11231732.19 |
| pct_change | float | 涨跌幅(%) | 2.65 |

## 🚀 使用方法

### 1. 解析单个股票文件

```python
from dat_parser_final import parse_single_stock

# 解析单个DAT文件
df = parse_single_stock('data/szdayK/000001.DAT', 'output/000001.csv')
print(f"解析了 {len(df)} 条记录")
```

### 2. 使用解析器类

```python
from data_parser_final import DATParser

parser = DATParser()

# 解析单个文件
df = parser.parse_dat_file('data/szdayK/000001.DAT')

# 解析整个目录
all_df = parser.parse_directory('data/szdayK', 'output')
```

### 3. 批量解析所有股票

```python
from dat_parser_final import parse_all_stocks

# 解析所有股票数据
parse_all_stocks('data', 'output')
```


