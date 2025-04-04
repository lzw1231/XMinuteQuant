import tqsdk, datetime, os, shutil, json
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
import tomllib
from pathlib import Path


# 获取K线的方法
def kline_get(api: tqsdk.TqApi, symbol_name, n):  # 初始化kline和signal
    kline_data = api.get_kline_serial(symbol_name, 60 * 60 * 24, data_length=n)
    kline_data['candle_begin_time'] = pd.to_datetime(kline_data['datetime'], unit='ns')
    kline_data['candle_begin_time_GMT8'] = kline_data['candle_begin_time'] + datetime.timedelta(hours=8)
    kline_data['trade_time'] = kline_data['candle_begin_time_GMT8'].astype('str').str[-8:]
    return kline_data


# 清空destination_folder(目标文件夹)，将source_folder（源文件夹）的内容复制过去
def copy_directory(destination_folder, source_folder):
    try:
        # 如果目标路径存在，先删除（根据需求选择是否保留原始数据）
        if os.path.exists(destination_folder):
            shutil.rmtree(destination_folder)
            print(f"文件夹已清空： {destination_folder} ")
        # 执行复制并重命名
        shutil.copytree(source_folder, destination_folder)
        print(f"源文件夹复制成功：{source_folder} -> {destination_folder}")
    except FileNotFoundError:
        print(f"错误：源文件夹不存在： {source_folder} ")
    except PermissionError:
        print("权限错误：请检查文件夹访问权限。")
    except Exception as e:
        print(f"未知错误：{str(e)}")


# 根据品种代号，获取交易所，交易品种名称
def get_exchange_symbol_cn(dict, a):
    value = dict.get(a, (None, None))  # 默认值可自定义
    return value[0], value[1]


# 构建字典，从合约代码到（交易所名称，品种名称）的映射
def build_contract_map(path=r'./utils/dominant_contract.json'):
    # 注意下面的路径，是main的目录出发
    with open(path, 'r', encoding='utf-8') as f:
        zl_js = json.load(f)
    contract_map = {}
    for exchange, contracts in zl_js.items():
        # contracts 是字典，键是品种名称，值是合约代码
        for name, code in contracts.items():
            contract_map[code] = (exchange, name)
    return contract_map


# 把df中‘交易日期’这一列的数据类型改为pa.date32
def convert_to_date32(table: pa.Table) -> pa.Table:
    # 使用pyarrow.compute.cast进行类型转换
    casted_date = pc.cast(table['交易日期'], pa.date32())
    # 替换原列并返回新Table
    return table.set_column(
        table.column_names.index('交易日期'),
        '交易日期',
        casted_date
    )


# 加载配置的超简版本
def load_para_config(file=r'./utils/para_config.toml'):
    with open(file, "rb") as f:
        conf = tomllib.load(f)

    # 直接返回字典（最简方案）
    return {
        "days": conf["download"]["days"],
        "user": conf["credentials"]["username"],
        "pwd": conf["credentials"]["password"],
        "temp": Path(conf["paths"]["path_temp"]),
        "historical": Path(conf["paths"]["path_historical"]),
        "latest": Path(conf["paths"]["path_latest"])
    }
