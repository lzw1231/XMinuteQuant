import pandas as pd
import tomllib  # Python 3.11+ 内置库，3.11以下需安装 tomli
from tabulate import tabulate

def load_pd_config(config_path="./utils/pd.toml"):
    """从 TOML 文件加载 Pandas 显示配置"""
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
        display_config = config["pandas"]["display"]

    # 设置所有显示选项
    pd.set_option('display.max_rows', display_config["max_rows"])
    pd.set_option('display.min_rows', display_config["min_rows"])
    pd.set_option('display.max_columns', display_config["max_columns"])
    pd.set_option('display.width', display_config["width"])
    pd.set_option('display.max_colwidth', display_config["max_colwidth"])
    pd.set_option('display.unicode.ambiguous_as_wide', display_config["ambiguous_as_wide"])
    pd.set_option('display.unicode.east_asian_width', display_config["east_asian_width"])
    pd.set_option('expand_frame_repr', display_config["expand_frame_repr"])


# 载入期货商品配置文件
def load_future_config(config_path="./utils/future.toml"):
    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print(f"❌ 错误：配置文件 {config_path} 不存在")
        return None
    except tomllib.TOMLDecodeError as e:
        print(f"❌ TOML 格式错误：{e}")
        return None
    except Exception as e:
        print(f"❌ 未知错误：{e}")
        return None

    try:
        # 确保所有必要键存在
        required_keys = [
            'date_start', 'date_end', 'commodity',
            'initial_cash', 'invest_ratio', 'slippage',
            'c_rate', 'invest_margin_ratio', 'min_margin_ratio',
            'volume_per_lot', 'trade_mode'
        ]
        for key in required_keys:
            if key not in config:
                raise KeyError(f"缺少必要配置项: {key}")

        date_start = pd.to_datetime(config["date_start"])
        date_end = pd.to_datetime(config["date_end"])

        return {
            'date_start': date_start,
            'date_end': date_end,
            'commodity': config["commodity"],
            'initial_cash': config["initial_cash"],
            'invest_ratio': config["invest_ratio"],
            'slippage': config["slippage"],
            'c_rate': config["c_rate"],
            'invest_margin_ratio': config["invest_margin_ratio"],
            'min_margin_ratio': config["min_margin_ratio"],
            'volume_per_lot': config["volume_per_lot"],
            'trade_mode': config["trade_mode"]
        }
    except KeyError as e:
        print(f"❌ 配置项错误: {e}")
        return None
    except Exception as e:
        print(f"❌ 处理配置时发生错误: {e}")
        return None


def myprint(df: pd.DataFrame)->None:
    # 处理所有datetime类型的列，无需循环
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    if not datetime_cols.empty:
        df = df.assign(**{col: df[col].dt.strftime('%Y-%m-%d') for col in datetime_cols})

    # 设置对齐方式为右对齐，包括索引列
    alignment = ['right'] * (len(df.columns) + 1)
    print(tabulate(df, headers='keys', tablefmt='github', showindex=True, colalign=alignment))