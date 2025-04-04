import pandas as pd
from tabulate import tabulate
import numpy as np
from .methods import myprint


# 计算每日涨跌幅，MA，bias，截取交易日期
def calc_ma_bias(df: pd.DataFrame, date_start: pd.Timestamp = None, date_end: pd.Timestamp = None, ma_list: list = [5, 10, 20, 30, 60, 120, 250]) -> pd.DataFrame:
    df = df.copy()

    # 计算涨跌幅
    df['涨跌幅'] = df['收盘价'].pct_change(1)

    # 计算MA和BIAS指标
    for n in ma_list:
        ma_col = f'MA{n}'
        bias_col = f'bias{n}'
        df[ma_col] = df['收盘价'].rolling(n).mean()
        df[bias_col] = (df['收盘价'] - df[ma_col]) / df[ma_col]

    # 处理交易日期
    # 若原始数据中存在无法解析的日期（如 "2023-02-30"、"未知"、空值或格式混乱），通过errors = 'coerce'
    # 将其转换为NaT（Not - a - Time，时间数据中的缺失值），避免抛出错误导致程序中断。
    df['交易日期'] = pd.to_datetime(df['交易日期'], errors='coerce')

    # 按日期范围筛选
    if date_start is not None:
        df = df[df["交易日期"] >= date_start]
    if date_end is not None:
        df = df[df["交易日期"] <= date_end]

    # 重新生成索引
    return df.reset_index(drop=True)


# 计算账户净值曲线
def equity_curve(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    # copy，避免修改传进来的参数,改个英文名字，打印出来能对齐
    df = df.copy().rename(columns={'交易日期': 'trade_time', '开盘价': 'open', '收盘价': 'close', '最高价': 'high', '最低价': 'low'})

    # 读取配置文件里的参数
    initial_cash = cfg['initial_cash']
    invest_ratio = cfg['invest_ratio']
    slippage = cfg['slippage']
    c_rate = cfg['c_rate']
    invest_margin_ratio = cfg['invest_margin_ratio']
    min_margin_ratio = cfg['min_margin_ratio']
    volume_per_lot = cfg['volume_per_lot']
    trade_mode = cfg['trade_mode']

    # 公共条件：当前持仓非空（只需计算一次）
    current_pos_non_zero = df['position_side'] != 0
    # 开仓条件：当前持仓变化（与上一周期不同）
    open_pos_condition = current_pos_non_zero & df['position_side'].ne(df['position_side'].shift(1))
    print(type(open_pos_condition))
    # 平仓条件：即将发生持仓变化（与下一周期不同）
    close_pos_condition = current_pos_non_zero & df['position_side'].ne(df['position_side'].shift(-1))

    # =====对每次交易进行分组
    # 符合开仓的，把‘交易日期’这列复制给‘start_time’这一列
    df.loc[open_pos_condition, 'start_time'] = df['trade_time']
    # 填充‘start_time’列
    df['start_time'] = df['start_time'].ffill()
    # 仓位为0的，‘start_time’这列日期=pd.NaT
    df.loc[df['position_side'] == 0, 'start_time'] = pd.NaT

    # 判断交易模式
    if trade_mode == "NEXT":
        # ===== 计算开仓，平仓价格
        # 开仓价()=出现交易信号的第二根K线的，开盘价
        # 平仓价=出现交易信号的第二根K线的，开盘价
        df.loc[open_pos_condition, 'signal_entry_price'] = df['open']
        df.loc[close_pos_condition, 'signal_exit_price'] = df['open'].shift(-1)

        # ===== 计算资金曲线
        invest_cash = initial_cash * invest_ratio

        # ===在开仓时
        # 在open_pos_condition的K线，以ma均价计算买入合约的数量。（当资金量大的时候，可以提高5跳的价格买入）
        df.loc[open_pos_condition, 'contract_num'] = invest_cash / (volume_per_lot * (df['signal_entry_price'] + slippage * df['position_side']) * invest_margin_ratio)
        df['contract_num'] = np.floor(df['contract_num'])  # 对合约张数向下取整

        # 加上滑点的开仓价格
        df.loc[open_pos_condition, 'entry_price'] = (df['signal_entry_price'] + slippage * df['position_side'])

        # 开仓之后， cash = 可用现金+持仓市值-开仓手续费
        df['cash'] = initial_cash - df['entry_price'] * volume_per_lot * df['contract_num'] * c_rate

        # 管理开仓后的持仓状态数据，确保在持仓期间关键信息（合约数量、开仓价、现金）保持不变，并在平仓时重置这些值
        for column in ['contract_num', 'entry_price', 'cash']:
            df.loc[df['position_side'] != 0, column] = df[column].ffill()

        # 平仓后，清除合约数量、开仓价、现金的数值
        df.loc[df['position_side'] == 0, ['contract_num', 'entry_price', 'cash']] = None

        # ========平仓时
        # 平仓价格，增加滑点影响，position_side=±1，无需区分多空，直接利用持仓方向（正/负）自动调整滑点的影响方向，确保公式适用于所有情况
        df.loc[close_pos_condition, 'exit_price'] = (df['signal_exit_price'] - slippage * df['position_side'])
        # 平仓手续费
        df.loc[close_pos_condition, 'exit_fee'] = df['exit_price'] * volume_per_lot * df['contract_num'] * c_rate

        # ==========计算利润
        # 开仓至今持仓盈亏,净值波动
        df['profit'] = volume_per_lot * df['contract_num'] * (df['close'] - df['entry_price']) * df['position_side']
        # 平仓时理论额外处理,手续费扣除在后面的net_value中处理
        df.loc[close_pos_condition, 'profit'] = volume_per_lot * df['contract_num'] * (df['exit_price'] - df['entry_price']) * df['position_side']
        # 账户净值
        df['net_value'] = df['cash'] + df['profit']

        # ====计算爆仓
        # 至今持仓盈亏最小值
        df.loc[df['position_side'] == 1, 'price_min'] = df['low']
        df.loc[df['position_side'] == -1, 'price_min'] = df['high']
        df['profit_min'] = volume_per_lot * df['contract_num'] * (df['price_min'] - df['entry_price']) * df['position_side']

        # 账户净值最小值
        df['net_value_min'] = df['cash'] + df['profit_min']

        # 计算保证金比例，当持仓保证金小于18%的时候即为爆仓，这是交易所规定的，目前18%为保守计算
        # 保证金比例 = （保证金金额 / 合约总价值） × 100%
        df['margin_ratio'] = df['net_value_min'] / (volume_per_lot * df['contract_num'] * df['price_min'])

        # 计算是否爆仓
        df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), 'is_liquidated'] = 1

        # ===平仓时扣除手续费
        df.loc[close_pos_condition, 'net_value'] -= df['exit_fee']
        # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。
        df.loc[close_pos_condition & (df['net_value'] < 0), 'is_liquidated'] = 1

        # ===对爆仓进行处理
        df['is_liquidated'] = df.groupby('start_time')['is_liquidated'].ffill()
        df.loc[df['is_liquidated'] == 1, 'net_value'] = 0

        # ==== 计算资金曲线 ====
        # 初始化收益率列
        df['equity_change'] = 0.0

        # 处理开仓日收益率
        if df.index[0] in df[open_pos_condition].index:
            # 首日开仓，直接以 initial_cash 为基准
            df.loc[open_pos_condition, 'equity_change'] = df['net_value'] / initial_cash - 1
        else:
            # 非首日开仓，用前一日净值计算收益率
            prev_net_value = df['net_value'].shift(1)
            df.loc[open_pos_condition, 'equity_change'] = (df['net_value'] / prev_net_value) - 1

        # 非开仓日用 pct_change() 计算（禁用填充）
        non_open_mask = ~df.index.isin(df[open_pos_condition].index)
        df.loc[non_open_mask, 'equity_change'] = df['net_value'].pct_change(fill_method=None)  # 修改点

        # 填充缺失值（首日非开仓日或中间缺失）
        df['equity_change'] = df['equity_change'].fillna(0)  # 确保无 NaN

        # 生成资金曲线
        df['equity_curve'] = (1 + df['equity_change']).cumprod()

        # myprint(df)


    elif trade_mode == "INSTANTLY":
        pass

    return df
