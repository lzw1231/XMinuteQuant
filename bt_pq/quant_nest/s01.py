# 60日单均线策略，上穿买入，下穿卖出
import pandas as pd


def s01(df: pd.DataFrame, n=60):
    df = df.copy()

    ma = int(n)

    # 找出上穿的k线，将其'signal'设为1，做多
    c1 = df['收盘价'] >= df[f'MA{n}']
    c2 = df['收盘价'].shift(1) < df[f'MA{n}'].shift(1)
    df.loc[c1 & c2, 'signal'] = 1
    # print((c1 & c2).sum())
    # print(df)

    # 找出下穿穿的k线，将其'signal'设为-1，做空
    c1 = df['收盘价'] <= df[f'MA{n}']  # 当前K线的收盘价 < 中轨
    c2 = df['收盘价'].shift(1) > df[f'MA{n}'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[c1 & c2, 'signal'] = -1  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 增加一个临时列，便于后续操作观察
    df['t_signal'] = df['signal']
    # 增加一个临时列，便于后续操作观察
    df['t_open'] = df['开盘价']

    # columns = ['交易日期', '开盘价', '收盘价', 't_open', 't_signal', 'signal']

    columns = ['交易日期', '开盘价', '收盘价', '最高价', '最低价', 't_signal', 'signal']

    return df[columns].copy()
