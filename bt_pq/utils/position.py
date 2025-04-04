# 由交易信号产生实际持仓，模式next是产生交易信号后下一个K线成交
def next(df):
    # 拷贝一个副本，不修改传进来的df
    df = df.copy()

    # ===由signal计算出实际的每天持有仓位
    # 在产生signal的k线结束的时候，进行买入

    # 从上往下扫描，如果此列本行为NaN,将上一行的值填进来
    df['signal'] = df['signal'].ffill()
    # 这个是处理初始几行的空值，将它们设为0
    df['signal'] = df['signal'].fillna(value=0)

    # 将signal的值整体下移一行，赋给position_side,持仓方向。因为是产生信号后的下一根K线才进行开仓
    df['position_side'] = df['signal'].shift(1)
    # 显然,position第一行的值是空，让它等于0
    df['position_side'] = df['position_side'].fillna(value=0)

    # print(df[['交易日期', 't_signal', 'signal', 'position']])
    return df


# 由交易信号产生实际持仓，模式now是产生交易信号后,立即成交
def instant(df):
    # 拷贝一个副本，不修改传进来的df
    df = df.copy()

    # 从上往下扫描，如果此列本行为NaN,将上一行的值填进来
    df['signal'] = df['signal'].ffill()
    # 这个是处理初始几行的空值，将它们设为0
    df['signal'] = df['signal'].fillna(value=0)

    # 将signal的值整体赋给position
    df['position_side'] = df['signal']

    return df
