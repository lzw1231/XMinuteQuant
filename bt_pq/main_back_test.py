import utils, quant_nest
import pandas as pd
import utils

# 载入pd配置文件
utils.load_pd_config()

# 载入期货商品配置文件，f表示future
f_cfg = utils.load_future_config()

# 导入期货商品数据
df = pd.read_parquet(fr"../db_pq/latest/{f_cfg['commodity']}.parquet")

# 计算 MA ,bias , MA = [5,10,20,30,60,120,250]
df_ma_bias = utils.calc_ma_bias(df, f_cfg['date_start'], f_cfg['date_end'])

# ['交易日期', '交易所', '主连名称', '合约代码', '开盘价', '最高价', '最低价', '收盘价', '成交量', '涨跌幅', 'MA5', 'bias5', 'MA10', 'bias10', 'MA20', 'bias20', 'MA30', 'bias30', 'MA60', 'bias60', 'MA120', 'bias120', 'MA250', 'bias250']
# print(df_ma_bias.columns)

# ************* 最重要！核心！ 运行策略，计算开平仓信号  *******************
df_signal = quant_nest.s01(df_ma_bias)
# ['交易日期', '开盘价', 't_signal', 'signal']
# print(df_signal.columns)
# *********************************************************************

# 增加pos列，表示持仓情况。 用三元表达式判断交易模式
df_pos = (utils.next(df_signal) if f_cfg['trade_mode'] == 'NEXT' else utils.instant(df_signal))
# ['交易日期', '开盘价', 't_signal', 'signal', 'position']
# print(df_pos.columns)

# 计算账户净值曲线
df_equity = utils.equity_curve(df_pos, f_cfg)

# 评价策略
df_evaluate = utils.evaluate_strategy(df_equity)

print(df_evaluate.T)
# utils.myprint(df_evaluate.T)
