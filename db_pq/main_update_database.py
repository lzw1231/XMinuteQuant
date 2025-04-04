import utils, tqsdk, datetime, os, json
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


def main():
    # 加载配置文件. k线数量在配置文件里
    cfg = utils.load_para_config()
    # 构建映射字典，从合约代码到（交易所名称，品种名称）的映射 ，方便查询
    dict_dc = utils.build_contract_map()
    # 将path_latest中的parquet文件复制到path_historical
    utils.copy_directory(cfg['historical'], cfg['latest'])
    # 登录api, 输入TQ账户和密码
    api = tqsdk.TqApi(account=tqsdk.TqKq(), auth=tqsdk.TqAuth(cfg['user'], cfg['pwd']))
    # 从天勤api中查询交易品种 —— dc指主连合约
    dc = sorted(api.query_quotes(ins_class="CONT"))
    print(f"从天勤得到的交易品种数量为{len(dc)}")
    # 北京时间、
    time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"**************** {time}  准备下载所有交易品种最近{cfg['days']}根日K线！****************")
    # 更新品种计数
    i, j = 0, 0
    # 遍历交易品种
    for symbol_name in dc:
        # 根据交易品种代码，获取交易所名称，交易品种名称
        exchange_cn, symbol_cn = utils.get_exchange_symbol_cn(dict_dc, symbol_name)

        # 判断是交易品种否在json中，如果是的话，下载K线，存入临时文件夹
        if (exchange_cn, symbol_cn) != (None, None):
            # 获取交易品种的最新日K线,num为k线个数
            df_latest_kline = utils.kline_get(api, symbol_name, cfg['days'])
            # 拼接临时数据.parquet的地址
            path_temp_pq = os.path.join(cfg['temp'], f"{symbol_cn}.parquet")
            # 选择需要的列
            df_latest_kline = df_latest_kline[['candle_begin_time_GMT8', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            # 删除空数据
            df_latest_kline.dropna(how='any')
            # 删除交易量为0的数据
            df_latest_kline = df_latest_kline[df_latest_kline['volume'] != 0]

            # 将获取的最新df_latest_kline,复制给df_temp_kline
            df_temp_kline = df_latest_kline.copy()
            # 新增两列
            df_temp_kline["交易所"] = exchange_cn
            df_temp_kline["主连名称"] = symbol_cn

            # 交易时间去掉后面的字符串 8:00:00
            df_temp_kline['candle_begin_time_GMT8'] = df_temp_kline['candle_begin_time_GMT8'].astype('str').str[0:10]
            # 取出需要的列
            df_temp_kline = df_temp_kline[
                ['candle_begin_time_GMT8', '交易所', '主连名称', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            # 给列重新命名
            rename_dict = {'candle_begin_time_GMT8': '交易日期', 'symbol': '合约代码', 'open': '开盘价', 'high': '最高价', 'low': '最低价', 'close': '收盘价', 'volume': '成交量'}
            df_temp_kline.rename(columns=rename_dict, inplace=True)

            df_temp_kline['交易日期'] = pd.to_datetime(df_temp_kline['交易日期'], format='%Y-%m-%d', errors='coerce')

            # 将读取的日k线，‘交易日期’这一列的数据格式改为pa.date32
            df_temp_kline_date32 = utils.convert_to_date32(pa.Table.from_pandas(df_temp_kline))

            # 写入Parquet文件,存入临时文件夹
            pq.write_table(
                df_temp_kline_date32,
                path_temp_pq,
                compression='zstd',
                compression_level=9,
                row_group_size=100000,
                use_dictionary=['合约代码'],  # 对商品代码启用字典编码
                data_page_size=1024 * 1024,  # 1MB
                version='2.6',  # 使用Parquet 2.6格式以支持所有特性
            )
            print(f"{(i := i + 1):>2}: {symbol_cn} -> 日K线数据已下载", flush=True)
    # 从json中取出期货名称，以数组形式存入 symbol_cn 中
    with open('./utils/dominant_contract.json', 'r', encoding='utf-8') as f:
        zl_js = json.load(f)
    symbol_cn = [kv for exchange in zl_js.values() for kv in exchange.keys()]
    # 北京时间、
    time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"**************** {time}  准备合并更新所有交易品种日K线！****************")
    # 拼接K数据
    for cn in symbol_cn:
        # 拼接parquet文件名
        path_temp_pq = os.path.join(cfg['temp'], f"{cn}.parquet")
        path_historical_pq = os.path.join(cfg['historical'], f"{cn}.parquet")
        path_latest_pq = os.path.join(cfg['latest'], f"{cn}.parquet")

        # print(path_temp_pq, path_historical_pq, path_latest_pq)
        df_temp = pd.read_parquet(path_temp_pq, engine="pyarrow")
        df_temp = df_temp[['交易日期', '交易所', '主连名称', '合约代码', '开盘价', '最高价', '最低价', '收盘价', '成交量']]

        # 如果不是新增的品种
        if os.path.exists(path_historical_pq):
            df_historical = pd.read_parquet(path_historical_pq, engine="pyarrow")
            df_historical = df_historical[['交易日期', '交易所', '主连名称', '合约代码', '开盘价', '最高价', '最低价', '收盘价', '成交量']]

            # 合并保存
            df = pd.concat([df_historical, df_temp], axis=0)
            # 数据去重
            df.drop_duplicates(subset=['交易日期'], keep='last', inplace=True)

            # 统一输出格式
            df = df[['交易日期', '交易所', '主连名称', '合约代码', '开盘价', '最高价', '最低价', '收盘价', '成交量']]
            # 按时间排序
            df.sort_values(by='交易日期', ascending=True, inplace=True)
            df = df.reset_index(drop=True)

            # 写入Parquet文件,存入latest文件夹
            pq.write_table(
                pa.Table.from_pandas(df),
                path_latest_pq,
                compression='zstd',
                compression_level=9,
                row_group_size=100000,
                use_dictionary=['合约代码'],  # 对商品代码启用字典编码
                data_page_size=1024 * 1024,  # 1MB
                version='2.6',  # 使用Parquet 2.6格式以支持所有特性
            )

            print(f"{(j := j + 1):>2} : {cn[:12]} -> 日K线数据已更新！")

        # 如果是新增品种，则不用合并，直接写入
        else:
            # 写入Parquet文件,存入latest文件夹
            pq.write_table(
                pa.Table.from_pandas(df_temp),
                path_latest_pq,
                compression='zstd',
                compression_level=9,
                row_group_size=100000,
                use_dictionary=['合约代码'],  # 对商品代码启用字典编码
                data_page_size=1024 * 1024,  # 1MB
                version='2.6',  # 使用Parquet 2.6格式以支持所有特性
            )
            print(f"{(j := j + 1):>2} : {cn[:12]} -> 日K线数据已更新")
    # 北京时间
    print(f"**************** {time}  所有交易品种日K线更新完毕！****************")
    api.close()
    exit()


# 配置文件在./utils里面。
# para_config.toml是参数配置文件
# dominant_contract.json是交易品种的json
if __name__ == "__main__":
    main()
