import tushare as ts
import pandas as pd
from dateutil.utils import today
import time
ts.set_token('4dc12a8feef3e5f980d89212234065f85d00867791ac37d2836cdc7b')
pro = ts.pro_api()


# 转换函数
def ts_code_to_code(_ts_code):
    return _ts_code.split('.')[-1].lower() + _ts_code.split('.')[0]


def code_to_ts_code(stock_code):
    return stock_code[2:8] + '.' + stock_code[:2].upper()


def date_to_ts_date(date_str):
    return date_str.replace('-', '')


def ts_date_to_date(ts_date):
    return ts_date[:4] + '-' + ts_date[4:6] + '-' + ts_date[6:]


def ts_get_daily_data(code=None, trade_days=None):
    use_cols = ['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume']
    ts_code = ''
    if code:
        for c in code:
            ts_code += code_to_ts_code(c) + ','
    if trade_days is None:
        _df = pro.daily(ts_code=ts_code, trade_date=today().strftime("%Y%m%d"))
    elif isinstance(trade_days, str):
        _df = pro.daily(ts_code=ts_code, trade_date=trade_days)
    elif isinstance(trade_days, list):
        _df = pd.DataFrame()
        for day in trade_days:
            _df = pd.concat([_df, pro.daily(trade_date=day.replace('-', ''))])
            time.sleep(0.8)
    else:
        raise ValueError("trade_days 必须是 None, str 或 list 类型")
    if _df.empty:
        return _df
    _df['stock_code'] = _df['ts_code'].apply(ts_code_to_code)
    _df['date'] = (
            _df['trade_date'].str[:4] + '-' +
            _df['trade_date'].str[4:6] + '-' +
            _df['trade_date'].str[6:]
    )
    _df['volume'] = _df['vol']
    _df["volume"] = _df["volume"].round().astype(int)
    return _df.reset_index()[use_cols]


def ts_get_history(ts_code):
    # 获取股票历史数据
    if ts_code is None:
        return pd.DataFrame()
    else:
        end_date = None
        _df = None
        while True:
            new_df = pd.DataFrame()
            if end_date:
                new_df = pro.daily(ts_code=ts_code, end_date=end_date)
            else:
                new_df = pro.daily(ts_code=ts_code)
            if new_df.empty:
                return _df.reset_index()
            _df = pd.concat([_df, new_df])
            end_date = (pd.to_datetime(_df.trade_date.iloc[-1]) - pd.Timedelta(days=1)).strftime("%Y%m%d")


if __name__ == "__main__":
    # df = get_daily_data(["20250926", "20250925"])
    # df = pro.daily(ts_code=code_to_ts_code('sz000001'), end_date='19910403')
    df = ts_get_daily_data(["20250926", "20250925"])
    print(df)
    # print(code_to_ts_code('sz000001'))
