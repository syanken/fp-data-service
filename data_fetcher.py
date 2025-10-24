import datetime
import os
import threading
from typing import Optional

import numpy as np
import pandas as pd
import requests

from ts import *

STOCK_FIELDS = {
    'f2': '最新价',
    'f3': '涨跌幅',
    'f4': '涨跌额',
    'f5': '总手',
    'f6': '成交额',
    'f7': '振幅',
    'f8': '换手率',
    'f9': '市盈率',
    'f10': '量比',
    'f11': '5分钟涨跌幅',
    'f12': '股票代码',
    'f13': '市场',
    'f14': '股票名称',
    'f15': '最高价',
    'f16': '最低价',
    'f17': '开盘价',
    'f18': '昨收',
    'f20': '总市值',
    'f21': '流通市值',
    'f22': '涨速',
    'f23': '市净率',
    'f24': '60日涨跌幅',
    'f25': '年初至今涨跌幅',
    'f26': '上市日期',
    'f28': '昨日结算价',
    'f30': '现手',
    'f31': '买入价',
    'f32': '卖出价',
    'f33': '委比',
    'f34': '外盘',
    'f35': '内盘',
    'f36': '人均持股数',
    'f37': '净资产收益率(加权)',
    'f38': '总股本',
    'f39': '流通股',
    'f40': '营业收入',
    'f41': '营业收入同比',
    'f42': '营业利润',
    'f43': '投资收益',
    'f44': '利润总额',
    'f45': '净利润',
    'f46': '净利润同比',
    'f47': '未分配利润',
    'f48': '每股未分配利润',
    'f49': '毛利率',
    'f50': '总资产',
    'f51': '流动资产',
    'f52': '固定资产',
    'f53': '无形资产',
    'f54': '总负债',
    'f55': '流动负债',
    'f56': '长期负债',
    'f57': '资产负债比率',
    'f58': '股东权益',
    'f59': '股东权益比',
    'f60': '公积金',
    'f61': '每股公积金',
    'f62': '主力净流入',
    'f63': '集合竞价',
    'f64': '超大单流入',
    'f65': '超大单流出',
    'f66': '超大单净额',
    'f69': '超大单净占比',
    'f70': '大单流入',
    'f71': '大单流出',
    'f72': '大单净额',
    'f75': '大单净占比',
    'f76': '中单流入',
    'f77': '中单流出',
    'f78': '中单净额',
    'f81': '中单净占比',
    'f82': '小单流入',
    'f83': '小单流出',
    'f84': '小单净额',
    'f87': '小单净占比',
    'f88': '当日DDX',
    'f89': '当日DDY',
    'f90': '当日DDZ',
    'f91': '5日DDX',
    'f92': '5日DDY',
    'f94': '10日DDX',
    'f95': '10日DDY',
    'f97': 'DDX飘红天数(连续)',
    'f98': 'DDX飘红天数(5日)',
    'f99': 'DDX飘红天数(10日)',
    'f100': '行业',
    'f101': '板块领涨股',
    'f102': '地区板块',
    'f103': '备注',
    'f104': '上涨家数',
    'f105': '下跌家数',
    'f106': '平家家数',
    'f112': '每股收益',
    'f113': '每股净资产',
    'f114': '市盈率（静）',
    'f115': '市盈率（TTM）',
    'f124': '当前交易时间',
    'f128': '板块领涨股',
    'f129': '净利润',
    'f130': '市销率TTM',
    'f131': '市现率TTM',
    'f132': '总营业收入TTM',
    'f133': '股息率',
    'f134': '行业板块的成分股数',
    'f135': '净资产',
    'f138': '净利润TTM'
}


def normalize_adjust(adjust):
    if adjust is None:
        adjust = "qfq"
    input_str = str(adjust).strip().lower()

    if input_str in ["hfq", "1"]:
        return "hfq"
    elif input_str in ["qfq", "2"]:
        return "qfq"
    elif input_str in ["nfn", "3", "", "raw"]:
        return ""
    else:
        return "qfq"  # 默认前复权


def change_name(df):
    """把东方财富原始单位换算成常用单位，并补充市场前缀"""

    def safe_div(x, factor=100, r=None):
        if x == '-':
            return x
        if r:
            return round(float(x) / factor, r)
        return float(x) / factor

    _cols = ['最新价', '开盘价', '涨跌额', '昨收', '最高价', '最低价', '涨跌幅', '换手率']
    for col in _cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: safe_div(x, 100))

    if '总市值' in df.columns:
        df['总市值'] = df['总市值'].apply(lambda x: safe_div(x, 100000000, 2))
    if '总手' in df.columns:
        df['总手'] = df['总手'].apply(lambda x: safe_div(x, 10000, 2))

    # 加市场前缀
    def add_prefix(code):
        if code.startswith(('00', '30')):
            return 'sz' + code
        if code.startswith(('60', '68')):
            return 'sh' + code
        if code.startswith(('8', '92', '43')):
            return 'bj' + code
        return code

    if '股票代码' in df.columns:
        df['股票代码'] = df['股票代码'].astype(str).apply(add_prefix)

    return df


class DataFetcher:
    def __init__(self, session: Optional[requests.Session] = None):
        self.trading_days_file = 'data/trading_days.csv'

        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.timeout = 10
        if not os.path.exists('data/'):
            os.makedirs('data/')
        self.stock_list = self.read_all_stock_list()  # 慢
        self.trading_days, self.last_day = self.read_trading_days()
        self.stock_metadata = self.get_stock_metadata()
        # threading.Thread(target=self.update_all_stock_history, daemon=True).start()

    def _request(self, url: str) -> dict:
        """统一请求方法"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise RuntimeError(f"请求失败 {url}: {str(e)}")

    def _get_minute_kline(self, stock_code: str, type: str = "m1", end: str = "", length: int = 800) -> pd.DataFrame:
        """
        获取股票分钟k线数据
        :param stock_code: 股票代码
        :param type: k线类型    可选"m1","m5","m15","m30","m60","m120"
        :param end: 结束日期    格式：yyyyMMddHHmmss
        :param length: 数据长度  最大800
        :return: k线数据
        """
        url = f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={stock_code},{type},{end},{length}"
        data = self._request(url)
        data = data['data'][stock_code][type]
        df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', '1', 'exchange'])
        num_cols = ['open', 'close', 'high', 'low', 'volume']
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        return df

    def _get_day_kline(self, stock_code: str, type: str = "day", start: str = "", end: str = "", length: int = 800,
                       adjust: str = "qfq") -> pd.DataFrame:
        """
        获取股票日k线数据
        :param stock_code: 股票代码
        :param type: k线类型    可选"day","week","month","year"
        :param start: 开始日期    格式：yyyy-MM-dd
        :param end: 结束日期    格式：yyyy-MM-dd
        :param length: 数据长度  最大800
        :param adjust: 复权类型 可选、"hfq"（后复权） "qfq"（前复权）、""（不复权）
        :return: k线数据
        """
        url_1 = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?param={stock_code},{type},{start},{end},{length},{adjust}"
        url_2 = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={stock_code},{type},{start},{end},{length},{adjust}"

        try:
            data = self._request(url_1)
        except:
            data = self._request(url_2)
        if not data['data']:
            return pd.DataFrame()
        key = adjust + type if adjust + type in data['data'][stock_code] else type
        data = data['data'][stock_code][key]
        if not data:
            return pd.DataFrame()
        col_map = {6: ["date", "open", "close", "high", "low", "volume"],
                   7: ["date", "open", "close", "high", "low", "volume", "info"],
                   10: ["date", "open", "close", "high", "low", "volume", "info", "ex", "amount", "cnt"],
                   11: ["date", "open", "close", "high", "low", "volume", "info", "ex", "amount", "cnt", ""]}
        df = pd.DataFrame(data, columns=col_map.get(len(data[0])))
        num_cols = ['open', 'close', 'high', 'low', 'volume']
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        df["volume"] = df["volume"].astype(int)
        return df

    def _get_today_kline(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票今日k线数据
        :param stock_code: 股票代码
        :return: k线数据
        """
        url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={stock_code}"
        data = self._request(url)
        if 'data' in data and stock_code in data['data'] and 'data' in data['data'][stock_code]:
            data = data['data'][stock_code]['data']['data']
            data = [x.split(' ') for x in data]
            return pd.DataFrame(data, columns=['date', 'close', 'volume', 'amount'])
        else:
            return pd.DataFrame()

    def _get_five_day_kline(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票5日k线数据
        :param stock_code: 股票代码
        :return: k线数据
        """
        url = f"https://web.ifzq.gtimg.cn/appstock/app/day/query?code={stock_code}"
        data = self._request(url)
        if 'data' in data and stock_code in data['data'] and 'data' in data['data'][stock_code]:
            data = data['data'][stock_code]['data']
            data = [x['data'] for x in data]
            data = [item for sublist in data for item in sublist]
            data = [x.split(' ') for x in data]
            return pd.DataFrame(data, columns=['date', 'close', 'volume', 'amount'])
        else:
            return pd.DataFrame()

    def _get_week_kline(self, stock_code: str, adjust: str = "qfq") -> pd.DataFrame:
        """
        获取股票周k线数据
        :param stock_code: 股票代码
        :param adjust: 复权类型 可选、"hfq"（后复权） "qfq"（前复权）、""（不复权）
        :return: k线数据
        """
        url = f"https://web.ifzq.gtimg.cn/other/klineweb/klineWeb/weekTrends?code={stock_code}&type={adjust}"
        data = self._request(url)
        data = data['data']
        return pd.DataFrame(data, columns=['date', 'close'])

    def get_kline_from_qq(self, stock_code: str, type: str = "day", start: str = "", end: str = "",
                          length: int = 800, adjust: str = "qfq") -> pd.DataFrame:
        """
        从腾讯接口拉取 K线数据的通用接口
        :param stock_code: 股票代码
        :param type: k线类型    可选"m1","m5","m15","m30","m60","m120"、"1day","5day","day"、"week"、"month"、"year"
        :param start: 开始日期    格式：yyyy-MM-dd
        :param end: 结束日期    格式：yyyy-MM-dd
        :param length: 数据长度  最大800
        :param adjust: 复权类型 可选、"hfq"（后复权） "qfq"（前复权）、""（不复权）
        :return: k线数据
        """
        # minute_url = f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={stock_code},{type},{end},{length}"
        # day_url_1 = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?param={stock_code},{type},{start},{end},{length},{adjust}"
        # day_url_2 = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={stock_code},{type},{start},{end},{length},{adjust}"
        # today_url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={stock_code}"
        # five_day_url = f"https://web.ifzq.gtimg.cn/appstock/app/day/query?code={stock_code}"
        # week_url = f"https://web.ifzq.gtimg.cn/other/klineweb/klineWeb/weekTrends?code={stock_code}&type={adjust}"
        adjust = normalize_adjust(adjust)
        length = min(length, 800)
        if type in ['m1', 'm5', 'm15', 'm30', 'm60', 'm120']:
            if end != '':
                end = end.replace('-', '') + '0000'
            df = self._get_minute_kline(stock_code, type, end, length).iloc[:, :6]
        elif type in ['day', 'week', 'month', 'year']:
            df = self._get_day_kline(stock_code, type, start, end, length, adjust).iloc[:, :6]
        elif type == '1day':
            df = self._get_today_kline(stock_code)
        elif type == '5day':
            df = self._get_five_day_kline(stock_code)
        else:
            df = self._get_week_kline(stock_code, adjust)
        return df

    def get_history(self, stock_code: str, type: str = "day", start: str = "", adjust: str = "qfq"):
        """
        下载单个股票全部历史日线（不限长度）
        内部循环拼接，返回完整 DataFrame
        """
        adjust = normalize_adjust(adjust)
        all_df = []
        end = ""  # 空表示最新
        while True:
            chunk = self._get_day_kline(stock_code, type, start=start, end=end, length=800, adjust=adjust).iloc[:, :6]
            if not len(chunk):
                break
            all_df.append(chunk)
            end = chunk.iat[0, 0]  # 最早一根的 date
            if start and end <= start:  # 已经到头
                break
            if len(chunk) < 800:  # 已经到头
                break
        if chunk.empty:
            return pd.DataFrame()
        df = pd.concat(all_df[::-1], ignore_index=True).drop_duplicates(subset=['date']).sort_values('date')
        # df.to_csv(f"{stock_code}.csv", index=False)
        return df.reset_index(drop=True)

    def get_all_stock_list(self):
        """
        获取所有股票列表
        :return: 股票列表
        """
        import time
        all_stocks = []
        page = 1

        while True:
            try:
                url = f"https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f23,f26&fid=f12&pn={page}&pz=100&po=0&dect=1"
                data = self._request(url)
                stocks = ((data or {}).get('data') or {}).get('diff', [])
                if not stocks:
                    break
                all_stocks.extend(stocks)
                page += 1
                # 频率控制
                time.sleep(0.5)
            except Exception as e:
                print(f"获取股票列表失败: {e}")
                return None
        all_stocks = pd.DataFrame(all_stocks)
        all_stocks.columns = [STOCK_FIELDS.get(fid, fid) for fid in all_stocks.columns]
        change_name(all_stocks)
        return all_stocks

    def read_trading_days(self):
        """
        读取交易日历
        :return: 交易日历列表, 最新交易日
        """
        if not os.path.exists(self.trading_days_file):
            trading_days = self.update_trading_days()
            return trading_days, '1970-01-01'
        _df = pd.read_csv(self.trading_days_file)
        return _df['trading_days'].tolist(), _df.iat[-1, 0]

    def update_trading_days(self):
        """
        更新交易日历
        :return: 交易日历列表,
        """
        if not os.path.exists(self.trading_days_file):
            _df = self.get_history('sh000001', 'day')
            if not _df.empty:
                trading_days = _df['date'].tolist()
                pd.DataFrame({'trading_days': trading_days}).to_csv(self.trading_days_file, index=False)
                return trading_days
        else:
            _df = pd.read_csv(self.trading_days_file)
            trading_days = _df['trading_days'].tolist()
            if not _df.empty:
                last_day = _df.iat[-1, 0]
                _df = self.get_history('sh000001', 'day', start=last_day)
                if not _df.empty:
                    trading_days = sorted(set(trading_days + _df['date'].tolist()))
                    pd.DataFrame({'trading_days': trading_days}).to_csv(self.trading_days_file, index=False)
                    return trading_days

    def update_daily_history(self, stock_code: str, adjust: str = "qfq"):
        """
        增量更新单个股票日线文件（csv）
        首次运行会自动全量下载；后续只拉取新增日期
        """
        file_path = os.path.join('data', 'day', f"{stock_code}.csv")
        adjust = normalize_adjust(adjust)

        # 如果文件存在，读最新日期；否则全量
        if os.path.exists(file_path):
            old_df = pd.read_csv(file_path)
            last_date = old_df['date'].iat[-1]
            if last_date == self.trading_days[-1]:
                return old_df
            start = pd.to_datetime(last_date) + pd.Timedelta(days=1)
            start_str = start.strftime('%Y-%m-%d')
            new_df = self.get_kline_from_qq(stock_code, 'day', start=start_str, adjust=adjust)
            _df = pd.concat([old_df, new_df]).drop_duplicates(subset=['date']).sort_values('date')
        else:
            _df = self.get_history(stock_code, 'day', adjust)

        # 写回
        if not _df.empty:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            _df.to_csv(file_path, index=False)
        return _df

    def read_all_stock_list(self):
        cache_stock_data_path = 'data/all_stock_value.csv'
        if os.path.exists(cache_stock_data_path):
            if datetime.date.fromtimestamp(os.path.getmtime(cache_stock_data_path)) == datetime.date.today():
                df = pd.read_csv(cache_stock_data_path, encoding='utf-8')
                print(f'从文件读取股票列表')
            else:
                df = self.get_all_stock_list()
                df.to_csv(cache_stock_data_path, encoding='utf-8', index=False)
                print(f'从接口获取股票列表')
        else:
            df = self.get_all_stock_list()
            if df is None:
                return None
            df.to_csv(cache_stock_data_path, encoding='utf-8', index=False)
            print(f'从接口获取股票列表')
        return df

    def update_all_data(self):
        """
        更新所有数据
        1.更新最新所有股票列表
        2.更新元数据表
        3.更新所有股票日线数据
        """
        print('开始更新所有数据')
        # self.stock_list = self.read_all_stock_list()
        self.trading_days = self.update_trading_days()
        self.stock_metadata = pd.read_csv('data/stock_metadata.csv')
        codes_in_list = set(self.stock_list['股票代码'])
        mask_keep = self.stock_metadata['stock_code'].isin(codes_in_list)
        keep_stocks = self.stock_metadata[mask_keep].copy()
        existing_codes = set(self.stock_metadata['stock_code'])
        new_codes = self.stock_list['股票代码'][~self.stock_list['股票代码'].isin(existing_codes)].unique()
        new_stocks = pd.DataFrame({
            'stock_code': new_codes,
            'latest_trade_date': pd.NaT,
            'earliest_trade_date': pd.NaT,
            'last_sync_time': pd.NaT,
            'exchange': [code[:2] for code in new_codes],  # 提取 sz/sh/bj
            'status': 'Unknown'  # 后续可更新
        })
        updated_metadata = pd.concat([keep_stocks, new_stocks], ignore_index=True)

        need_update = updated_metadata[(updated_metadata['status'].isin(['Active', 'Halting', 'Unknown']))]
        for c in need_update[need_update['status'].isin(['Halting', 'Unknown'])]['stock_code'].tolist():
            _df = self.get_history(stock_code=c)
            if not _df.empty:
                _df.to_csv(os.path.join('data', 'day', f"{c}.csv"), index=False)
                updated_metadata.loc[updated_metadata['stock_code'] == c, 'latest_trade_date'] = _df.iat[
                    -1, _df.columns.get_loc('date')]
                updated_metadata.loc[updated_metadata['stock_code'] == c, 'earliest_trade_date'] = _df.iat[
                    0, _df.columns.get_loc('date')]
                updated_metadata.loc[updated_metadata['stock_code'] == c, 'last_sync_time'] = self.last_day
        # print(updated_metadata)
        need_update_list = need_update[need_update['status'].isin(['Active'])]['stock_code'].tolist()
        pos = 0
        for i, d in enumerate(self.trading_days):
            if d == self.last_day:
                pos = i
                break
        update_dates = self.trading_days[max(0, pos - 2):]
        res = ts_get_daily_data(trade_days=update_dates)
        self.last_day = self.trading_days[-1]
        if not res.empty:
            for c in need_update_list:
                _df = res[res['stock_code'] == c].drop('stock_code', axis=1)
                if not _df.empty:
                    try:
                        old_df = pd.read_csv(os.path.join('data', 'day', f"{c}.csv"))
                    except Exception as e:
                        old_df = pd.DataFrame(columns=['date', 'open', 'close', 'high', 'low', 'volume'])
                    # 往前多取几天，防止数据不完整，处理复权问题
                    common_dates = _df['date'].isin(old_df['date'])
                    overlap1 = _df[common_dates].set_index('date').sort_index()[
                        ['open', 'high', 'low', 'close', 'volume']]
                    overlap2 = old_df[old_df['date'].isin(_df['date'])].set_index('date').sort_index()[
                        ['open', 'high', 'low', 'close', 'volume']]
                    if overlap1.equals(overlap2):
                        old_df = old_df[~old_df['date'].isin(_df['date'])]
                        _df = pd.concat([old_df, _df])
                    else:
                        print(f' {c} 重新下载')
                        _df = self.get_history(stock_code=c)

                    _df.to_csv(os.path.join('data', 'day', f"{c}.csv"), index=False)
                    updated_metadata.loc[updated_metadata['stock_code'] == c, 'latest_trade_date'] = _df.iat[
                        -1, _df.columns.get_loc('date')]
                    updated_metadata.loc[updated_metadata['stock_code'] == c, 'earliest_trade_date'] = _df.iat[
                        0, _df.columns.get_loc('date')]
                    updated_metadata.loc[updated_metadata['stock_code'] == c, 'last_sync_time'] = self.last_day

        updated_metadata = self.set_status(updated_metadata)
        updated_metadata.to_csv('data/stock_metadata.csv', index=False)
        self.stock_metadata = updated_metadata
        print('所有数据更新完成')

    def get_stock_metadata(self):
        if not os.path.exists('data/stock_metadata.csv'):
            if self.stock_list is None:
                return pd.DataFrame()
            else:
                meta = pd.DataFrame()
                meta['stock_code'] = self.stock_list['股票代码'].copy()
                meta['latest_trade_date'] = pd.NaT
                meta['earliest_trade_date'] = pd.NaT
                meta['last_sync_time'] = pd.NaT
                meta['exchange'] = self.stock_list['股票代码'].str[:2]
                meta['status'] = 'Unknown'
                meta = self.set_status(meta)
                meta.to_csv('data/stock_metadata.csv', index=False)
                return meta
        else:
            return pd.read_csv('data/stock_metadata.csv')

    def set_status(self, meta):
        temp_df = self.stock_list[['最新价', '市盈率', '上市日期']].replace('-', np.nan)
        latest_price = temp_df['最新价']
        volume_ratio = temp_df['市盈率']
        listing_date = temp_df['上市日期']
        is_active = latest_price.notna()  # 最新价有数据
        is_halting = ~is_active & volume_ratio.notna()  # 最新价无数据但市盈率有数据
        is_unlisted = listing_date.isna()  # 无上市日期 → 未上市
        meta.loc[is_active, 'status'] = 'Active'
        meta.loc[is_halting, 'status'] = 'Halting'
        meta.loc[is_unlisted, 'status'] = 'Unlisted'
        meta.loc[~is_active & ~is_halting & ~is_unlisted, 'status'] = 'Delisted'
        return meta


if __name__ == "__main__":
    fetcher = DataFetcher()
    fetcher.update_all_data()
