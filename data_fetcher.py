import datetime
import os
from typing import Optional

import pandas as pd
import requests

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
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        # 可选：设置超时、代理、重试策略等
        self.timeout = 10

        self.stock_list = self.read_all_stock_list()
        self.trading_days = self.get_stock_history('sh000001', 'day')['date']  # 获取所有交易日

    def _request(self, url: str) -> dict:
        """统一请求方法，带错误处理"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise RuntimeError(f"请求失败 {url}: {str(e)}")

    def get_minute_kline(self, stock_code: str, type: str = "m1", end: str = "", length: int = 800) -> pd.DataFrame:
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

    def get_day_kline(self, stock_code: str, type: str = "day", start: str = "", end: str = "", length: int = 800,
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
        return df

    def get_today_kline(self, stock_code: str) -> pd.DataFrame:
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

    def get_five_day_kline(self, stock_code: str) -> pd.DataFrame:
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

    def get_week_kline(self, stock_code: str, adjust: str = "qfq") -> pd.DataFrame:
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
            df = self.get_minute_kline(stock_code, type, end, length).iloc[:, :6]
        elif type in ['day', 'week', 'month', 'year']:
            df = self.get_day_kline(stock_code, type, start, end, length, adjust).iloc[:, :6]
        elif type == '1day':
            df = self.get_today_kline(stock_code)
        elif type == '5day':
            df = self.get_five_day_kline(stock_code)
        else:
            df = self.get_week_kline(stock_code, adjust)
        return df

    def get_stock_history(self, stock_code: str, type: str = "day", adjust: str = "qfq"):
        """
        下载单个股票全部历史日线（不限长度）
        内部循环拼接，返回完整 DataFrame
        """
        adjust = normalize_adjust(adjust)
        all_df = []
        end = ""  # 空表示最新
        while True:
            chunk = self.get_day_kline(stock_code, type, start="", end=end, length=800, adjust=adjust).iloc[:, :6]
            if not len(chunk):
                break
            all_df.append(chunk)
            end = chunk.iat[0, 0]  # 最早一根的 date
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
                stocks = (data or {}).get('data', {}).get('diff', [])
                if not stocks:
                    break
                all_stocks.extend(stocks)
                page += 1
                # 频率控制
                time.sleep(0.5)
            except Exception as e:
                print(f"获取股票列表失败: {e}")
                break
        all_stocks = pd.DataFrame(all_stocks)
        all_stocks.columns = [STOCK_FIELDS.get(fid, fid) for fid in all_stocks.columns]
        change_name(all_stocks)
        return all_stocks

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
            # 防止接口闭市无数据，往前多取 3 天重叠
            start = pd.to_datetime(last_date) - pd.Timedelta(days=3)
            start_str = start.strftime('%Y-%m-%d')
            new_df = self.get_kline_from_qq(stock_code, 'day', start=start_str, adjust=adjust)
            _df = pd.concat([old_df, new_df]).drop_duplicates(subset=['date']).sort_values('date')
        else:
            _df = self.get_stock_history(stock_code, 'day', adjust)

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
            df.to_csv(cache_stock_data_path, encoding='utf-8', index=False)
            print(f'从接口获取股票列表')
        return df

    def get_trading_days(self):
        """
        获取股票的所有交易日
        :return: 所有交易日
        """
        df = self.get_stock_history('sh000001', 'day')
        if df.empty:
            return []
        return df['date'].tolist()


if __name__ == "__main__":
    # stock_list = read_all_stock_list()
    # for x in stock_list['股票代码']:
    #     update_daily_history(x)
    #     print(f'更新股票 {x} 日线数据')
    # df=update_daily_history('sz300059')
    # print(df)
    fetcher = DataFetcher()
    data = fetcher.get_stock_history('sh000001', 'day')
    for i in data.iloc[-1]:
        print(type(i))
