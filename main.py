from contextlib import asynccontextmanager
from fastapi import FastAPI

from data_fetcher import DataFetcher
from pydantic import BaseModel
from data_reader import read_stock_history

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

fetcher = DataFetcher()
scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))  # 设置为中国时区
# 添加定时任务：每天 16:00 执行
scheduler.add_job(
    fetcher.update_all_data,
    'cron',
    hour=16,
    minute=0,
    id='update_stock_data_daily',
    replace_existing=True,
    name='每日16:00更新股票数据'
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动事件
    scheduler.start()
    print("⏰ 定时任务调度器已启动")
    yield
    # 关闭事件
    scheduler.shutdown()
    print("⏰ 定时任务调度器已关闭")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "Quant World!"}


@app.get("/api/all-list")
def get_all_list():
    return {
        "data": fetcher.stock_list.to_dict("records")
    }


@app.get("/api/kline")
def get_kline(stock_code: str, type: str = "day"):
    try:
        _df = read_stock_history(stock_code, type)
        if _df.empty:
            _df = fetcher.get_history(stock_code, type)
    except Exception as e:
        return {
            "error": str(e)
        }
    print(_df)
    return {
        "stock_code": stock_code,
        "data": _df.values.tolist()
    }


class KlineRequest(BaseModel):
    """
    获取股票k线数据
    :param stock_code: 股票代码
    :param type: k线类型    可选"m1","m5","m15","m30","m60","m120"、"1day","5day","day"、"week"、"month"、"year"
    :param start: 开始日期    格式：yyyy-MM-dd
    :param end: 结束日期    格式：yyyy-MM-dd
    :param length: 数据长度  指数最大2000，个股最大800
    :param adjust: 复权类型 可选、"hfq"、"1"（后复权） "qfq"、"2"（前复权）、"raw"、""、"nfn"、"3"（不复权）
    :return: k线数据
    """
    stock_code: str
    type: str = "day"
    start: str = ""
    end: str = ""
    length: int = 800
    adjust: str = "qfq"


if __name__ == "__main__":
    # import uvicorn
    #
    # uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
    import requests

    res = requests.get('http://localhost:8000/api/kline?stock_code=sh000001&type=day')
    data = res.json().get("data")[-1]
    # data=read_stock_history('sh600001','day')
    for i in data:
        print(type(i))


# uvicorn main:app --host 127.0.0.1 --port 8000 --reload
