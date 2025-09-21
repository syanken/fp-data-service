from fastapi import FastAPI
from data_fetcher import *
from pydantic import BaseModel
from data_reader import read_stock_history
from contextlib import asynccontextmanager

stock_list = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global stock_list
    stock_list = read_all_stock_list()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "Quant World!"}


@app.get("/api/all-list")
def get_all_list():
    return {
        "data": stock_list.to_dict("records")
    }


@app.get("/api/kline")
def get_kline(stock_code: str, type: str = "day"):
    try:
        _df = read_stock_history(stock_code, type)
        if _df.empty:
            _df = get_stock_history(stock_code, type)
            num_cols = ['open', 'close', 'high', 'low', 'volume']
            for c in num_cols:
                if c in _df.columns:
                    _df[c] = pd.to_numeric(_df[c], errors='coerce')
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


@app.get("/api/all_history")
def get_today_kline(request: KlineRequest):
    if request.type in ["day", "week", "month", "year"]:
        try:
            _df = get_stock_history(request.stock_code, request.type, request.adjust)
        except Exception as e:
            return {"error": str(e)}
        return {
            "stock_code": request.stock_code,
            "adjust": request.adjust,
            "data": _df.values.tolist()
        }
    else:
        return {"error": "type must be day, week, month, year"}


if __name__ == "__main__":
    # 初始化数据
    # stock_list = read_all_stock_list()
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000)
