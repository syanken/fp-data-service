# FP-Data-Service

轻量级股票 K 线数据服务（FastAPI + Python），支持日线 / 周线 / 月线复权行情，本地缓存 CSV，接口简洁，即启即用。

---

## ✨ 特性

- 日线、周线、月线、年复权行情（前复权 / 后复权 / 不复权）
- 自动落盘到本地 `data/{type}/{code}.csv`，二次请求直接读文件，提速省流量
- 增量更新：只拉取本地缺失的最新日期
- RESTful 接口，自带 OpenAPI 文档（/docs）
- Docker 一键打包，支持 `docker-compose` 快速部署



## 🚀 快速开始

1. 克隆仓库

git clone https://github.com/syanken/fp-data-service.git
cd fp-data-service

📡 核心接口

| 方法  | 路径               | 说明        | query 示例                     |
|-----|------------------|-----------|------------------------------|
| GET | /api/all-list    | 获取全市场股票列表 | —                            |
| GET | /api/kline       | 单只股票 K 线  | stock_code=sz000001&type=day |
| GET | /api/all_history | 指定区间/复权历史 | 见下表                          |

/api/all_history 参数

| 字段         | 类型     | 必填 | 描述                                |
|------------|--------|----|-----------------------------------|
| stock_code | string | ✓  | 股票代码，如 000001                     |
| type       | string | ✘  | day, week,month ,year，默认 day      |
| adjust     | string | ✘  | qfq(前复权),hfq(后复权),raw(不复权)，默认 qfq |
| start      | string | ✘  | 开始日期 yyyy-MM-dd，留空表示最早            |
| end        | string | ✘  | 结束日期 yyyy-MM-dd，留空表示最新            |
| length     | int    | ✘  | 最多返回条数，默认 800，上限 2000             |

返回示例
```json
{
  "stock_code": "000001",
  "adjust": "qfq",
  "data": [
    ["2023-01-03", 14.68, 14.99, 15.20, 14.50, 1234567],
    ...
  ]
}

## 数据来源

- 股票数据：[Tushare](https://tushare.pro/)
- 全市场股票列表：[全市场股票列表](https://www.sse.com.cn/assortment/stock/list/share/)
