import pandas as pd
import matplotlib.pyplot as plt

from data_fetcher import DataFetcher


def backtest(df: pd.DataFrame) -> pd.DataFrame:
    """
    回测函数
    :param df: 输入的DataFrame，包含日期、价格等列
    :param strategy: 策略函数，输入为DataFrame，输出为交易信号（1为买入，-1为卖出，0为不交易）
    :param kwargs: 策略函数的其他参数
    :return: 包含交易信号、持仓、资金变化等列的DataFrame
    """
    period=600
    df=df.iloc[-period:]
    # 1. 计算5日和10日移动平均线
    df['MA5'] = df['close'].rolling(window=5).mean()  # 5日均线
    df['MA10'] = df['close'].rolling(window=10).mean()  # 10日均线

    # 2. 获取前一日的MA值（用于判断交叉方向）
    df['MA5_prev'] = df['MA5'].shift(1)  # 前一日MA5
    df['MA10_prev'] = df['MA10'].shift(1)  # 前一日MA10

    # 3. 定义买入/卖出条件
    buy_condition = (df['MA5_prev'] < df['MA10_prev']) & (df['MA5'] > df['MA10'])  # MA5上穿MA10
    sell_condition = (df['MA5_prev'] > df['MA10_prev']) & (df['MA5'] < df['MA10'])  # MA5下穿MA10

    # 4. 生成信号列（替换原df['signal']=df['close']的逻辑）
    df['signal'] = 0 # 默认无信号
    df.loc[buy_condition, 'signal'] =1  # 买入信号
    df.loc[sell_condition, 'signal'] = -1  # 卖出信号

    print(df.head())

    return df

if __name__ == '__main__':
    # 读取数据
    df = pd.read_csv('data/day/sz000581.csv')

    # 回测
    df = backtest(df)
    # df = df.reset_index(drop=True)
    plt.plot(df['close'])
    plt.plot(df['open'])
    for x in [5,10,20,40]:
        plt.plot(df['close'].rolling(x).mean())

    buy_signals = df[df['signal'] == 1]  # 买入信号数据
    sell_signals = df[df['signal'] == -1]  # 卖出信号数据

    # 3. 叠加买入信号（红色圆点）
    plt.scatter(
        buy_signals.index,  # x轴：信号发生的日期（df索引，需为datetime类型）
        buy_signals['close'],  # y轴：信号发生时的收盘价
        color='red',  # 红色标记买入
        marker='o',  # 圆形标记
        s=100,  # 点大小
        label='买入信号'
    )

    # 4. 叠加卖出信号（绿色圆点）
    plt.scatter(
        sell_signals.index,  # x轴：信号发生的日期
        sell_signals['close'],  # y轴：信号发生时的收盘价
        color='green',  # 绿色标记卖出
        marker='o',  # 圆形标记
        s=100,  # 点大小
        label='卖出信号'
    )

    plt.show()
