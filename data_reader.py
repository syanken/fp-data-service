import pandas as pd
import os

def read_stock_history(stock_code,type) :
    file_path = f"data/{type}/{stock_code}.csv"
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path)

