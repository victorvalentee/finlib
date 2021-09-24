from os import name
import pandas as pd

"""Colunm names for the binance tick data format."""
_binance_columns = ['trade_id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']

def read_csv(filepath, is_binance=True) -> pd.DataFrame:
    """Reads a csv file into a pandas Dataframe."""
    if is_binance:
        return pd.read_csv(filepath, names=_binance_columns)
    else:
        return None


if __name__ == '__main__':
    tick_data = read_csv('/home/victorvalentee/python_projects/data/test/binance_ticks.csv')
    print(tick_data.head())
