from datetime import time
import numpy as np
import pandas as pd


# Just as a reference: _binance_columns = ['trade_id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']
_bars_columns = ['time', 'open', 'close', 'high', 'low', 'volume_contracts', 'volume_dollars', 'buyer_maker_pct', 'best_match_pct']


def get_bars_index_by_column_amount(tick_data, column, amount):
    sum = 0
    new_index = 0
    index = []

    for index_, value in enumerate(tick_data[column]):    
        sum += value
        
        if sum > amount:
            index.append(index_)
            new_index = index_
            sum = 0
            
        else:
            index.append(new_index)
        
    return index


def time_bars(tick_data: pd.DataFrame, period: str, is_binance=True) -> pd.DataFrame:
    # TODO: this is a slow function. How can I speed it up so I can use it for a full day analysis?

    tick_data_cp = tick_data.copy()
    tick_data_cp['datetime'] = tick_data_cp['time'].apply(lambda time: pd.to_datetime(time, unit='ms'))
    
    tick_data_cp.index = tick_data_cp['datetime']

    time_bars = tick_data_cp.resample(period).agg(
        func = None, # TODO: this line can be erased when pandas is upgraded to version 1.4.
        time = ('datetime', 'min'),
        open = ('price', 'first'),
        close = ('price', 'last'),
        high = ('price', 'max'),
        low = ('price', 'min'),
        volume_contracts = ('qty', 'sum'),
        volume_dollars = ('quoteQty', 'sum'),
        buyer_maker_pct = ('isBuyerMaker', lambda sample: np.mean(sample)),
        best_match_pct = ('isBestMatch', lambda sample: np.mean(sample))
    )

    # TODO: implement VWAP. But first, does vwap work for cryto?
    # time_bars['vwap'] = time_bars['volume_dollars'] / time_bars['volume_contracts']

    return time_bars[_bars_columns].reset_index(drop=True)


def tick_bars(tick_data: pd.DataFrame, n=1000, is_binance=True) -> pd.DataFrame:
    tick_bars = tick_data.groupby(tick_data.trade_id // n).agg(
        time = ('time', 'min'),
        open = ('price', 'first'),
        close = ('price', 'last'),
        high = ('price', 'max'),
        low = ('price', 'min'),
        volume_contracts = ('qty', 'sum'),
        volume_dollars = ('quoteQty', 'sum'),
        buyer_maker_pct = ('isBuyerMaker', lambda sample: np.mean(sample)),
        best_match_pct = ('isBestMatch', lambda sample: np.mean(sample))
    )

    tick_bars['time'] = tick_bars['time'].apply(lambda time: pd.to_datetime(time, unit='ms'))

    return tick_bars[_bars_columns].reset_index(drop=True)


def volume_bars(tick_data: pd.DataFrame, volume=10, is_binance=True) -> pd.DataFrame:
    #TODO: volume bars and dollar bars are prone to overflow. How to deal with this characteristic? 
    bars_index = get_bars_index_by_column_amount(tick_data, column='qty', amount=volume)

    volume_bars = tick_data.groupby(bars_index).agg(
        time = ('time', 'min'),
        open = ('price', 'first'),
        close = ('price', 'last'),
        high = ('price', 'max'),
        low = ('price', 'min'),
        volume_contracts = ('qty', 'sum'),
        volume_dollars = ('quoteQty', 'sum'),
        buyer_maker_pct = ('isBuyerMaker', lambda sample: np.mean(sample)),
        best_match_pct = ('isBestMatch', lambda sample: np.mean(sample))
    )

    volume_bars['time'] = volume_bars['time'].apply(lambda time: pd.to_datetime(time, unit='ms'))

    return volume_bars[_bars_columns].reset_index(drop=True)


def dollar_bars(tick_data: pd.DataFrame, dollars=1000, is_binance=True) -> pd.DataFrame:
    #TODO: volume bars and dollar bars are prone to overflow. How to deal with this characteristic? 
    bars_index = get_bars_index_by_column_amount(tick_data, column='quoteQty', amount=dollars)

    dollar_bars = tick_data.groupby(bars_index).agg(
        time = ('time', 'min'),
        open = ('price', 'first'),
        close = ('price', 'last'),
        high = ('price', 'max'),
        low = ('price', 'min'),
        volume_contracts = ('qty', 'sum'),
        volume_dollars = ('quoteQty', 'sum'),
        buyer_maker_pct = ('isBuyerMaker', lambda sample: np.mean(sample)),
        best_match_pct = ('isBestMatch', lambda sample: np.mean(sample))
    )

    dollar_bars['time'] = dollar_bars['time'].apply(lambda time: pd.to_datetime(time, unit='ms'))

    return dollar_bars[_bars_columns].reset_index(drop=True)

class TickImbalanceBars:
    def __init__(self) -> None:
        print('Here it goes!')

class VolumeImbalanceBars:
    def __init__(self) -> None:
        print('Here it goes!')

class DollarImbalanceBars:
    def __init__(self) -> None:
        print('Here it goes!')

class TickRunsBars:
    def __init__(self) -> None:
        print('Here it goes!')

class VolumeRunsBars:
    def __init__(self) -> None:
        print('Here it goes!')

class DollarRunsBars:
    def __init__(self) -> None:
        print('Here it goes!')


if __name__ == '__main__':
    import tick_data
    
    tick_data = tick_data.read_csv('/home/victorvalentee/python_projects/data/test/binance_ticks.csv')
    time_bars = time_bars(tick_data, period='5min')
    print(time_bars.head())