from datetime import time
import numpy as np
import pandas as pd


# Just as a reference: _binance_columns = ['trade_id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']
_bars_columns = ['time', 'open', 'close', 'high', 'low', 'volume_contracts', 'volume_dollars', 'buyer_maker_pct', 'best_match_pct']


def time_bars(tick_data: pd.DataFrame, period: str, is_binance=True) -> pd.DataFrame:
    # TODO: this is a slow function. How can I speed it up so I can use it for a full day analysis?
    tick_data['datetime'] = tick_data['time'].apply(lambda time: pd.to_datetime(time, unit='ms'))
    
    tick_data.index = tick_data['datetime']

    time_bars = tick_data.resample(period).agg(
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
    tick_bars = tick_data.groupby(tick_data.index // n).agg(
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

class VolumeBars:
    def __init__(self) -> None:
        print('Here it goes!')

class DollarBars:
    def __init__(self) -> None:
        print('Here it goes!')

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