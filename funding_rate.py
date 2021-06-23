from datetime import date, datetime, timedelta
from exchange_interface import BinanceUSDClient, BinanceUSDTClient, FtxClient, PerpetualClient
from time import gmtime, strftime
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import numpy

import time
import os
import settings

# Environment variables
FUTURES = os.getenv('LIST_OF_FUTURES')

ftx = FtxClient()
binance_usd = BinanceUSDClient()
binance_usdt = BinanceUSDTClient()
perpetual = PerpetualClient()


def get_common_assets(client1, client2):
    assets1 = list(map(client1.get_asset, client1.get_all_futures()))
    assets2 = list(map(client2.get_asset, client2.get_all_futures()))
    return list(set(assets1) & set(assets2))


def plot_cross_funding_return(client1, client2, _prefix, _assets=[], _start=datetime(2020, 6, 8), _end=datetime.now(), _save=False):
    """Plot values of two assets comparing them in two diffrents exchanges

    @_assets - list of assets, if null the method gets all futures in common in both exchanges
    @_start - Start date
    @_end - End date
    @_combined - If plot the result fundina a - funding b or in separated lines
    @_save - If True save a png file instead pesent the output  """

    assets = _assets.copy()
    for asset in assets:
        dfs = []
        client1_final = []
        client2_final = []
        start = _start
        end = _end
        print(f'{asset} Starting {start} Ending {end}')
        count = 1
        while(start < _end):
            end = start + timedelta(days=20)
            client1_rates = client1.get_historical_funding_rates(
                client1.parse(asset), start.timestamp(), end.timestamp())
            client1_final = client1_final + client1_rates
            client2_rates = client2.get_historical_funding_rates(
                client2.parse(asset), start.timestamp(), end.timestamp())
            client2_final = client2_final + client2_rates
            count += 1
            start = end
            time.sleep(5)

        client1_df = pd.DataFrame(client1_final)
        client2_df = pd.DataFrame(client2_final)
        df = pd.merge(client1_df, client2_df, how='outer', on='time')
        df = df.fillna(0)
        df['rate'] = df['rate_x'] - df['rate_y']
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': asset})
        client1_df = pd.DataFrame(client1_final)
        df = client1_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': client1.parse(asset)})
        client2_df = pd.DataFrame(client2_final)
        df = client2_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': client2.parse(asset)})
        generate_chart(f'{client1.name}_{client2.name}', dfs, _save)


def plot_fundings(client, _futures, _start, _end, _increment=20, _save=False):
    dfs = []
    for future in _futures:

        final = []
        start = _start
        end = _end
        count = 1
        while(start < _end):
            end = start + timedelta(days=_increment)
            print(f'Start {start} Ending {end}')
            funding_rates = client.get_historical_funding_rates(
                future, start.timestamp(), end.timestamp())
            final = final + funding_rates
            count += 1
            start = end
            time.sleep(5)

        # print(final)
        df = pd.DataFrame(final)
        df = df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': f'{future}'})
    generate_chart(f'{client.name}-{future}', dfs, _save)


def generate_chart(prefix, dfs, save=False):
    # Set the locator
    locator = mdates.MonthLocator()  # every month
    # Specify the format - %b gives us Jan, Feb...
    fmt = mdates.DateFormatter('%b')
    for item in dfs:
        plt.plot(item['data'], label=item['name'])
        plt.legend(loc='lower left')
        plt.title('Funding Arb', fontsize='16')
        plt.ylabel('Funding Rate (%)')
        plt.xlabel('Time')
        X = plt.gca().xaxis
        X.set_major_locator(locator)
        X.set_major_formatter(fmt)
    if(save):
        plt.savefig(f'output/{prefix}-{item["name"]}')
    else:
        plt.show()
        time.sleep(10)
    plt.close()


if __name__ == "__main__":
    '''
    FTX release date: datetime(2020, 11, 1)

    Usage:
    # SIMPLE
        plot_fundings(perpetual, perpetual.get_all_futures(), datetime(2021,5,1), datetime(2021,6,21), 3)
    # CROSS
        assets = get_common_assets(ftx, binance_usdt))
        plot_cross_funding_return(ftx, binance_usdt, assets,
                         datetime(2021, 5, 20), datetime.now(), True)
    '''

    print('FTX BINANCE_USDT', get_common_assets(ftx, binance_usdt))
    print('FTX BINANCE_USD', get_common_assets(ftx, binance_usd))
    print('FTX PERPETUAL', get_common_assets(ftx, perpetual))