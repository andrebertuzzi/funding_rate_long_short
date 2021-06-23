from datetime import date, datetime, timedelta
from exchange_interface import FtxClient, BinanceClient, PerpetualClient
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

fundings = {}

ftx = FtxClient()
binance = BinanceClient()
perpetual = PerpetualClient()


def get_futures():
    """Get list of futures assets, if envirorment variable is all then return all common futures between both exchanges"""
    if(FUTURES == 'all'):
        binance_futures = [future.replace('USDT', '')
                           for future in binance.get_all_futures()]

        ftx_futures = [future.replace('-PERP', '')
                       for future in ftx.get_all_futures()]
        return list(set(binance_futures) & set(ftx_futures))
    else:
        return FUTURES.split(',')


def get_futures_usd():
    """Get list of futures assets, if envirorment variable is all then return all common futures between both exchanges"""
    if(FUTURES == 'all'):
        binance_futures = [future.replace('USD_PERP', '')
                           for future in binance.get_all_futures_usd()]

        ftx_futures = [future.replace('-PERP', '')
                       for future in ftx.get_all_futures()]
        return list(set(binance_futures) & set(ftx_futures))
    else:
        return FUTURES.split(',')


def get_futures_binance():
    """Get list of futures assets, if envirorment variable is all then return all common futures between both exchanges"""
    if(FUTURES == 'all'):
        binance_futures1 = [future.replace('USDT', '')
                            for future in binance.get_all_futures()]

        binance_futures2 = [future.replace('USD_PERP', '')
                            for future in binance.get_all_futures_usd()]

        return list(set(binance_futures1) & set(binance_futures2))
    else:
        return FUTURES.split(',')


def plot_funding_return(client1, client2, _prefix, _futures=[], _start=datetime(2020, 6, 8), _end=datetime.now(), _save=False):
    """Plot values of two assets comparing them in two diffrents exchanges

    @_futures - list of futures, if null the method gets all futures in common in both exchanges
    @_start - Start date
    @_end - End date
    @_combined - If plot the result fundina a - funding b or in separated lines
    @_save - If True save a png file instead pesent the output  """

    futures = _futures.copy()
    for future in futures:
        dfs = []
        client1_final = []
        client2_final = []
        start = _start
        end = _end
        print(f'{future} Starting {start} Ending {end}')
        count = 1
        while(start < _end):
            end = start + timedelta(days=20)
            client1_rates = client1.get_historical_funding_rates(
                client1.parse(future), start.timestamp(), end.timestamp())
            client1_final = client1_final + client1_rates
            client2_rates = client2.get_historical_funding_rates(
                client2.parse(future), start.timestamp(), end.timestamp())
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
        dfs.append({'data': df, 'name': future})
        client1_df = pd.DataFrame(client1_final)
        df = client1_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': client1.parse(future)})
        client2_df = pd.DataFrame(client2_final)
        df = client2_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': client2.parse(future)})
        generate_chart(_prefix, dfs, _save)


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


if __name__ == "__main__":
    '''
    FTX release date: datetime(2020, 11, 1)

    Common contracts:
    ['XRP', 'EOS', 'ATOM', 'EGLD', 'LTC', 'DEFI', 'THETA', 'ZEC', 'BAND', 'AVAX', 'HNT', 'MATIC', 'BNB', 
     'RSR', 'ALPHA', 'CRV', 'ETC', 'KNC', 'TOMO', 'UNI', 'BCH', 'SUSHI', 'MKR', 'YFI', 'ONT', 'XTZ', 'RUNE', 
     'XMR', 'YFII', 'VET', 'GRT', 'FLM', 'FIL', 'ADA', 'BAL', 'ETH', 'OMG', 'AAVE', 'SNX', 'SOL', 'REN', 'SXP', 
     'ALGO', 'DOT', 'DOGE', 'KSM', 'TRX', 'XLM', 'COMP', 'BAT', 'NEO', '1INCH', 'WAVES', 'CHZ', 'LINK', 'BTC']

    USD BINANCE CONTRACTS:
    ['BTC','ETH','LINK','BNB','TRX','DOT','ADA','LTC','BCH','EOS','XRP','ETC','FIL','EGLD']

    ['BAND', 'ETC', 'HOT', 'ICP', 'LTC', 'STORJ', 'TRX', 'ZRX']

    PERPETUAL CONTRACTS
    ['AmmBTCUSDC', 'AmmETHUSDC', 'AmmDOTUSDC', 'AmmYFIUSDC', 'AmmSNXUSDC', 'AmmAAVEUSDC', 'AmmLINKUSDC', 'AmmSUSHIUSDC', 'AmmCOMPUSDC',
        'AmmRENUSDC', 'AmmPERPUSDC', 'AmmUNIUSDC', 'AmmCRVUSDC', 'AmmMKRUSDC', 'AmmCREAMUSDC', 'AmmGRTUSDC', 'AmmALPHAUSDC', 'AmmFTTUSDC']
    '''

    # plot_funding_return_binance('BIN', get_futures_binance(),
    #                     datetime(2021, 5, 20), datetime.now(), True)
    plot_funding_return(ftx, binance, 'BIN_FTX', ['BAND', 'ETC'],
                        datetime(2021, 6, 1), datetime.now(), True)

    # plot_fundings(perpetual, perpetual.get_all_futures(), datetime(2021,5,1), datetime(2021,6,21), 3)
