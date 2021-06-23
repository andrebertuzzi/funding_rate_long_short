from datetime import date, datetime, timedelta
from exchange_interface import FtxClient, BinanceClient
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


def plot_funding_return(_prefix, _futures=[], _start=datetime(2020, 6, 8), _end=datetime.now(), _save=False):
    """Plot values of two assets comparing them in two diffrents exchanges

    @_futures - list of futures, if null the method gets all futures in common in both exchanges
    @_start - Start date
    @_end - End date
    @_combined - If plot the result fundina a - funding b or in separated lines
    @_save - If True save a png file instead pesent the output  """

    if(len(_futures) == 0):
        futures = get_futures()
    else:
        futures = _futures.copy()
    for future in futures:
        dfs = []
        print(future)
        ftx_final = []
        binance_final = []
        start = _start
        end = _end
        print(start)
        print(end)
        count = 1
        while(start < _end):
            print(count)
            end = start + timedelta(days=20)
            ftx_funding_rates = ftx.get_historical_funding_rates(
                f'{future}-PERP', start.timestamp(), end.timestamp())
            ftx_final = ftx_final + ftx_funding_rates
            binance_funding_rates = binance.get_historical_funding_rates(
                f'{future}USDT', int(start.timestamp()*1000), int(end.timestamp()*1000))
            binance_final = binance_final + binance_funding_rates
            count += 1
            start = end
            time.sleep(5)

        ftx_df = pd.DataFrame(ftx_final)
        binance_df = pd.DataFrame(binance_final)
        df = pd.merge(ftx_df, binance_df, how='outer', on='time')
        df = df.fillna(0)
        df['rate'] = df['rate_x'] - df['rate_y']
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': future})
        ftx_df = pd.DataFrame(ftx_final)
        df = ftx_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': f'{future}-PERP'})
        binance_df = pd.DataFrame(binance_final)
        df = binance_df.fillna(0)
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': f'{future}-USDT'})
        generate_chart(_prefix, dfs, _save)


def plot_funding_return_binance(_prefix, _futures=[], _start=datetime(2020, 6, 8), _end=datetime.now(), _save=False):
    """Plot values of two assets comparing them in two diffrents exchanges

    @_futures - list of futures, if null the method gets all futures in common in both exchanges
    @_start - Start date
    @_end - End date
    @_combined - If plot the result fundina a - funding b or in separated lines
    @_save - If True save a png file instead pesent the output  """

    if(len(_futures) == 0):
        futures = get_futures_binance()
    else:
        futures = _futures.copy()
    # print(futures)
    for future in futures:
        dfs = []
        print(future)
        binance_final_usd = []
        binance_final_usdt = []
        start = _start
        end = _end
        count = 1
        while(start < _end):
            print(count)
            print(start)
            print(end)
            end = start + timedelta(days=20)
            binance_funding_rate_usd = binance.get_historical_funding_rates_usd(
                f'{future}USD_PERP', int(start.timestamp()*1000), int(end.timestamp()*1000))
            binance_final_usd = binance_final_usd + binance_funding_rate_usd
            binance_funding_rates_usdt = binance.get_historical_funding_rates(
                f'{future}USDT', int(start.timestamp()*1000), int(end.timestamp()*1000))
            binance_final_usdt = binance_final_usdt + binance_funding_rates_usdt
            count += 1
            start = end
            time.sleep(5)

        binance_usd_df = pd.DataFrame(binance_final_usd)
        binance_usdt_df = pd.DataFrame(binance_final_usdt)
        df = pd.merge(binance_usd_df, binance_usdt_df,
                      how='outer', on='time')
        df = df.fillna(0)
        df['rate'] = df['rate_x'] - df['rate_y']
        df['acum'] = df['rate'].cumsum()
        df = df.set_index('time')
        df = df['acum']
        df = df * 100
        df.index = pd.to_datetime(df.index)
        dfs.append({'data': df, 'name': f'{future}'})
        df_usd = binance_usd_df.fillna(0)
        df_usd['acum'] = df_usd['rate'].cumsum()
        df_usd = df_usd.set_index('time')
        df_usd = df_usd['acum']
        df_usd = df_usd * 100
        df_usd.index = pd.to_datetime(df_usd.index)
        dfs.append({'data': df_usd, 'name': f'{future}-USD'})
        df_usdt = binance_usdt_df.fillna(0)
        df_usdt['acum'] = df_usdt['rate'].cumsum()
        df_usdt = df_usdt.set_index('time')
        df_usdt = df_usdt['acum']
        df_usdt = df_usdt * 100
        df_usdt.index = pd.to_datetime(df_usdt.index)
        dfs.append({'data': df_usdt, 'name': f'{future}-USDT'})
        generate_chart(_prefix, dfs, _save)


def generate_chart(prefix, dfs, save=False):
    # Set the locator
    locator = mdates.MonthLocator()  # every month
    # Specify the format - %b gives us Jan, Feb...
    fmt = mdates.DateFormatter('%b')
    if(save):
        for item in dfs:
            plt.plot(item['data'], label=item['name'])
        plt.legend(loc='lower left')
        plt.title('Funding Arb', fontsize='16')
        plt.ylabel('Funding Rate (%)')
        plt.xlabel('Time')
        X = plt.gca().xaxis
        X.set_major_locator(locator)
        X.set_major_formatter(fmt)
        plt.savefig(f'output/{prefix}-{item["name"]}')
        plt.close()
    else:
        for item in dfs:
            plt.plot(item['data'], label=item['name'])
        plt.legend(loc='lower left')
        plt.title('Funding Arb', fontsize='16')
        plt.ylabel('Funding Rate (%)')
        plt.xlabel('Time')
        X = plt.gca().xaxis
        X.set_major_locator(locator)
        X.set_major_formatter(fmt)
        plt.show()
        time.sleep(10)
        plt.close()

def plot_fundings(client, _futures, _start, _end, _save=False):
    for future in _futures:
        dfs = []
        final = []
        start = _start
        end = _end
        count = 1
        while(start < _end):
            asset = client.parse(future)
            end = start + timedelta(days=20)
            print(f'Start {start} Ending {end}')
            funding_rates = client.get_historical_funding_rates(
                asset, start.timestamp(), end.timestamp())
            final = final + funding_rates
            count += 1
            start = end
            time.sleep(5)

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
    FTX release: datetime(2020, 11, 1)

    Common contracts:
    ['XRP', 'EOS', 'ATOM', 'EGLD', 'LTC', 'DEFI', 'THETA', 'ZEC', 'BAND', 'AVAX', 'HNT', 'MATIC', 'BNB', 
     'RSR', 'ALPHA', 'CRV', 'ETC', 'KNC', 'TOMO', 'UNI', 'BCH', 'SUSHI', 'MKR', 'YFI', 'ONT', 'XTZ', 'RUNE', 
     'XMR', 'YFII', 'VET', 'GRT', 'FLM', 'FIL', 'ADA', 'BAL', 'ETH', 'OMG', 'AAVE', 'SNX', 'SOL', 'REN', 'SXP', 
     'ALGO', 'DOT', 'DOGE', 'KSM', 'TRX', 'XLM', 'COMP', 'BAT', 'NEO', '1INCH', 'WAVES', 'CHZ', 'LINK', 'BTC']

    USD BINANCE CONTRRCTS:
    ['BTC','ETH','LINK','BNB','TRX','DOT','ADA','LTC','BCH','EOS','XRP','ETC','FIL','EGLD']
    '''

    ['BAND', 'ETC', 'HOT', 'ICP', 'LTC', 'STORJ', 'TRX', 'ZRX']

    # plot_funding_return_binance('BIN', get_futures_binance(),
    #                     datetime(2021, 5, 20), datetime.now(), True)
    plot_funding_return('BIN_FTX', get_futures(),
                        datetime(2021, 5, 20), datetime.now(), True)
    # plot_fundings(ftx, ['BAND'], datetime(2021,5,1), datetime(2021,6,21))
