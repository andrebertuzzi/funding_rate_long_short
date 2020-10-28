from datetime import datetime, timedelta
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
    if(FUTURES == 'all'):
        binance_futures = [future.replace('USDT', '')
                           for future in binance.get_all_futures()]

        ftx_futures = [future.replace('-PERP', '')
                       for future in ftx.get_all_futures()]
        return list(set(binance_futures) & set(ftx_futures))
    else:
        return FUTURES.split(',')


def plot_funding_return(_futures=[], _start=datetime(2020, 6, 8), _end=datetime.now(), _combined=True, _save=False):
    if(len(_futures) == 0):
        futures = get_futures()
    else:
        futures = _futures.copy()
    # print(futures)
    dfs = []
    for future in futures:
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
            ftx_funding_rates.reverse()
            ftx_final = ftx_final + ftx_funding_rates
            # print('FTX------------')
            # print(ftx_funding_rates)
            binance_funding_rates = binance.get_historical_funding_rates(
                f'{future}USDT', int(start.timestamp()*1000), int(end.timestamp()*1000))
            # print('BINANCE--------', start, end)
            # print(binance_funding_rates)
            binance_final = binance_final + binance_funding_rates
            count += 1
            start = end
            time.sleep(5)
        if(_combined):
            if(len(ftx_final) > 10 and len(binance_final) > 1):
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
        else:
            if(len(ftx_final) > 1):
                # print(ftx_final)
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
    generate_chart(dfs, _save)


def generate_chart(dfs, save=False):
    # Set the locator
    locator = mdates.MonthLocator()  # every month
    # Specify the format - %b gives us Jan, Feb...
    fmt = mdates.DateFormatter('%b')
    if(save):
        for item in dfs:
            plt.plot(item['data'], label=item['name'])
            plt.legend(loc='lower left')
            plt.title('FTX Funding Rate Acum', fontsize='16')
            plt.ylabel('Funding Rate (%)')
            plt.xlabel('Time')
            X = plt.gca().xaxis
            X.set_major_locator(locator)
            X.set_major_formatter(fmt)
            plt.savefig(f'output/{item["name"]}')
            plt.close()
    else:
        for item in dfs:
            plt.plot(item['data'], label=item['name'])
        plt.legend(loc='lower left')
        plt.title(f'FTX Funding Rate Acum {item["name"]}', fontsize='16')
        plt.ylabel('Funding Rate (%)')
        plt.xlabel('Time')
        X = plt.gca().xaxis
        X.set_major_locator(locator)
        X.set_major_formatter(fmt)
        plt.show()
        time.sleep(10)
        plt.close()


# ['FLM', 'DOGE', 'SXP', 'UNI', 'BAL', 'XRP', 'ADA', 'BNB', 'KNC', 'TOMO', 'LINK', 'OMG', 'RUNE', 'XTZ', 'SOL', 'BCH', 'AVAX',
# 'NEO', 'DOT', 'AAVE', 'ATOM', 'BTC', 'DEFI', 'YFI', 'HNT', 'EOS', 'MKR', 'COMP', 'TRX', 'SUSHI', 'ZEC', 'VET', 'ALGO', 'ETH', 'LTC', 'THETA', 'ETC']
plot_funding_return([],
                    datetime(2020, 10, 1), datetime.now(), False, True)
