import time
import sys
import settings
from requests.api import get
from services import Services
from exchanges import FtxClient, PerpetualClient, BinanceUSDClient, BinanceUSDTClient
from datetime import date, datetime, timedelta, timezone
import threading
import json
import boto3
import os

services = Services()
ftx = FtxClient()
busdt = BinanceUSDTClient()
busd = BinanceUSDClient()
perpetual = PerpetualClient()

BINANCE_KEY = os.getenv('BINANCE_KEY')
BINANCE_SECRET = os.getenv('BINANCE_SECRET')
FTX_KEY = os.getenv('FTX_KEY')
FTX_SECRET = os.getenv('FTX_SECRET')
FTX_SUBACC = os.getenv('FTX_SUBACC')


def _get_current_positions():
    return [{'asset': 'BAND', 'notional': 100, 'qty': 2776, 'type': 'ARB', 'exchanges': ['FTX', 'BINANCEUSDT']},
            {'asset': 'BAO', 'notional': 100, 'qty': 2776, 'type': 'SELL', 'exchanges': ['FTX']},
            {'asset': 'ETH', 'notional': 75813.92, 'qty': 35.56, 'type': 'ARB', 'exchanges': ['PERP', 'BINANCEUSDT']}]


def _get_exchanges():
    return [ftx, busdt, busd, perpetual]


def save_fundings(client, future, fundings, type):
    [services.save_trade(client.name, funding.get('time'), client.get_asset(
        future), future, funding.get('type'), funding.get('rate'), funding.get('notional', 0), funding.get('payment', 0)) for funding in fundings]


def get_common_assets(client1, client2):
    assets1 = list(map(client1.get_asset, client1.get_all_futures()))
    assets2 = list(map(client2.get_asset, client2.get_all_futures()))
    return list(set(assets1) & set(assets2))


def _get_assets():
    return list(set(get_common_assets(ftx, busdt) + get_common_assets(ftx, busd) + get_common_assets(ftx, perpetual) +
                get_common_assets(busdt, busd) + get_common_assets(busdt,
                                                                   perpetual) + get_common_assets(busd, perpetual)))


def call_exchange(client, start, end, assets, increment, function, type):
    """Summary or Description of the Function

    Parameters:
    client (Exchange): Exchange interface client
    start (int): Start date
    end (datetime): End date
    assets (list): List os assets 
    increment (int): Number of days

    Returns:
    int:Returning value
    """
    futures_total = list(map(client.parse, assets))
    futures_client = client.get_all_futures()
    futures = list(set(futures_total) & set(futures_client))

    print(futures)
    
    for future in futures:
        temp_start = start
        temp_end = end
        while(temp_start < end):
            temp_end = temp_start + timedelta(days=increment)
            print(f'{client.name} {future} Start {temp_start} Ending {temp_end}')
            funding_rates = function(
                future, temp_start.timestamp(), temp_end.timestamp())
            save_fundings(client, future, funding_rates, type)
            temp_start = temp_end
            time.sleep(5)


def get_all_fundings_thread(last_date):
    """Summary or Description of the Function

    Parameters:
    last_date (datetime): Last date when method ran, use to be yesterday

    Returns:
    """
    assets = _get_assets()
    exchanges = _get_exchanges()

    start = last_date
    end = datetime.utcnow()

    threads = [threading.Thread(target=call_exchange, args=(
        client, start, end, assets, 3, client.get_historical_funding_rates, 'FUNDING')) for client in exchanges]
    [thread.start() for thread in threads]

# [call_exchange(client, start, end, assets) for client in exchanges]


def get_all_payments_thread(last_date):
    """Summary or Description of the Function

    Parameters:
    last_date (datetime): Last date when method ran, use to be yesterday

    Returns:
    """
    assets = [x.get('asset') for x in _get_current_positions()]
    exchanges = [ftx_client, binance_client, perpetual]

    start = last_date
    end = datetime.utcnow()

    threads = [threading.Thread(target=call_exchange, args=(
        client, start, end, assets, 3, client.get_private_funding_payments, 'PAYMENT')) for client in exchanges]
    [thread.start() for thread in threads]


def get_all_fundings_by_asset(last_date, asset):
    """Summary or Description of the Function

    Parameters:
    last_date (datetime): Last date when method ran, use to be yesterday

    Returns:
    """
    exchanges = [ftx, busdt, busd, perpetual]

    start = last_date
    end = datetime.utcnow()
    [call_exchange(client, start, end, [asset], 3) for client in exchanges]


def call_lambda(asset):
    client = boto3.client('lambda')
    time.sleep(5)
    client.invoke(
        FunctionName='crypto-consumer',
        InvocationType='Event',
        LogType='None',
        Payload=json.dumps({'asset': asset}))


def lambda_handler(event, context):
    asset = event.get('asset')
    try:
        if(asset):
            last_date = services.get_load_date()
            get_all_fundings_by_asset(last_date, asset)
            message = f'All {asset} funding rates saved'
        else:
            message = f'All funding rates saved'
            assets = _get_assets()
            last_date = services.get_load_date()
            [call_lambda for asset in assets]

            # result = json.loads(response['Payload'].read().decode("utf-8"))
    except:
        message = 'Error trying to get funding rates'

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }


if __name__ == '__main__':
    ftx_client = FtxClient(FTX_KEY, FTX_SECRET, FTX_SUBACC, 30)
    binance_client = BinanceUSDTClient(BINANCE_KEY, BINANCE_SECRET)

    if sys.argv[1] == 'payment':
        print('Payments')
        payment_date = datetime.utcnow() + timedelta(days=-1)
        get_all_payments_thread(payment_date)
        
    elif sys.argv[1] == 'funding':
        print('Fundings')
        last_date = services.get_load_date()
        get_all_fundings_thread(last_date)
    
    else:
        print('No options')
