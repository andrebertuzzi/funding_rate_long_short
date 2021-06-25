import time

from requests.api import get
from services import Services
from exchanges import FtxClient, PerpetualClient, BinanceUSDClient, BinanceUSDTClient
from datetime import date, datetime, timedelta
import threading
import json
import boto3

services = Services()
ftx = FtxClient()
busdt = BinanceUSDTClient()
busd = BinanceUSDClient()
perpetual = PerpetualClient()


def save_fundings(client, future, fundings):
    for funding in fundings:
        services.save_funding(client.name, funding.get('time'), client.get_asset(
            future), future, 'FUNDING', funding.get('rate'))


def get_common_assets(client1, client2):
    assets1 = list(map(client1.get_asset, client1.get_all_futures()))
    assets2 = list(map(client2.get_asset, client2.get_all_futures()))
    return list(set(assets1) & set(assets2))


def get_assets():
    return list(set(get_common_assets(ftx, busdt) + get_common_assets(ftx, busd) + get_common_assets(ftx, perpetual) +
                get_common_assets(busdt, busd) + get_common_assets(busdt,
                                                                   perpetual) + get_common_assets(busd, perpetual)))


def call_exchange(client, start, end, assets, increment):
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

    for future in futures:
        temp_start = start
        temp_end = end
        while(temp_start < end):
            temp_end = temp_start + timedelta(days=increment)
            print(f'{client.name} {future} Start {temp_start} Ending {temp_end}')
            funding_rates = client.get_historical_funding_rates(
                future, temp_start.timestamp(), temp_end.timestamp())
            save_fundings(client, future, funding_rates)
            temp_start = temp_end
            time.sleep(5)


def get_all_fundings_thread(last_date):
    """Summary or Description of the Function

    Parameters:
    last_date (datetime): Last date when method ran, use to be yesterday

    Returns:
    """

    ftx = FtxClient()
    busdt = BinanceUSDTClient()
    busd = BinanceUSDClient()
    perpetual = PerpetualClient()

    assets = get_assets()

    exchanges = [ftx, busdt, busd, perpetual]

    start = last_date
    end = datetime.utcnow()

    threads = [threading.Thread(target=call_exchange, args=(
        client, start, end, assets, 3)) for client in exchanges]
    [thread.start() for thread in threads]

# [call_exchange(client, start, end, assets) for client in exchanges]


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
            assets = get_assets()
            last_date = services.get_load_date()
            [call_lambda for asset in assets]

            # result = json.loads(response['Payload'].read().decode("utf-8"))
    except:
        message = 'Error trying to get funding rates'

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }


# lambda_handler({}, None)
