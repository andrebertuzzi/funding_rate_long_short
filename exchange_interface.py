import requests
import json
from datetime import datetime, timedelta


class FtxClient:
    def __init__(self, base_url=None):
        self._base_url = 'https://ftx.com/api/'

    def get_all_futures(self):
        url = f'{self._base_url}futures'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['name']
                         for symbol in response['result'] if symbol['perpetual'] == True]
        return futures_names

    # UTC TIME
    def get_historical_funding_rates(self, future, start, end):
        url = f'{self._base_url}funding_rates?future={future}&start_time={start}&end_time={end}'
        response = json.loads((requests.get(url)).content)
        funding_rates = response['result']
        return funding_rates


class BinanceClient:
    def __init__(self, base_url=None):
        self._base_url = 'https://www.binance.com/fapi/v1/'

    def get_all_futures(self):
        url = f'{self._base_url}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return futures_names

    # UTC -3, need ajusts
    def get_historical_funding_rates(self, future, start, end):
        url = f'{self._base_url}fundingRate?symbol={future}&startTime={start}&endTime={end}'
        # print(url)
        response = json.loads((requests.get(url)).content)
        funding_rates = [{'time': (datetime.fromtimestamp(item['fundingTime']/1000) + timedelta(hours=3)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['fundingRate'])} for item in response]
        return funding_rates
