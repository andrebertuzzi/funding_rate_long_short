import requests
import json
from datetime import datetime, timedelta
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport


class FtxClient:
    def __init__(self, base_url=None):
        self._base_url = 'https://ftx.com/api/'
        self.name = 'FTX'

    @staticmethod
    def parse(asset):
        return f'{asset}-PERP'

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
        funding_rates.reverse()
        return funding_rates


class BinanceClient:
    def __init__(self, base_url=None):
        self._base_url = 'https://www.binance.com/fapi/v1/'
        self._base_url_usd = 'https://www.binance.com/dapi/v1/'
        self.name = 'BINANCE'

    @staticmethod
    def parse(asset):
        return f'{asset}USDT'

    def get_all_futures(self):
        url = f'{self._base_url}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return futures_names

    def get_all_futures_usd(self):
        url = f'{self._base_url_usd}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return futures_names

    # UTC -3, need ajusts
    def get_historical_funding_rates(self, future, start, end):
        url = f'{self._base_url}fundingRate?symbol={future}&startTime={start}&endTime={end}'
        print(url)
        response = json.loads((requests.get(url)).content)
        funding_rates = [{'time': (datetime.utcfromtimestamp(item['fundingTime']/1000)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['fundingRate'])} for item in response]
        return funding_rates

    # UTC -3, need ajusts
    def get_historical_funding_rates_usd(self, future, start, end):
        url = f'{self._base_url_usd}fundingRate?symbol={future}&startTime={start}&endTime={end}'
        print(url)
        response = json.loads((requests.get(url)).content)
        funding_rates = [{'time': (datetime.fromtimestamp(item['fundingTime']/1000) + timedelta(hours=3)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['fundingRate'])} for item in response]
        return funding_rates


class PerpetualClient:
    def __init__(self, base_url=None):
        self._base_url = 'https://api.thegraph.com/subgraphs/name/perpetual-protocol/perp-position-subgraph'
        transport = AIOHTTPTransport(url=self._base_url)
        self._client = Client(transport=transport,
                              fetch_schema_from_transport=True)
        self.name = 'PERPETUAL'

    def get_all_futures(self):
        url = f'{self._base_url}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return futures_names

    def get_historical_funding_rates(self, future, start, end):
        query = """
                    {
                        fundingRateUpdatedEvents(
                            where: {timestamp_gte: {start}, timestamp_lt: {end} }
                            orderBy: blockNumber
                            orderDirection: desc
                        ) {
                            id
                            amm
                            rate
                            underlyingPrice
                    timestamp
                        }
                    }
                """
        query_gql = gql(query.format(start=start, end=end))
        result = self._client.execute(query_gql)
        response = result.get(['fundingRateUpdatedEvents'])
        funding_rates = [{'time': (datetime.utcfromtimestamp(int(item['timestamp']))).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['rate'])/1e18} for item in response]
        return funding_rates
