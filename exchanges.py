import requests
import json
from datetime import datetime, timedelta
import time
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from utils.constants import PERPETUAL_CONTRACTS as perp_contracts


class Exchange:
    def get_fundings(self, futures, start, end, increment=20):
        response = {}
        print(futures)
        for future in futures:
            final = []
            temp_start = start
            temp_end = end
            while(temp_start < end):
                temp_end = temp_start + timedelta(days=increment)
                print(f'{future} Start {temp_start} Ending {temp_end}')
                funding_rates = self.get_historical_funding_rates(
                    future, temp_start.timestamp(), temp_end.timestamp())
                final = final + funding_rates
                temp_start = temp_end
                time.sleep(5)
            response[future] = final
        return response


class FtxClient(Exchange):
    def __init__(self, base_url=None):
        self._base_url = 'https://ftx.com/api/'
        self.name = 'FTX'

    @staticmethod
    def parse(asset):
        return f'{asset}-PERP'

    @staticmethod
    def get_asset(future):
        return future.replace('-PERP', '')

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


class BinanceUSDTClient(Exchange):
    def __init__(self, base_url=None):
        self._base_url = 'https://www.binance.com/fapi/v1/'
        self.name = 'BINANCE-USDT'

    @staticmethod
    def parse(asset):
        return f'{asset}USDT'

    @staticmethod
    def get_asset(future):
        return future.replace('USDT', '')

    def get_all_futures(self):
        url = f'{self._base_url}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return list(filter(lambda x: '_' not in x, futures_names))

    # UTC -3, need ajusts
    def get_historical_funding_rates(self, future, start, end):
        url = f'{self._base_url}fundingRate?symbol={future}&startTime={int(start*1000)}&endTime={int(end*1000)}'
        response = json.loads((requests.get(url)).content)
        funding_rates = [{'time': (datetime.utcfromtimestamp(item['fundingTime']/1000)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['fundingRate'])} for item in response]
        return funding_rates


class BinanceUSDClient(Exchange):
    def __init__(self, base_url=None):
        self._base_url = 'https://www.binance.com/dapi/v1/'
        self.name = 'BINANCE-USD'

    @staticmethod
    def parse(asset):
        return f'{asset}USD_PERP'

    @staticmethod
    def get_asset(future):
        return future.replace('USD_PERP', '')

    def get_all_futures(self):
        url = f'{self._base_url}ticker/24hr'
        response = json.loads((requests.get(url)).content)
        futures_names = [symbol['symbol'] for symbol in response]
        return list(filter(lambda x: 'PERP' in x, futures_names))

    # UTC -3, need ajusts
    def get_historical_funding_rates(self, future, start, end):
        url = f'{self._base_url}fundingRate?symbol={future}&startTime={int(start*1000)}&endTime={int(end*1000)}'
        print(url)
        response = json.loads((requests.get(url)).content)
        funding_rates = [{'time': (datetime.utcfromtimestamp(item['fundingTime']/1000)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['fundingRate'])} for item in response]
        return funding_rates


class PerpetualClient(Exchange):
    def __init__(self, base_url=None):
        self._base_url = 'https://api.thegraph.com/subgraphs/name/perpetual-protocol/perp-position-subgraph'
        transport = AIOHTTPTransport(url=self._base_url)
        self._client = Client(transport=transport,
                              fetch_schema_from_transport=True)
        self.name = 'PERPETUAL'

    @staticmethod
    def parse(asset):
        return f'Amm{asset}USDC'

    @staticmethod
    def get_asset(future):
        return future.replace('Amm', '').replace('USDC', '')

    def get_all_futures(self):
        return [x.get('name') for x in perp_contracts]

    def get_historical_funding_rates(self, future, start, end):
        try:
            contract = next(
                filter(lambda x: future in x.get('name'), perp_contracts), None)
            query = """
                        {{
                            fundingRateUpdatedEvents(
                                where: {{ timestamp_gte: {start}, timestamp_lt: {end}, amm: "{contract}" }}
                                orderBy: blockNumber
                                orderDirection: asc
                            ) {{
                                id
                                amm
                                rate
                                underlyingPrice
                        timestamp
                            }}
                        }}
                    """
            query_gql = gql(query.format(start=int(start), end=int(
                end), contract=str(contract.get('contract'))))
            result = self._client.execute(query_gql)
            response = result.get('fundingRateUpdatedEvents')
            funding_rates = [{'time': (datetime.utcfromtimestamp(int(item['timestamp']))).strftime(
                '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['rate'])/1e18} for item in response]
        except:
            funding_rates = []
        return funding_rates
