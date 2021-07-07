import requests
import json
from datetime import datetime, timedelta
from time import time as _time
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from utils.constants import PERPETUAL_CONTRACTS as perp_contracts
from urllib.parse import urlencode
import urllib
import hashlib
import hmac


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
    def __init__(self, key=None, secret=None, account=None, base_url=None):
        self._base_url = 'https://ftx.com/api/'
        self.PUBLIC_API_URL = 'https://ftx.com/api'
        self.PRIVATE_API_URL = 'https://ftx.com/api'
        self.name = 'FTX'
        self._key = key
        self._secret = secret
        self._account = account

    def _build_headers(self, scope, method, endpoint, query=None):
        if query is None:
            query = {}

        endpoint = '/api/' + endpoint

        headers = {
            'Accept': 'application/json',
            'User-Agent': 'FTX-Trader/1.0',
        }

        if scope.lower() == 'private':
            nonce = str(self.get_current_timestamp())
            payload = f'{nonce}{method.upper()}{endpoint}'

            if method is 'GET' and query:
                payload += '?' + urlencode(query, safe='/')
            elif query:
                payload += json.dumps(query)
 
            sign = hmac.new(bytes(self._secret, 'utf-8'), bytes(payload, 'utf-8'), hashlib.sha256).hexdigest()

            headers.update({
                # This header is REQUIRED to send JSON data.
                'Content-Type': 'application/json',
                'FTX-KEY': self._key,
                'FTX-SIGN': sign,
                'FTX-TS': nonce
            })

            if self._account:
                headers.update({
                # If you want to access a subaccount 
                'FTX-SUBACCOUNT': urllib.parse.quote(self._account)
            })

        return headers

    def _build_url(self, scope, method, endpoint, query=None):
        if query is None:
            query = {}

        if scope.lower() == 'private':
            url = f"{self.PRIVATE_API_URL}/{endpoint}"
        else:
            url = f"{self.PUBLIC_API_URL}/{endpoint}"

        if method == 'GET':
            return f"{url}?{urlencode(query, True, '/[]')}" if len(query) > 0 else url
        else:
            return url

    def _send_request(self, scope, method, endpoint, query=None):
        if query is None:
            query = {}

        # Build header first
        headers = self._build_headers(scope, method, endpoint, query)

        # Build final url here
        url = self._build_url(scope, method, endpoint, query)

        try:
            if method == 'GET':
                response = requests.get(url, headers = headers).json()
            elif method == 'POST':
                response = requests.post(url, headers = headers, json = query).json()
            elif method == 'DELETE':
                if query == {}:
                    response = requests.delete(url, headers = headers).json()
                else:
                    response = requests.delete(url, headers = headers, json = query).json()
        except Exception as e:
            print ('[x] Error: {}'.format(e.args[0]))

        if 'result' in response:
            return response['result']
        else:
            return response

    @staticmethod
    def parse(asset):
        return f'{asset}-PERP'

    @staticmethod
    def get_asset(future):
        return future.replace('-PERP', '')
    
    @staticmethod
    def get_current_timestamp():
        return int(round(_time() * 1000))

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

    ## PRIVATE
    def get_private_funding_payments(self, coin=None, start_time=None, end_time=None):
        """
        https://docs.ftx.com/#funding-payments

        :param coin: the trading coin to query
        :param start_time: the target period after an Epoch time in seconds
        :param end_time: the target period before an Epoch time in seconds
        :return: a list contains all funding payments of perpetual future
        """

        query = {}

        if start_time != None:
            query.update({ 
                'start_time': start_time,
            })
        
        if end_time != None:
            query.update({ 
                'end_time': end_time,
            })

        if coin != None:
            query.update({ 
                'future': coin.upper() + '-PERP'
            })

        return self._send_request('private', 'GET', f"funding_payments", query)




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

    # List position changes
    # {
    # positionChangedEvents(where: {trader:"0x28B572F7f1Ac8cd8A01864fe39Ca5b81FE671855"}, first: 5, orderBy: blockNumber, orderDirection: desc) {
    #     id
    #     trader
    #     amm
    #     margin
    #     positionNotional
    #     exchangedPositionSize
    #     fee
    #     positionSizeAfter
    #     realizedPnl
    #     unrealizedPnlAfter
    #     badDebt
    #     liquidationPenalty
    #     spotPrice
    #     fundingPayment
    #     }
    # }
