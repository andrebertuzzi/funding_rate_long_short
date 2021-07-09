import requests
import json
from datetime import datetime, timedelta, time
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

            sign = hmac.new(bytes(self._secret, 'utf-8'),
                            bytes(payload, 'utf-8'), hashlib.sha256).hexdigest()

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
                response = requests.get(url, headers=headers).json()
            elif method == 'POST':
                response = requests.post(
                    url, headers=headers, json=query).json()
            elif method == 'DELETE':
                if query == {}:
                    response = requests.delete(url, headers=headers).json()
                else:
                    response = requests.delete(
                        url, headers=headers, json=query).json()
        except Exception as e:
            print('[x] Error: {}'.format(e.args[0]))

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

    # PRIVATE FUNCTIONS
    def get_private_funding_payments(self, future=None, start_time=None, end_time=None):
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

        if future != None:
            query.update({
                'future': future
            })

        payments = self._send_request(
            'private', 'GET', f"funding_payments", query)

        return [{'exchange': self.name, 'asset': self.get_asset(future), 'future': future, 'type': 'PAYMENTS', 'type': 'PAYMENTS',
                 'time': x.get('time'), 'rate': x.get('rate'), 'payment': x.get('payment'), 'notional': x.get('payment')/x.get('rate') if x.get('rate') != 0 else 0} for x in payments]


class BinanceUSDTClient(Exchange):
    def __init__(self, key=None, secret=None, base_url=None):
        self._base_url = 'https://www.binance.com/fapi/v1/'
        self.name = 'BINANCE-USDT'
        self._key = key
        self._secret = secret
        self.session = self._init_session()

    def _init_session(self):

        session = requests.session()
        session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'binance/python',
                                'X-MBX-APIKEY': self._key})
        return session

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

    # PRIVATE FUNCTIONS
    def get_private_funding_payments(self, future=None, start_time=None, end_time=None):
        """Get income history for authenticated account

        https://binance-docs.github.io/apidocs/futures/en/#get-income-history-user_data

        """

        fundings = self.get_historical_funding_rates(
            future, start_time, end_time)

        uri = 'https://fapi.binance.com/fapi/v1/income'

        params = {'symbol': future, 'startTime': start_time,
                  'endTime': end_time, 'incomeType': 'FUNDING_FEE'}
        data = {'data': params}
        payments = self._request('get', uri, True, True, **data)
        payments_response = [{'exchange': self.name, 'asset': self.get_asset(future), 'future': future, 'type': 'PAYMENTS',
                              'time': (datetime.utcfromtimestamp(x.get('time')/1000)).strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'), 'payment': float(x.get('income'))} for x in payments]

        for payment in payments_response:
            for funding in fundings:
                if(payment['time'].split(':')[0] == funding['time'].split(':')[0]):
                    payment['rate'] = funding['rate']
                    payment['notional'] = abs(
                        payment['payment'] / funding['rate']) if funding['rate'] != 0 else 0

        return [payment for payment in payments_response if 'rate' in payment.keys()]

    def _create_website_uri(self, path):
        return self.WEBSITE_URL + '/' + path

    def _order_params(self, data):
        """Convert params to list with signature as last element

        :param data:
        :return:

        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, value))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _request(self, method, uri, signed, force_params=False, **kwargs):

        # set default requests timeout
        kwargs['timeout'] = 10

        # add our global requests params
        # if self._requests_params:
        #     kwargs.update(self._requests_params)

        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data

            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del(kwargs['data']['requests_params'])

        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(_time() * 1000)
            kwargs['data']['signature'] = self._generate_signature(
                kwargs['data'])

        # sort get and post params to match signature order
        if data:
            # sort post params
            kwargs['data'] = self._order_params(kwargs['data'])
            # Remove any arguments with values of None.
            null_args = [i for i, (key, value) in enumerate(
                kwargs['data']) if value is None]
            for i in reversed(null_args):
                del kwargs['data'][i]

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = '&'.join('%s=%s' % (
                data[0], data[1]) for data in kwargs['data'])
            del(kwargs['data'])

        self.response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response()

    def _handle_response(self):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(self.response.status_code).startswith('2'):
            pass
        try:
            return self.response.json()
        except ValueError:
            pass

    def _generate_signature(self, data):
        ordered_data = self._order_params(data)
        query_string = '&'.join(["{}={}".format(d[0], d[1])
                                for d in ordered_data])
        m = hmac.new(self._secret.encode('utf-8'),
                     query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()


class itemgetter:
    """
    Return a callable object that fetches the given item(s) from its operand.
    After f = itemgetter(2), the call f(r) returns r[2].
    After g = itemgetter(2, 5, 3), the call g(r) returns (r[2], r[5], r[3])
    """
    __slots__ = ('_items', '_call')

    def __init__(self, item, *items):
        if not items:
            self._items = (item,)

            def func(obj):
                return obj[item]
            self._call = func
        else:
            self._items = items = (item,) + items

            def func(obj):
                return tuple(obj[i] for i in items)
            self._call = func

    def __call__(self, obj):
        return self._call(obj)

    def __repr__(self):
        return '%s.%s(%s)' % (self.__class__.__module__,
                              self.__class__.__name__,
                              ', '.join(map(repr, self._items)))

    def __reduce__(self):
        return self.__class__, self._items


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

    def get_private_funding_payments(self, future=None, start_time=None, end_time=None):
        try:
            contract = next(
                filter(lambda x: future in x.get('name'), perp_contracts), None)
            query = """
                        {{
                            {"operationName":"QueryTraderPositionLogsHistory","variables":
                                {"traderAddress":"0x470caaD12Bc7C0995c64A4194D6bFFe49844bE23","limit":99999,"eventType":"FundingRateUpdated","nextToken":null},
                                "query":"query QueryTraderPositionLogsHistory($traderAddress: String!, $eventType: String, $limit: Int, $nextToken: String) 
                                {  queryTraderPositionLogsHistory(traderAddr: $traderAddress, filter: {eventType: {eq: $eventType}}, limit: $limit, nextToken: $nextToken) 
                                {    items {      
                                    traderAddr
                                    ammAddr
                                    blockNumber      
                                    timestamp      
                                    marketPair      
                                    eventType      
                                    positionNotional      
                                    exchangedPositionSize      
                                    fee      
                                    positionSizeAfter      
                                    realizedPnl      
                                    underlyingPrice      
                                    fundingRate      
                                    estimatedFundingPayment      
                                    txHash      
                                    __typename    
                                }    
                                nextToken    __typename  }}"}


​

                        }}
                    """
            query_gql = gql(query.format())
            result = self._client.execute(query_gql)
            response = result.get('positionChangedEvents')
            funding_rates = [{'time': (datetime.utcfromtimestamp(int(item['timestamp']))).strftime(
                '%Y-%m-%dT%H:%M:%S+00:00'), 'rate': float(item['rate'])/1e18} for item in response]
        except Exception as ex:
            print(ex)
            funding_rates = []
        return funding_rates
