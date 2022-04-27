from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.hooks.base_hook import BaseHook
from binance.spot import Spot as Client
import json

class binanceHook(BaseHook):
    
    def __init__(self, binance_conn_id='binance_conn_id'):
        self.base_url = 'https://pro-api.binance.com'
        self.connection = self.get_connection(binance_conn_id)
        self.API_KEY = self.connection.extra_dejson.get('API_KEY')

    def getData(self,
                symbol,
                time_interval,
                limit,
                parameters=None,
                headers=None):

        '''
        Args:
        symbol (str): the trading pair
        interval (str): the interval of kline, e.g 1m, 5m, 1h, 1d, etc.

        Keyword Args:
        limit (int, optional): limit the results. Default 500; max 1000.
        startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
        endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
        '''

        try:
            spot_client = Client(base_url="https://testnet.binance.vision")
            data = spot_client.klines(symbol, time_interval, limit = limit)

            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)