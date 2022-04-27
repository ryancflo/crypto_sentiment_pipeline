from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.hooks.base_hook import BaseHook
import json

class coinmarketcapHook(BaseHook):
    
    def __init__(self, coinmarketcap_conn_id='coinmarketcap_conn_id'):
        self.base_url = 'https://pro-api.coinmarketcap.com'
        self.connection = self.get_connection(coinmarketcap_conn_id)
        self.API_KEY = self.connection.extra_dejson.get('API_KEY')
        # self.API_KEY = "0279083e-0cbe-4fae-8205-3c689b98b1a5"

    def getData(self,
                category,
                subcategory,
                subsub,
                parameters=None,
                headers=None):

        url = 'pro-api.coinmarketcap.com'
        parameters = {
        'start':'1',
        'limit':'150',
        'convert':'USD'
        }
        headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': self.API_KEY,
        }
        
        self.log.info(self.API_KEY)
        session = Session()
        session.headers.update(headers)

        url_text = "https://{base_url}/v1/{category}/{subcategory}/{subsub}".format(
            base_url=url,
            category=category,
            subcategory=subcategory,
            subsub=subsub
            )

        try:
            response = session.get(url_text, params=parameters)
            response.raise_for_status()

            data = json.loads(response.text)
            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)