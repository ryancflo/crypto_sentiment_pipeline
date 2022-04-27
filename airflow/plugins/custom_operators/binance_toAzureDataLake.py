from datetime import datetime, timedelta
import logging
from os import path 
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.binance_hook import binanceHook
import pandas as pd
import requests
import json
from tempfile import NamedTemporaryFile

class binance_toAzureDataLakeOperator(BaseOperator):
    """
    Coinmarketcap To Azure DataLake Operator
    :param coinmarketcap_conn_id:       The source coinmarketcap connection id.
    :type azure_conn_id::               string
    :param azure_conn_id::              The destination azure connection id.
    :type azure_conn_id::               string
    :param azure_bucket:                The destination azure bucket.
    :type azure_bucket:                 string
    :param azure_key:                   The destination azure key.
    :type azure_key:                    string
    :param category:                    Category URL.
    :type category:                     string
    :param subcategory:                 SubCategory URL.
    :type subcategory:                  string
    :param subsub:                      SubSub URL.
    :type subsub:                       string
    """
    @apply_defaults
    def __init__(self,
                 binance_conn_id = "",
                 azure_blob="",
                 azure_conn_id="",
                 azure_key="",
                 symbol = "",
                 time_interval = "",
                 current_time = None,
                 limit = "",
                 ignore_headers=1,
                 *args, **kwargs):

        super(binance_toAzureDataLakeOperator, self).__init__(*args, **kwargs)

        self.binance_conn_id = binance_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.azure_key = azure_key
        self.symbol = symbol
        self.time_interval = time_interval
        self.current_time = current_time
        self.limit = limit
        self.ignore_headers = ignore_headers


    def execute(self, context):
        self.log.info('StageToAzureLake not implemented yet')
        self.upload_to_azureLake()
        self.log.info("Upload to azure!")

    def upload_to_azureLake(self):

        # Create azure Connection
        wasb_hook = WasbHook(self.azure_conn_id)
        self.log.info(wasb_hook.get_conn)
        self.log.info("Created Azure Connection")
        
        binance_hook = binanceHook(self.binance_conn_id)
        self.log.info("Created Coinmarketcap Connection")
        response_data = binance_hook.getData(self.symbol, self.time_interval, self.limit)
            
        self.log.info(response_data)

        # Write ratings to temp file.
        sub_df = pd.DataFrame(response_data, columns =[
                                                    'Open_time',
                                                    'Open',
                                                    'High',
                                                    'Low',
                                                    'Close',
                                                    'Volume',
                                                    'Close_time',
                                                    'Quote_asset_volume',
                                                    'Number_of_trades',
                                                    'Taker_buy_base_asset',
                                                    'Taker_buy_quote_asset',
                                                    'Ignore'
                                                    ])

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "response_data.csv")
            sub_df.to_csv(tmp_path, header=True, index=False, columns=list(sub_df.axes[1]))
            self.log.info(sub_df)

            wasb_hook.load_file(
                tmp_path,
                container_name="binance",
                blob_name="data/{year}/{month}/{day}/{hour}.csv".format(year=self.current_time.year,
                                                                        month=self.current_time.month,
                                                                        day=self.current_time.day,
                                                                        hour=self.current_time.hour),
            )

        