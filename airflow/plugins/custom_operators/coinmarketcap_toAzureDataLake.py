from datetime import datetime, timedelta
import logging
from os import path 
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.coinmarketcap_hook import coinmarketcapHook
import pandas as pd
import requests
import json
from tempfile import NamedTemporaryFile

class coinmarketcap_toAzureDataLakeOperator(BaseOperator):
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
                 coinmarketcap_conn_id = "",
                 azure_blob="",
                 azure_conn_id="",
                 azure_key="",
                 category = "",
                 subcategory = "",
                 subsub = "",
                 current_time=None,
                 ignore_headers=1,
                 *args, **kwargs):

        super(coinmarketcap_toAzureDataLakeOperator, self).__init__(*args, **kwargs)

        self.coinmarketcap_conn_id = coinmarketcap_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.azure_key = azure_key
        self.category = category
        self.subcategory = subcategory
        self.subsub = subsub
        self.current_time = current_time
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
        
        coinmarketcap_hook = coinmarketcapHook(self.coinmarketcap_conn_id)
        self.log.info("Created Coinmarketcap Connection")
        response_data = coinmarketcap_hook.getData(self.category, self.subcategory, self.subsub)
            
        self.log.info(response_data)

        # Write ratings to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "reponse_data.json")
            with open(tmp_path, 'w') as fp:
                json.dump(response_data, fp)

            wasb_hook.load_file(
                tmp_path,
                container_name="coinmarketcap",
                blob_name="data/{year}/{month}/{day}/{hour}.json".format(year=self.current_time.year,
                                                                        month=self.current_time.month,
                                                                        day=self.current_time.day,
                                                                        hour=self.current_time.hour),
            )

        