from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from custom_operators.binance_toAzureDataLake import binance_toAzureDataLakeOperator
from custom_transfers.azureDataLake_toSnowflake import AzureDataLakeToSnowflakeTransferOperator
from custom_operators.data_quality import DataQualityOperator

AZURE_CONN_ID = 'azure_conn_id'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.today()
d = datetime.today() - timedelta(days = 1)
start_epoch = int(d.timestamp())
# end_epoch = int(dt.datetime(2021, 4, 12).timestamp())

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'binance_dag',
          default_args=default_args,
          description='Load and transform data in Azure with Airflow',
          max_active_runs=1,
        #   schedule_interval=timedelta(minutes=60),
          schedule_interval='0 * * * *'
        )

def print_params_fn():
    import logging
    logging.info('hello')
    return None

start_operator = PythonOperator(task_id='Begin_execution',  python_callable=print_params_fn, provide_context=True, dag=dag, )

binance_toAzureDataLake = binance_toAzureDataLakeOperator(
    task_id='binance_toAzureDataLake',
    dag=dag,
    binance_conn_id = 'binance_conn_id',
    azure_conn_id=AZURE_CONN_ID,
    symbol = "BTCUSDT",
    time_interval = "1h",
    current_time = datetime.today(),
    limit = 10,
    ignore_headers=1
)

azure_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
    task_id='azure_snowflake',
    dag=dag,
    azure_keys=['{0}/{1}/{2}/{3}/{4}.csv'.format("data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
    stage='MY_AZURE_BINANCE_STAGE',
    table='STAGING_BINANCE',
    file_format='BINANCE_FILEFORMAT',
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

data_quality_check = DataQualityOperator(
    task_id='data_quality',
    dag=dag,
    tables=['STAGING_BINANCE'],
    where_parameters = None,
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> binance_toAzureDataLake >> azure_toSnowflake >> data_quality_check >> end_operator