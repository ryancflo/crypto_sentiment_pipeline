from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from custom_operators.coinmarketcap_toAzureDataLake import coinmarketcap_toAzureDataLakeOperator
from custom_transfers.azureDataLake_toSnowflake import AzureDataLakeToSnowflakeTransferOperator
from custom_transfers.load_toSnowflakeTables import LoadToSnowflakeOperator
from custom_operators.data_quality import DataQualityOperator
from helpers.sql_queries import SQLQueries

AZURE_CONN_ID = 'azure_conn_id'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.today()

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 31, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'coinmarketcap_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
          schedule_interval='0 * * * *'
        )

def print_params_fn():
    import logging
    logging.info('hello')
    return None

start_operator = PythonOperator(task_id='Begin_execution',  python_callable=print_params_fn, provide_context=True, dag=dag, )

coinmarketcap_toAzureDataLake = coinmarketcap_toAzureDataLakeOperator(
    task_id='coinmarketcap_toAzureDataLake',
    dag=dag,
    coinmarketcap_conn_id = 'coinmarketcap_conn_id',
    azure_conn_id=AZURE_CONN_ID,
    category = "cryptocurrency",
    subcategory = "listings",
    subsub = "latest",
    current_time = datetime.today(),
    ignore_headers=1
)

azure_coinmarketcap_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
    task_id='azure_coinmarketcap_snowflake',
    dag=dag,
    azure_keys=['{0}/{1}/{2}/{3}/{4}.json'.format("data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
    stage='MY_AZURE_COINMARKETCAP_STAGE',
    table='JSONSTAGING_COINMARKETCAP',
    file_format='COINMARKETCAP_FILEFORMAT',
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

with dag as dag:
    with TaskGroup(group_id='json_transform') as json_transform:
        jsonRawMarket_toSnowflake = LoadToSnowflakeOperator(
            task_id='coinmarketcapjsonRawMarket_toSnowflake',
            dag=dag,
            select_query = SQLQueries.select_coinmarketcapdata_from_jsonstaging,
            target_table='STAGING_COINMARKETCAP_MARKET',
            truncate_table = False,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        jsonRawTags_toSnowflake = LoadToSnowflakeOperator(
            task_id='coinmarketcapjsonRawTags_toSnowflake',
            dag=dag,
            select_query = SQLQueries.select_coinmarketcapdata_tags_from_jsonstaging,
            target_table='STAGING_COINMARKETCAP_TAGS',
            source_table = 'JSONSTAGING_COINMARKETCAP',
            truncate_table = True,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        jsonRawMarket_toSnowflake >> jsonRawTags_toSnowflake

data_quality_check = DataQualityOperator(
    task_id='data_quality',
    dag=dag,
    tables=['STAGING_COINMARKETCAP_MARKET', 'STAGING_COINMARKETCAP_TAGS'],
    where_parameters = None,
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

with dag as dag:
    with TaskGroup(group_id='final_load') as final_load:
        loadMarket_toSnowflakeFinalTables = LoadToSnowflakeOperator(
            task_id='loadMarket_toSnowflakeFinalTables',
            dag=dag,
            select_query = SQLQueries.select_coinmarketcapdata_from_StagingMarket,
            target_table='COINMARKETCAP_MARKET_FINAL',
            source_table = 'STAGING_COINMARKETCAP_MARKET',
            truncate_table = True,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        loadTags_toSnowflakeFinalTables = LoadToSnowflakeOperator(
            task_id='loadTags_toSnowflakeFinalTables',
            dag=dag,
            select_query = SQLQueries.select_coinmarketcapdata_from_StagingTags,
            target_table='COINMARKETCAP_TAGS_FINAL',
            source_table = 'STAGING_COINMARKETCAP_TAGS',
            truncate_table = True,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        [loadMarket_toSnowflakeFinalTables, loadTags_toSnowflakeFinalTables]

        

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> coinmarketcap_toAzureDataLake >> azure_coinmarketcap_toSnowflake >> json_transform >> data_quality_check >> final_load >> end_operator
