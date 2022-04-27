from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from custom_operators.reddit_toAzureDataLake import reddit_toAzureDataLakeOperator
from custom_transfers.azureDataLake_toSnowflake import AzureDataLakeToSnowflakeTransferOperator
from custom_transfers.load_toSnowflakeTables import LoadToSnowflakeOperator
from custom_operators.data_quality import DataQualityOperator
from helpers.sql_queries import SQLQueries

AZURE_CONN_ID = 'azure_conn_id'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.today()
d = datetime.today() - timedelta(days = 1)
start_epoch = int(d.timestamp())
# end_epoch = int(dt.datetime(2021, 4, 12).timestamp())

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 31, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'reddit_dag',
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

reddit_toAzureDataLake = reddit_toAzureDataLakeOperator(
    task_id='reddit_toAzureDataLake',
    dag=dag,
    reddit_conn_id = 'reddit_conn_id',
    azure_conn_id=AZURE_CONN_ID,
    start_epoch = start_epoch,
    subreddit = "Cryptocurrency",
    current_time = CURRENT_TIME,
    limit = 100,
    ignore_headers=1
)

azure_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
    task_id='azure_snowflake',
    dag=dag,
    azure_keys=['{0}/{1}/{2}/{3}/{4}.csv'.format("data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
    stage='MY_AZURE_REDDIT_STAGE',
    table='staging_reddit',
    file_format='REDDIT_FILEFORMAT',
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

data_quality_check = DataQualityOperator(
    task_id='data_quality',
    dag=dag,
    tables=['STAGING_REDDIT'],
    where_parameters = None,
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

loadReddit_toSnowflakeFinalTables = LoadToSnowflakeOperator(
    task_id='loadReddit_toSnowflakeFinalTables',
    dag=dag,
    select_query = SQLQueries.select_redditData_from_redditStaging,
    target_table='REDDIT_FINAL',
    source_table = 'STAGING_REDDIT',
    truncate_table = True,
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> reddit_toAzureDataLake >> azure_toSnowflake >> data_quality_check >> loadReddit_toSnowflakeFinalTables >> end_operator