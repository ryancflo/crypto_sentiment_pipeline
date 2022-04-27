from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from custom_operators.twitter_toAzureDataLake import twitter_toAzureDataLakeOperator
from custom_transfers.azureDataLake_toSnowflake import AzureDataLakeToSnowflakeTransferOperator
from custom_transfers.load_toSnowflakeTables import LoadToSnowflakeOperator
from custom_operators.data_quality import DataQualityOperator
from helpers.sql_queries import SQLQueries

AZURE_CONN_ID = 'azure_conn_id'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.today()
d = datetime.today() - timedelta(days = 1)
start_epoch = int(d.timestamp())


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 31, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'twitter_dag',
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

twitter_toAzureDataLake = twitter_toAzureDataLakeOperator(
    task_id='twitter_toAzureDataLake',
    dag=dag,
    twitter_conn_id = 'twitter_conn_id',
    CURRENCY = "bitcoin",
    CURRENCY_SYMBOL = "BTC",
    azure_conn_id=AZURE_CONN_ID,
    start_epoch = start_epoch,
    current_time = datetime.today(),
    limit = 100,
    ignore_headers=1
)

with dag as dag:
    with TaskGroup(group_id='load_toSnowflakeStaging') as load_toSnowflakeStaging:
        azure_tweet_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
            task_id='azure_tweet_snowflake',
            dag=dag,
            azure_keys=['{0}/{1}/{2}/{3}/{4}.csv'.format("tweet_data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
            stage='MY_AZURE_TWITTER_STAGE',
            table='STAGING_TWITTER_TWEET',
            file_format='TWITTER_FILEFORMAT',
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        azure_hashtags_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
            task_id='azure_hashtags_snowflake',
            dag=dag,
            azure_keys=['{0}/{1}/{2}/{3}/{4}.csv'.format("hashtags_data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
            stage='MY_AZURE_TWITTER_STAGE',
            table='STAGING_TWITTER_HASHTAGS',
            file_format='TWITTER_FILEFORMAT',
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        [azure_tweet_toSnowflake, azure_hashtags_toSnowflake]

data_quality_check = DataQualityOperator(
    task_id='data_quality',
    dag=dag,
    tables=['STAGING_TWITTER_TWEET', 'STAGING_TWITTER_HASHTAGS'],
    where_parameters = None,
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)

with dag as dag:
    with TaskGroup(group_id='final_load') as final_load:
        loadTweet_toSnowflakeFinalTables = LoadToSnowflakeOperator(
            task_id='loadTweet_toSnowflakeFinalTables',
            dag=dag,
            select_query = SQLQueries.select_twitterData_from_twitterTweetStaging,
            target_table='TWITTER_TWEET_FINAL',
            source_table = 'STAGING_TWITTER_TWEET',
            truncate_table = True,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        loadHashtags_toSnowflakeFinalTables = LoadToSnowflakeOperator(
            task_id='loadHashtags_toSnowflakeFinalTables',
            dag=dag,
            select_query = SQLQueries.select_twitterData_from_twitterHashtagsStaging,
            target_table='TWITTER_HASHTAGS_FINAL',
            source_table = 'STAGING_TWITTER_HASHTAGS',
            truncate_table = True,
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        [loadTweet_toSnowflakeFinalTables, loadHashtags_toSnowflakeFinalTables]

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> twitter_toAzureDataLake >> load_toSnowflakeStaging >> data_quality_check >> final_load >> end_operator