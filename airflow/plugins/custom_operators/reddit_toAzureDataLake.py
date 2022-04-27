from datetime import datetime, timedelta
import logging
from os import path 
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.reddit_hook import redditHook
import pandas as pd
import requests
import json
from tempfile import NamedTemporaryFile
from custom_scripts.vaderSentiment import SentimentIntensityAnalyzer

class reddit_toAzureDataLakeOperator(BaseOperator):
    """
    Reddit To Azure DataLake Operator
    :param reddit_conn_id:              The source reddit connection id.
    :type azure_conn_id::               string
    :param azure_conn_id::              The destination azure connection id.
    :type azure_conn_id::               string
    :param azure_bucket:                The destination azure bucket.
    :type azure_bucket:                 string
    :param azure_key:                   The destination azure key.
    :type azure_key:                    string
    :param start_epoch:                 start_time of the query.
    :type start_epoch:                  string
    :param end_epoch:                   end_time of the query.
    :type end_epoch:                    string
    :param subreddit:                   Subreddit to pull data from.
    :type subreddit:                    string
    :param limit:                       Post limit.
    :type limit:                        string
    """
    @apply_defaults
    def __init__(self,
                 reddit_conn_id = "",
                 azure_blob="",
                 azure_conn_id="",
                 azure_key="",
                 start_epoch="",
                 subreddit="",
                 current_time=None, 
                 limit="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(reddit_toAzureDataLakeOperator, self).__init__(*args, **kwargs)

        self.reddit_conn_id = reddit_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.azure_key = azure_key
        self.start_epoch = start_epoch
        self.subreddit = subreddit
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
        
        reddit_hook = redditHook(self.reddit_conn_id)
        self.log.info("Created Reddit Connection")
        response_data = reddit_hook.getData(
            start_epoch = self.start_epoch,
            subreddit = self.subreddit, 
            limit = self.limit)

        sub_df = self.redditETL(response_data)

        # Write response data to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "response_data.csv")
            sub_df.to_csv(tmp_path, header=True, index=False, columns=list(sub_df.axes[1]))
            self.log.info(sub_df)

            # Upload file to Azure Blob.
            wasb_hook.load_file(
                tmp_path,
                container_name="reddit",
                blob_name="data/{year}/{month}/{day}/{hour}.csv".format(year=self.current_time.year,
                                                                        month=self.current_time.month,
                                                                        day=self.current_time.day,
                                                                        hour=self.current_time.hour),
            )

    def redditETL(self, submission):
        result = []
        analyzer = SentimentIntensityAnalyzer()
        
        #function to unpack the tweets list into a dataframe
        #analyze polarity score
        for post in submission:
            score = analyzer.polarity_scores(post['title'])
            result.append({'author': post['author'],
                        'created_utc': post['created_utc'],
                        'reddit_post_id' :post['id'],
                        'title': post['title'],
                        'neu': score['neu'],
                        'pos': score['pos'],
                        'neg': score['neg'],
                        'compound': score['compound']
                        })

        df = pd.DataFrame(result)
        print(df)
        return df