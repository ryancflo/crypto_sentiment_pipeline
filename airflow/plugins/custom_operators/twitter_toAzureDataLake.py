from datetime import datetime, timedelta
import logging
from os import path 
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.twitter_hook import twitterHook
import pandas as pd
import requests
import json
import re
from tempfile import NamedTemporaryFile
from custom_scripts.vaderSentiment import SentimentIntensityAnalyzer

class twitter_toAzureDataLakeOperator(BaseOperator):
    """
    Twitter To Azure DataLake Operator
    :param twitter_conn_id:             The source twitter connection id.
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
    :param CURRENCY:                    CURRENCY to be queried
    :type CURRENCY:                     string
    :param CURRENCY_SYMBOL:             CURRENCY_SYMBOL to be queried
    :type CURRENCY_SYMBOL:              string  
    :param limit:                       Post limit.
    :type limit:                        string
    """
    @apply_defaults
    def __init__(self,
                 twitter_conn_id = "",
                 azure_blob="",
                 azure_conn_id="",
                 start_epoch="",
                 CURRENCY = "",
                 CURRENCY_SYMBOL = "",
                 limit="",
                 current_time=None,
                 ignore_headers=1,
                 *args, **kwargs):

        super(twitter_toAzureDataLakeOperator, self).__init__(*args, **kwargs)

        self.twitter_conn_id = twitter_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.start_epoch = start_epoch
        self.CURRENCY = CURRENCY
        self.CURRENCY_SYMBOL = CURRENCY_SYMBOL
        self.limit = limit
        self.current_time = current_time
        self.ignore_headers = ignore_headers


    def execute(self, context):
        self.log.info('StageToAzureLake not implemented yet')
        self.upload_to_azureLake()
        self.log.info("Upload twitter data to Azure!")

    def upload_to_azureLake(self):

        # Create azure Connection
        wasb_hook = WasbHook(self.azure_conn_id)
        self.log.info(wasb_hook.get_conn)
        self.log.info("Created Azure Connection")
        
        twitter_hook = twitterHook(self.twitter_conn_id)
        self.log.info("Created Twitter Connection")
        response_data = twitter_hook.getTweetsData(
            CURRENCY = self.CURRENCY,
            CURRENCY_SYMBOL = self.CURRENCY_SYMBOL,
            limit = self.limit
        )

        tweet_df, hashtags_df = self.tweetsETL(response_data)

        # Write tweet data to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "tweet_data.csv")
            tmp_path2 = path.join(tmp_dir, "hashtag_data.csv")
            tweet_df.to_csv(tmp_path, header=True, index=False, columns=list(tweet_df.axes[1]))
            hashtags_df.to_csv(tmp_path2, header=True, index=False, columns=list(hashtags_df.axes[1]))

            # Upload file to Azure Blob.
            wasb_hook.load_file(
                tmp_path,
                container_name="twitter",
                blob_name="tweet_data/{year}/{month}/{day}/{hour}.csv".format(year=self.current_time.year,
                                                                        month=self.current_time.month,
                                                                        day=self.current_time.day,
                                                                        hour=self.current_time.hour),
            )

            wasb_hook.load_file(
                tmp_path2,
                container_name="twitter",
                blob_name="hashtags_data/{year}/{month}/{day}/{hour}.csv".format(year=self.current_time.year,
                                                                        month=self.current_time.month,
                                                                        day=self.current_time.day,
                                                                        hour=self.current_time.hour),
            )

    def tweetsETL(self, tweets):
        result = []
        hashtags = []
        analyzer = SentimentIntensityAnalyzer()
        i = 0
        #regex function to clean the tweet text from haashtags, mentions and links
        def cleanTweets(text):
            clean_text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",text).split())
            return clean_text

        def get_hashtag(tweet):
            tags = []
            tag = tweet.entities["hashtags"][0]['tag']
            # print(tweet.entities["hashtags"][0])

            if type(tag) == list:
                for j in range(0, len(tag)):
                    tags.append(tag[j])
            else: 
                tags.append(tag)
            
            return tags
        
        #function to unpack the tweets list into a dataframe
        #analyze polarity score
        for tweet in tweets.data:
            score = analyzer.polarity_scores(tweet.text)
            result.append({'tweet_id': tweet.id,
                        'text': tweet.text,
                        'clean_tweet' : cleanTweets(tweet.text),
                        'created_at': tweet.created_at,
                        'source': tweet.source,
                        'lang': tweet.lang,
                        'retweets': tweet.public_metrics['retweet_count'],
                        'replies': tweet.public_metrics['reply_count'],
                        'likes': tweet.public_metrics['like_count'],
                        'quote_count': tweet.public_metrics['quote_count'],
                        'neu': score['neu'],
                        'pos': score['pos'],
                        'neg': score['neg'],
                        'compound': score['compound'],
                        'location': tweet.geo,
                        })

            if tweet.geo is not None:
                result[i]['latitude'] = tweet.geo['coordinates'][0]
                result[i]['longitude'] = tweet.geo['coordinates'][1]
            else:
                result[i]['location'] = None
                result[i]['latitude'] = None
                result[i]['longitude'] = None


            if tweet.entities.get("hashtags") is None:
                result[i]['hashtags'] = None
            else:
                result[i]['hashtags'] = get_hashtag(tweet)
                # print(result[i]['hashtags'])
                for value in result[i]['hashtags']:
                    # print(result[0])
                    temp = (result[i]['tweet_id'], value)
                    hashtags.append(temp)
            i+=1
            
        
        tweet_df = pd.DataFrame(result)
        hashtags_df = pd.DataFrame(hashtags)
        hashtags_df.columns = ['tweet_id' , 'hashtag']
        return tweet_df, hashtags_df

