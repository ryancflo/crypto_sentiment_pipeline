from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.hooks.base_hook import BaseHook
import tweepy
from tweepy import Client
import json
import datetime as dt
from datetime import datetime, timedelta

class twitterHook(BaseHook):

    def __init__(self, twitter_conn_id='twitter_conn_id'):
        self.connection = self.get_connection(twitter_conn_id)
        self.bearer_token = self.connection.extra_dejson.get('bearer_token')


    def getTweetsData(self,
                CURRENCY,
                CURRENCY_SYMBOL,
                limit,
                parameters=None,
                headers=None):

        client = tweepy.Client(bearer_token=self.bearer_token)
        reponse_data = self.parseTweets(client, CURRENCY, CURRENCY_SYMBOL, limit)
        return reponse_data


    def parseTweets(self, client, CURRENCY, CURRENCY_SYMBOL, limit):
        tweets = []
        query = f'#{CURRENCY} OR #{CURRENCY_SYMBOL}'

        tweets = client.search_recent_tweets(query = query,                             
                                    tweet_fields = ['id','created_at', 'public_metrics', 'text', 'source', 'geo', 'lang', 'entities'],
                                    max_results = limit)
                                    
        return tweets
