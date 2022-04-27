from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.hooks.base_hook import BaseHook
import json
import praw
from pmaw import PushshiftAPI
import datetime as dt
from datetime import datetime, timedelta
import re

class redditHook(BaseHook):
    def __init__(self, reddit_conn_id='reddit_conn_id'):
        self.connection = self.get_connection(reddit_conn_id)
        self.client_id = self.connection.extra_dejson.get('client_id')
        self.client_secret = self.connection.extra_dejson.get('client_secret')
        self.password = self.connection.extra_dejson.get('password')
        self.user_agent = self.connection.extra_dejson.get('user_agent')
        self.username = self.connection.extra_dejson.get('username')


    def getData(self,
                start_epoch,
                subreddit,
                limit,
                parameters=None,
                headers=None):

        reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            password=self.password,
            user_agent=self.user_agent,
            username=self.username,
        )

        def fxn(item):
            return item['score'] > 2

        api = PushshiftAPI()
        # search_subreddit = reddit.subreddit(subreddit)

        # print(dt.datetime.now().timestamp())

        submission = api.search_submissions(after=start_epoch,
                                    filter=['id', 'author', 'title', 'created_utc'],
                                    subreddit=subreddit, limit = limit)

        return submission

