import tweepy
from KafkaProducer import KafkaProducer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from typing import Any, Optional

class TweepyStreamingClient(tweepy.StreamingClient):
    """
    This is a class provided by tweepy to access the Twitter Streaming API.
    """

    def __init__(self):
        """
        The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
        """
        super().__init__(
            tweepy.StreamingClient(
                bearer_token=BEARER_TOKEN,
                wait_on_rate_limit=True,
                max_retries=3
            )
        )
        producer = KafkaProducer()
    
    def stream_tweets(self, hash_tag_list: Optional[dict] = None):

        self.filter(tweet_fields=hash_tag_list)

    def on_error(self, status_code: int):
        """
        On error - if an error occurs, display the error / status code
        :param status_code: int
        :return: bool
        """
        print("Error received in kafka producer " + repr(status_code))
        return True

    def on_data(self, data: str):
        """
        This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue.
        :param data: JSON
        :return: bool
        """
        try:
            self.producer.send(data)
        except Exception as e:
            print(e)
            return False
        return True

