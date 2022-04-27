from twitter_connector import TweepyStreamingClient
from argparse import ArgumentParser, FileType
from configparser import ConfigParser


if __name__ == "__main__":
    # parser = ArgumentParser()
    # parser.add_argument('config_file', type=FileType('r'))
    # args = parser.parse_args()

    # # Parse the configuration.
    # config_parser = ConfigParser()
    # config_parser.read_file(args.config_file)

    # hash_tag_list = ["#Bitcoin"]
    streamer = TweepyStreamingClient()
    stream = streamer.stream_tweets()

    # LOGGER.info(f"Produced into Kafka topic: {CRYPTO_TOPIC}.")
    # LOGGER.info(f"Waiting for the next round...")