from airflow.plugins_manager import AirflowPlugin

from plugins.custom_hooks.coinmarketcap_hook import coinmarketcapHook
from plugins.custom_hooks.reddit_hook import redditHook
from plugins.custom_hooks.twitter_hook import twitterHook
from plugins.custom_hooks.binance_hook import binanceHook

#Define the plugin class
class MyPlugins(AirflowPlugin):
    name = "my_plugin"

    hooks = [coinmarketcapHook, redditHook, twitterHook, binanceHook]