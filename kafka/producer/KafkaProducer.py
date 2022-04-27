import os
import time
from pathlib import Path
from json import dumps
from typing import Dict
from confluent_kafka import Producer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

class KafkaProducer():
    # def __init__(self, config: Dict[str, str])):
    def __init__(self):
        self.producer = Producer({'bootstrap.servers':'broker:29092'})

    def produce(self, topic, data):
        try:
            self.producer.produce(
                topic=topic,
                value=data.encode("utf-8"),
                callback=self.delivery_callback)
            self.producer.poll(1)
        except Exception as e:
            print(f"The problem is: {e}!")

    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

        
