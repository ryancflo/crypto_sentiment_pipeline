FROM python:3.7.5-slim

COPY . /app
RUN mkdir /app/producer/__logger/

WORKDIR /app

RUN pip install --upgrade pip

RUN pip install confluent_kafka \
                pyspark \
                tweepy

CMD ["python", "./producer/twitter_producer.py"]