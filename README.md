# Data Engineering Project: Crypto Sentiment Pipeline (WIP)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-360/)


**Crypto Sentiment Pipeline** is an implementation of the data pipeline which consumes the latest data from coinmarketcap and post data from twitter and reddit. Consolidating all data into a centralized data store for sentiment analysis.
The pipeline infrastructure is built using popular, open-source projects.

## Tools & Technologies

- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Stream Processing - [**Kafka**](https://kafka.apache.org), [**Structured Streaming**](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- Orchestration & Batch Transformation - [**Airflow**](https://airflow.apache.org)
- Data Lake - [**Azure Data Lake**](https://azure.microsoft.com/en-us/solutions/data-lake/)
- Data Warehouse - [**Snowflake**](https://www.snowflake.com/)
- Data Visualization - [**Power BI**](https://powerbi.microsoft.com)
- Language - [**Python**](https://www.python.org)

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Architecture diagram](#architecture-diagram)
* [Project Structure](#project-structure)
* [Setup](#setup)
    * [Airflow Connections](airflow-connections)
* [How it works](#how-it-works)
    * [Data Flow-Airflow Dags](#data-flow)
    * [Data Schema](#data-schema)
* [Dashboards](#dashboards)
    * [Twitter Dashboard](#twitter-dashboard)
* [References](#references)
* [Work in Progress](#work-in-progress)

<!-- ARCHITECTURE DIAGRAM -->
## Architecture diagram

![Pipeline Architecture](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/projarchitecture.jpeg)


<!-- PROJECT STRUCTURE -->
## Project Structure

```
Airflow
|____config
| |____airflow.cfg                    # Set up customized Kafka topic
|
|____dags
| |____binance_dag.py                    # Set up customized Kafka topic
| |____coinmarketcap_dag.py
| |____reddit_dag.py
| |____twitter_dag.py
|
|____docker
| |____Dockerfile
|
|____plugins
| |____custom_hooks                                     # Result logs
| | |____binance_hook.py
| | |____coinmarketcap_hook.py
| | |____reddit_hook.py
| | |____twitter_hook.py
| |  
| |____custom_operators                                 # Result logs
| | |____binance_toAzureDataLake.py
| | |____coinmarketcap_toAzureDataLake.py
| | |____data_quality.py
| | |____reddit_toAzureDataLake.py
| | |____twitter_toAzureDataLake.py
| |
| |____custom_scripts 
| | |____vaderSentiment.py
| |
| |____custom_transfers                                 # Result logs
| | |____azureDataLake_toSnowflake.py
| | |____load_toSnowflakeTables.py
| |
| |____helpers                                          # Result logs
| | |____sql_queries.py
|
|____docker-compose.yml 
|
|____screenshots                                        # Application working screenshots
| |____screenshot 2.jpg
| |____screenshot 1.jpg
```

<!-- SETUP -->
### Setup

### Airflow Connections

Setup your airflow connections 

| Service | Conn ID | Conn Type | Other fields |
| ------- | ------- | --------- | ------------------ |
| Snowflake | `snowflake_conn_id` | `Snowflake` | Fill out your Snowflake database credentials |
| Azure Blob Storage | `azure_conn_id` | `Azure Blob Storage` | Fill out your Azure container credentials |
| Twitter API V2 (Tweepy)| `twitter_conn_id` | `HTTP` | Extras: {'BEARER_TOKEN' : YOUR_BEARER_TOKEN} |
| Reddit API | `reddit_conn_id` | `HTTP` | {'CLIENT_ID' : YOUR_CLIENT_ID, 'CLIENT_SECRET' : YOUR_CLIENT_SECRET} |
| Coinmarketcap API | `coinmarketcap_conn_id` | `HTTP` |  Extras: {'API_KEY' : YOUR_API_KEY}|


<!-- HOW IT WORKS -->
## How it works

#### Data Flow-Airflow Dags
Airflow Dags are seperated by data source. They are responsible for making calls to the API, extracting data and loading into target destinations.
It runs periodically every X minutes producing micro-batches.

##### CoinMarketCap DAG
 - Id: `coinmarketcap_dag`
 - Source Type: JSON API
 - Data Source: https://coinmarketcap.com/api/
    - Returns a paginated list of all active cryptocurrencies with latest market data. 

`coinmarketcap_toAzureDataLake`: Fetches data from the coinmarketcap API and loads it to Azure Blob Storage as a json file.\
`azure_coinmarketcap_snowflake`: Copy data from Azure Blob storage to coinmarketcap raw staging table.\
`json_transform`: Json data in staging tables are flatten and inserted into two seperate processing staging tables (coinmarketcap_tags and coinmarketcap_marketdata).\
`data_quality`: A simple data quality check for nulls and emptiness.\
`final_load`: Cleaned and transformed data in processed staging tables are inserted into coinmarketcap final tables.

![CoinMarketCap DAG](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/coinmarketcap_dag.PNG)

##### Reddit DAG
 - Id: `reddit_dag`
 - Source Type: JSON API - Returns Object using the PRAW and PMAW API wrapper
 - Data Source: reddit.com/dev/api
    - Returns a submission object for the newly created submission.

`reddit_toAzureDataLake`: Fetches data from the reddit API using the PMAW API wrapper. The sentiment analyzer assigns a score to the title of the reddit posts and loads the values to Azure Blob Storage as a csv file.\
`azure_reddit_snowflake`: Copy data from Azure Blob storage to reddit raw staging table.\
`data_quality`: A simple data quality check for nulls and emptiness.\
`loadReddit_toSnowflakeFinalTables`: Cleaned and transformed data in raw staging tables are inserted into reddit final tables.


![Reddit DAG](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/reddit_dag.PNG)

##### Twitter DAG
 - Id: `twitter_dag`
 - Source Type: JSON API
 - Data Source: https://developer.twitter.com/en/docs/twitter-api
    - Recent search endpoint allows you to programmatically access filtered public Tweets posted over the last week

`twitter_toAzureDataLake`: Fetches recent tweet data from the twitter API. Tweet data is seperated into two pandas dataframe to identify tweet data and the hashtags associated to that tweet. The sentiment analyzer assigns a score to the tweet and loads it to Azure Blob Storage as a csv file.\
`load_toSnowflakeStaging`: Copy data from Azure Blob storage to twitter raw staging tables.\
`data_quality`: A simple data quality check for nulls and emptiness.\
`final_load`: Cleaned and transformed data in raw staging tables are inserted into twitter final tables.

![Twitter DAG](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/twitter_dag.PNG)


#### Data Schema

![Staging Schema](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/staging_tables.PNG)

![Final Schema](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/final_tables.png)


## Dashboards

#### Twitter Dashboard

![Staging Schema](https://github.com/ryancflo/crypto_sentiment_pipeline/blob/main/images/twitter_dashboard.PNG)



#### References

Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.

#### Work in Progress

- [ ] Test robustness of pipelines with higher data volume
- [ ] Create views and materialized views then connect them to Power BI
- [ ] Normalize twitter hashtags to reflect coinmarketcap symbols
- [ ] Add Kafka and Spark structured streaming
- [ ] Create a simple web application to navigate/search in the data of these crawled jobs