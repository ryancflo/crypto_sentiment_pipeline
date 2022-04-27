CREATE OR REPLACE stage my_azure_twitter_stage
    url='azure://cryptodeproject.blob.core.windows.net/twitter'
    credentials=(azure_sas_token='?sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupx&se=2022-03-12T08:00:00Z&st=2022-03-07T22:10:43Z&spr=https&sig=kbzjl5C8068ckd4%2FTVp52yisOTel1zODPqAGy0iE094%3D')
    file_format = my_csv_format;

CREATE OR REPLACE FILE FORMAT IF NOT EXISTS twitter_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = auto;

CREATE TABLE IF NOT EXISTS STAGING_TWITTER_TWEET 
(
    tweet_id varchar NOT NULL,
    text varchar NOT NULL,
    clean_text varchar NOT NULL,
    created_at timestamp NOT NULL,
    source varchar NOT NULL,
    language varchar,
    retweets int NOT NULL,
    replies int NOT NULL,
    likes int NOT NULL,
    quote_count int NOT NULL,
    neu float,
    pos float,
    neg float,
    compound float,
    location varchar,
    latitude float,
    longitude float,
    hashtags varchar
);

CREATE TABLE IF NOT EXISTS STAGING_TWITTER_HASHTAGS
(
    tweet_id varchar NOT NULL,
    hashtag varchar NOT NULL
);

COPY INTO "staging_twitter"
FROM @my_azure_twitter_stage
file_format = csv;


CREATE OR REPLACE TABLE TWITTER_TWEET_FINAL
(
    tweet_id varchar PRIMARY KEY,
    clean_text varchar NOT NULL,
    created_at timestamp NOT NULL,
    source varchar NOT NULL,
    language varchar,
    retweets int NOT NULL,
    replies int NOT NULL,
    likes int NOT NULL,
    quote_count int NOT NULL,
    neu float,
    pos float,
    neg float,
    compound float,
    location varchar,
    latitude float,
    longitude float
) CLUSTER BY (created_at);

CREATE OR REPLACE TABLE TWITTER_HASHTAGS_FINAL
(
    tweet_id varchar references TWITTER_TWEET_FINAL(tweet_id),
    hashtag varchar NOT NULL
);