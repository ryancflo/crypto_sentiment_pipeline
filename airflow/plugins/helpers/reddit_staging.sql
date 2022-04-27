CREATE OR REPLACE stage MY_AZURE_REDDIT_STAGE
    url='azure://cryptodeproject.blob.core.windows.net/reddit'
    credentials=(azure_sas_token='?sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupx&se=2022-03-12T08:00:00Z&st=2022-03-07T22:10:43Z&spr=https&sig=kbzjl5C8068ckd4%2FTVp52yisOTel1zODPqAGy0iE094%3D')
    file_format = my_csv_format;

CREATE OR REPLACE FILE FORMAT IF NOT EXISTS REDDIT_FILEFORMAT
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = auto;

CREATE OR REPLACE TABLE STAGING_REDDIT
(
    reddit_post_id varchar NOT NULL,
    created_utc timestamp NOT NULL,
    author varchar NOT NULL,
    title varchar NOT NULL,
    neu float NOT NULL,
    pos float NOT NULL,
    neg float NOT NULL,
    compound float NOT NULL
)


COPY INTO "staging_reddit"
FROM @my_azure_reddit_stage
file_format = csv;

CREATE OR REPLACE TABLE REDDIT_FINAL
(
    reddit_post_id varchar PRIMARY KEY,
    created_utc timestamp NOT NULL,
    post_author varchar,
    title varchar NOT NULL,
    neutral_score float NOT NULL,
    negative_score float NOT NULL,
    positive_score float NOT NULL,
    compound float NOT NULL
) CLUSTER BY (created_utc)