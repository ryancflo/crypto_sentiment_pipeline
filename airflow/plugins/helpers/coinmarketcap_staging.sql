CREATE OR REPLACE stage MY_AZURE_BINANCE_STAGE
    url='azure://cryptodeproject.blob.core.windows.net/binance'
    credentials=(azure_sas_token='?sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupx&se=2022-03-12T08:00:00Z&st=2022-03-07T22:10:43Z&spr=https&sig=kbzjl5C8068ckd4%2FTVp52yisOTel1zODPqAGy0iE094%3D')
    -- encryption=(type='AZURE_CSE' master_key = 'kPxX0jzYfIamtnJEUTHwq80Au6NbSgPH5r4BDDwOaO8=')
    file_format = my_csv_format;

CREATE OR REPLACE FILE FORMAT IF NOT EXISTS coinmarketcap_fileformat
    type = json 
    null_if = ('NULL', 'null')
    compression = auto;

CREATE OR REPLACE TABLE JSONSTAGING_COINMARKETCAP
(
    json_data variant
);

CREATE OR REPLACE TABLE STAGING_COINMARKETCAP_MARKET
(
    symbol varchar(4) NOT NULL,
    name varchar,
    cmc_rank int,
    circulating_supply float,
    total_supply float,
    max_supply float,
    num_market_pairs int,
    market_cap float,
    market_cap_dominance float,
    fully_diluted_market_cap float,
    price float,
    volume_24h float,
    volume_7d float,
    volume_30d  float,
    percent_change_1h float,
    percent_change_24h  float,
    percent_change_7d float,
    last_updated  timestamp
);

CREATE OR REPLACE TABLE STAGING_COINMARKETCAP_TAGS
(
    symbol varchar(4) NOT NULL,
    tags varchar
);

COPY INTO "staging_coinmarketcap"
FROM @my_azure_coinmarketcap_stage
file_format = coinmarketcap_fileformat;

SELECT 
value:symbol::string AS symbol,
value:name::string AS name,
value:cmc_rank::integer AS cmc_rank,
value:circulating_supply::float AS circulating_supply,
value:total_supply::float AS total_supply,
value:max_supply::float AS max_supply,
value:num_market_pairs::integer AS num_market_pairs,
value:quote:USD.market_cap::float AS market_cap,
value:quote:USD.market_cap_dominance::float AS market_cap_dominance,
value:quote:USD.fully_diluted_market_cap::float AS full_diluted_market_cap,
value:quote:USD.price::float AS price,
value:quote:USD.volume_24h::float AS volume_24h,
value:quote:USD.volume_7d::float AS volume_7d,
value:quote:USD.volume_30d::float AS volume_30d,
value:quote:USD.percent_change_1h::float AS percent_change_1h,
value:quote:USD.percent_change_24h ::float AS percent_change_24h,
value:quote:USD.percent_change_7d::float AS percent_change_7d,
value:last_updated::timestamp AS last_updated
FROM STAGING_COINMARKETCAP, LATERAL FLATTEN(input => json_data:data)

SELECT
  S.value:symbol AS symbol,
  tagstbl.value AS tags
FROM STAGING_COINMARKETCAP, LATERAL FLATTEN(input => json_data:data) AS S,
table(flatten(S.value:tags,'')) tagstbl;

CREATE OR REPLACE TABLE COINMARKETCAP_MARKET_FINAL
(
    symbol varchar PRIMARY KEY,
    name varchar,
    cmc_rank int,
    circulating_supply float,
    total_supply float,
    max_supply float,
    num_market_pairs int,
    market_cap float,
    market_cap_dominance float,
    fully_diluted_market_cap float,
    price float,
    volume_24h float,
    volume_7d float,
    volume_30d  float,
    percent_change_1h float,
    percent_change_24h  float,
    percent_change_7d float,
    last_updated  timestamp
);

CREATE OR REPLACE TABLE COINMARKETCAP_TAGS_FINAL
(
    symbol varchar references COINMARKETCAP_MARKET_FINAL(symbol),
    tags varchar
);