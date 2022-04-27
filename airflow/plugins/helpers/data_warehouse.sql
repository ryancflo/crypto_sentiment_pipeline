DROP TABLE IF EXISTS final_marketdata;
CREATE TABLE final_marketdata (
    id int PRIMARY KEY,
    symbol_code varchar(3) references final_symbol(id) NOT NULL,
    open_time timestamp NOT NULL,
    open float NOT NULL,
    high float NOT NULL,
    low float NOT NULL,
    close float NOT NULL, 
    close_time timestamp NOT NULL,
    volume_last_1h float NOT NULL,
    quote_asset_volume float NOT NULL,
    number_of_trades int NOT NULL
)cluster by(
    to_date(close_time)
);

DROP TABLE IF EXISTS final_symbol;
CREATE TABLE final_symbol (
    symbol_code varchar(3) PRIMARY KEY,
    name varchar NOT NULL,
    cmc_rank int,
    circulating_supply int,
    total_supply int,
    max_supply int,
    num_market_pairs int,
    market_cap_by_total_supply_strict int,
    market_cap_dominance int,
    fully_diluted_market_cap int,
    last_updated timestamp
);

DROP TABLE IF EXISTS final_redditSentiment;
CREATE TABLE final_redditSentiment (
    post_id varchar PRIMARY KEY,
    date timestamp NOT NULL,
    title varchar
    neu float,
    neg float,
    pos float,
    compound float
)cluster by(
    to_date(date)
);

DROP TABLE IF EXISTS final_twitterSentiment;
CREATE TABLE final_twitterSentiment (
    post_id varchar PRIMARY KEY,
    date timestamp NOT NULL,
    title varchar
    neu float,
    neg float,
    pos float,
    compound float
)cluster by(
    to_date(date)
);