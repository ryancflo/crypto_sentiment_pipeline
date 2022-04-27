-- ###### DASHBOARD VIEWS #####

--REDDIT AVG HOUR SENTIMENT DASHBOARD
CREATE OR REPLACE VIEW reddit_avg_hour_sentiment AS
SELECT
    YEAR(RF.created_utc) AS Year,
    MONTH(RF.created_utc) AS Month,
    DAY(RF.created_utc) AS Day,
    HOUR(RF.created_utc) As Hour,
    AVG(RF.neutral_score) AS Average_Neutral_Score,
    AVG(RF.positive_score) AS Average_Postive_Score,
    AVG(RF.negative_score) AS Average_Negative_Score,
    AVG(RF.compound) AS Average_Compound_Score,
    AVG(CM.Price) AS Price
FROM REDDIT_FINAL AS RF 
JOIN COINMARKETCAP_MARKET_FINAL AS CM
ON 
    YEAR(RF.created_utc) = YEAR(CM.last_updated)AND
    MONTH(RF.created_utc) = MONTH(CM.last_updated) AND
    DAY(RF.created_utc) = DAY(CM.last_updated) AND
    HOUR(RF.created_utc) = HOUR(CM.last_updated)
GROUP BY Year, Month, Day, Hour

--TWITTER AVG HOUR SENTIMENT DASHBOARD
CREATE OR REPLACE VIEW twitter_avg_hour_sentiment AS
SELECT
    YEAR(TTF.created_at) AS Year,
    MONTH(TTF.created_at) AS Month,
    DAY(TTF.created_at) AS Day,
    HOUR(TTF.created_at) As Hour,
    AVG(TTF.neu) AS Average_Neutral_Score,
    AVG(TTF.pos) AS Average_Postive_Score,
    AVG(TTF.neg) AS Average_Negative_Score,
    AVG(TTF.compound) AS Average_Compound_Score
FROM TWITTER_TWEET_FINAL AS TTF 
JOIN COINMARKETCAP_MARKET_FINAL AS CM
ON 
    YEAR(TTF.created_at) = YEAR(CM.last_updated)AND
    MONTH(TTF.created_at) = MONTH(CM.last_updated) AND
    DAY(TTF.created_at) = DAY(CM.last_updated) AND
    HOUR(TTF.created_at) = HOUR(CM.last_updated)
GROUP BY Year, Month, Day, Hour;


--TWITTER % HOURLY SENTIMENT
CREATE OR REPLACE VIEW twitter_percent_hour_sentiment AS
SELECT
    YEAR(TTF.created_at) AS Year,
    MONTH(TTF.created_at) AS Month,
    DAY(TTF.created_at) AS Day,
    HOUR(TTF.created_at) As Hour,
    SUM(TTF.neu)/COUNT(tweet_id)*100 AS Percent_Neutral_Score,
    AVG(TTF.pos)/COUNT(tweet_id)*100 AS Percent_Postive_Score,
    AVG(TTF.neg)/COUNT(tweet_id)*100 AS Percent_Negative_Score,
    SUM(TTF.retweets) AS Number_of_Retweets
FROM TWITTER_TWEET_FINAL AS TTF 
GROUP BY Year, Month, Day, Hour

--TWITTER PRICE TO SENTIMENT
CREATE OR REPLACE VIEW twitter_priceto_hour_sentiment AS
SELECT
    YEAR(TTF.created_at) AS Year,
    MONTH(TTF.created_at) AS Month,
    DAY(TTF.created_at) AS Day,
    HOUR(TTF.created_at) As Hour,
    AVG(TTF.neu) AS Average_Neutral_Score,
    AVG(TTF.pos) AS Average_Postive_Score,
    AVG(TTF.neg) AS Average_Negative_Score,
    AVG(TTF.compound) AS Average_Compound_Score,
    AVG(CM.price) AS Price,
    AVG(CM.volume_24h) AS Volume,
    AVG(CM.market_cap) AS Market_Cap,
    AVG(CM.cmc_rank) AS Rank
FROM TWITTER_TWEET_FINAL AS TTF 
JOIN COINMARKETCAP_MARKET_FINAL AS CM
ON 
    YEAR(TTF.created_at) = YEAR(CM.last_updated)AND
    MONTH(TTF.created_at) = MONTH(CM.last_updated) AND
    DAY(TTF.created_at) = DAY(CM.last_updated) AND
    HOUR(TTF.created_at) = HOUR(CM.last_updated)
WHERE CM.symbol = 'BTC'
GROUP BY Year, Month, Day, Hour

--Most Common hashtags SENTIMENT
CREATE OR REPLACE VIEW TWITTER_HASHTAGS_LASTHOUR AS
SELECT
    TTF.created_at AS date_created,
    YEAR(TTF.created_at) AS Year,
    MONTH(TTF.created_at) AS Month,
    DAY(TTF.created_at) AS Day,
    HOUR(TTF.created_at) As Hour,
    THF.hashtag As Hashtag,
    COUNT(THF.hashtag) OVER (PARTITION BY THF.hashtag ORDER BY Year,Month,Day) As Count
FROM TWITTER_TWEET_FINAL AS TTF
JOIN TWITTER_HASHTAGS_FINAL AS THF
ON 
    TTF.tweet_id = THF.tweet_Id
WHERE date_created >= dateadd('hour',-1, CURRENT_TIMESTAMP())
GROUP BY date_created, Year, Month, Day, Hour, THF.hashtag
 
