class SQLQueries:

######### COINMARKETCAP QUERIES #########
    select_coinmarketcapdata_from_jsonstaging = ("""
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
    FROM JSONSTAGING_COINMARKETCAP, LATERAL FLATTEN(input => json_data:data)
    """)

    select_coinmarketcapdata_tags_from_jsonstaging = ("""
    SELECT
        S.value:symbol AS symbol,
        tagstbl.value AS tags
    FROM JSONSTAGING_COINMARKETCAP, LATERAL FLATTEN(input => json_data:data) AS S,
    table(flatten(S.value:tags,'')) tagstbl;
    """)

    select_coinmarketcapdata_from_StagingMarket = ("""
    SELECT
        symbol as symbol,  
        name as name,
        cmc_rank AS rank,
        circulating_supply AS circulating_supply,
        total_supply AS total_supply,
        max_supply AS max_supply,
        num_market_pairs AS num_market_pairs,
        market_cap AS market_cap,
        market_cap_dominance AS market_cap_dominance,
        fully_diluted_market_cap AS full_diluted_market_cap,
        price AS price,
        volume_24h AS volume_24h,
        volume_7d AS volume_7d,
        volume_30d AS volume_30d,
        percent_change_1h AS percent_change_1h,
        percent_change_24h AS percent_change_24h,
        percent_change_7d AS percent_change_7d,
        last_updated AS last_updated
    FROM STAGING_COINMARKETCAP_MARKET
    """)


    select_coinmarketcapdata_from_StagingTags = ("""
    SELECT
        symbol AS symbol,
        tags AS tags
    FROM STAGING_COINMARKETCAP_TAGS
    """)

######### REDDIT QUERIES #########
    select_redditData_from_redditStaging = ("""
    SELECT
        reddit_post_id AS reddit_post_id,
        created_utc AS created_utc,
        author AS post_author,
        title AS title,
        neu AS neutral_score,
        neg AS negative_score,
        pos AS positive_score,
        compound AS compound_score
    FROM STAGING_REDDIT
    WHERE reddit_post_id NOT IN (SELECT reddit_post_id FROM REDDIT_FINAL)
    """)
    
######### TWITTER QUERIES #########

    select_twitterData_from_twitterTweetStaging = ("""
    SELECT
        tweet_id AS tweet_id,
        clean_text AS text,
        created_at AS created_at,
        source AS source,
        language AS language,
        retweets AS num_of_retweets,
        replies AS num_of_replies,
        likes AS num_of_likes,
        quote_count AS num_of_quote_count,
        neu AS neutral_score,
        pos AS neutral_score,
        neg AS negative_score,
        compound AS compound_score,
        location AS location,
        latitude AS latitude,
        longitude AS longitude
    FROM STAGING_TWITTER_TWEET
    WHERE tweet_id NOT IN (SELECT tweet_id FROM TWITTER_TWEET_FINAL)
    """)


    select_twitterData_from_twitterHashtagsStaging = ("""
    SELECT
        tweet_id AS tweet_id,
        hashtag AS hashtag_text
    FROM STAGING_TWITTER_HASHTAGS
    WHERE tweet_id NOT IN (SELECT tweet_id FROM TWITTER_HASHTAGS_FINAL)
    """)







# MERGE INTO final_date USING
# (SELECT $1 barKey,
#  $2 newVal,
#  $3 newStatus,
#  ...
# FROM @my_stage( FILE_FORMAT => 'csv', PATTERN => '.*my_pattern.*')
# ) bar ON foo.fooKey = bar.barKey
# WHEN MATCHED THEN
#  UPDATE SET val = bar.newVal, status = bar.newStatusWHEN NOT MATCHED THEN
#  INSERT
#  (val, status
#  ) VALUES
#  (bar.newVal, bar.newStatus
#  );