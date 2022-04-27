CREATE OR REPLACE stage MY_AZURE_BINANCE_STAGE
    url='azure://cryptodeproject.blob.core.windows.net/binance'
    credentials=(azure_sas_token='?sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupx&se=2022-03-12T08:00:00Z&st=2022-03-07T22:10:43Z&spr=https&sig=kbzjl5C8068ckd4%2FTVp52yisOTel1zODPqAGy0iE094%3D')
    -- encryption=(type='AZURE_CSE' master_key = 'kPxX0jzYfIamtnJEUTHwq80Au6NbSgPH5r4BDDwOaO8=')
    file_format = my_csv_format;

CREATE OR REPLACE FILE FORMAT IF NOT EXISTS binance_fileformat
    type = json 
    null_if = ('NULL', 'null')
    compression = auto;

PUT file:///tmp/data.json @my_azure_binance_stage auto_compress=true;

CREATE TABLE IF NOT EXISTS staging_binance
(
    symbol varchar(3) NOT NULL,
    open_time timestamp NOT NULL,
    open float NOT NULL,
    high float NOT NULL,
    low float NOT NULL,
    close float NOT NULL,
    close_time timestamp NOT NULL,
    volume_1h float NOT NULL,
    quote_asset_volume float NOT NULL,
    number_of_trades int NOT NULL
);

COPY INTO "staging_binance"
FROM @my_azure_binance_stage
file_format = json;