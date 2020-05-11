DROP TABLE IF EXISTS stock_prices;
CREATE TABLE stock_prices
(ticker STRING, open DECIMAL, close DECIMAL, adj_close DECIMAL, low DECIMAL, high DECIMAL, volume BIGINT, day DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
LOAD DATA INPATH '/input/historical_stock_prices.csv' OVERWRITE INTO TABLE stock_prices;

DROP TABLE IF EXISTS stocks;
CREATE TABLE IF NOT EXISTS stocks
(ticker STRING, ex STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
LOAD DATA INPATH '/input/historical_stocks.csv' OVERWRITE INTO TABLE stocks;
