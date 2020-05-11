create table if not exists stock_prices (ticker string, open float, close float, adj_close float, low float, high float, volume float, data date) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

load data inpath '/input/historical_stock_prices.csv' overwrite into table stock_prices;

drop table if exists es1_result;







