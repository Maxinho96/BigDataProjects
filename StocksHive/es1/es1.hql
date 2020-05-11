create table es1_result row format delimited fields terminated by ',' as 

select min_dates.ticker as ticker, FLOOR( ( (max_dates.close - min_dates.close) / min_dates.close) *100 ) as variazione_percentuale, min_dates.min_close as min, min_dates.max_close as max, min_dates.avg_volume as avg_volume

from

(select t2.ticker, t1.min_close, t1.max_close, t1.avg_volume, t2.close from (select ticker, MIN(data) as min_data, MIN(close) as min_close, MAX(close) as max_close, AVG(volume) as avg_volume from stock_prices where data >= '2008-01-01' and data <='2018-12-31' group by ticker) t1 join stock_prices t2 on (t1.ticker = t2.ticker and t1.min_data = t2.data)) as min_dates 

join

(select t2.ticker, t2.close from (select ticker, MAX(data) as max_data from stock_prices where data >= '2008-01-01' and data <='2018-12-31' group by ticker) t1 join stock_prices t2 on (t1.ticker = t2.ticker and t1.max_data = t2.data)) as max_dates

on (min_dates.ticker = max_dates.ticker)

order by variazione_percentuale DESC;









