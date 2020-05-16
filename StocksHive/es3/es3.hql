drop table if exists variazioni;
drop table if exists variazioni_medie;
drop table if exists es3_result;


create temporary table variazioni row format delimited fields terminated by ',' as

select min_dates.ticker as ticker, ( ( ( max_dates.close - min_dates.close) / min_dates.close ) * 100 ) as variazione_percentuale_annua, min_dates.anno as anno

from

(select t2.ticker as ticker, t2.close as close, YEAR(t2.data) as anno from (select ticker, MIN(data) as min_data from stock_prices where YEAR(data) >= 2016 and YEAR(data) <= 2018 group by ticker, YEAR(data) ) t1 join stock_prices t2 on (t1.ticker = t2.ticker and t1.min_data = t2.data) ) as min_dates

join

(select t2.ticker as ticker, t2.close as close, YEAR(t2.data) as anno from (select ticker, MAX(data) as max_data from stock_prices where YEAR(data) >= 2016 and YEAR(data) <= 2018 group by ticker, YEAR(data) ) t1 join stock_prices t2 on (t1.ticker = t2.ticker and t1.max_data = t2.data) ) as max_dates

on (min_dates.ticker = max_dates.ticker and min_dates.anno = max_dates.anno )

;


create temporary table variazioni_medie row format delimited fields terminated by ',' as

select aziende.name as nome, variazioni.anno as anno, FLOOR(AVG(variazioni.variazione_percentuale_annua)) as variazione_media 

from variazioni 

join

aziende

on variazioni.ticker = aziende.ticker

group by aziende.name, variazioni.anno

;


create table es3_result row format delimited fields terminated by ',' as

select concat_ws('--', collect_set(v1.nome)), v1.variazione_media as var16, v2.variazione_media as var17, v3.variazione_media as var18

from variazioni_medie v1 

join variazioni_medie v2

on (v1.nome = v2.nome and v1.anno != v2.anno)

join variazioni_medie v3

on (v1.nome = v3.nome and v3.anno != v2.anno != v1.anno)

where v1.anno = '2016' and v2.anno = '2017' and v3.anno = '2018'

group by v1.variazione_media, v2.variazione_media, v3.variazione_media

having count(*) > 1

;






















