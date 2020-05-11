DROP TABLE IF EXISTS j;
CREATE TEMPORARY TABLE j AS
SELECT s.ticker, s.sector, sp.close, sp.volume, sp.day
FROM stocks AS s JOIN stock_prices AS sp ON s.ticker = sp.ticker
WHERE YEAR(sp.day) >= 2008 AND YEAR(sp.day) <= 2018;

DROP TABLE IF EXISTS result;
CREATE TABLE result 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS
SELECT s, y, ROUND(AVG(v)), ROUND(AVG(((j.close - first_join.close) / first_join.close) * 100), 2), ROUND(SUM(c) / SUM(n), 2)
FROM
(SELECT s, y, t, v, j.close, r, c, n FROM
(SELECT j.sector AS s, YEAR(j.day) AS y, j.ticker AS t, MIN(j.day) AS l, MAX(j.day) AS r, SUM(j.volume) as v, SUM(j.close) AS c, COUNT(*) AS n
FROM j
GROUP BY j.sector, YEAR(j.day), j.ticker) first_aggregation
JOIN j ON j.sector = s AND YEAR(j.day) = y AND j.ticker = t AND j.day = l) first_join
JOIN j ON j.sector = s AND YEAR(j.day) = y AND j.ticker = t AND j.day = r
GROUP BY s, y;