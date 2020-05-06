DROP TABLE IF EXISTS j;
CREATE TEMPORARY TABLE j AS
SELECT s.ticker, s.sector, sp.close, sp.volume, sp.day
FROM stocks AS s JOIN stock_prices AS sp ON s.ticker = sp.ticker
WHERE YEAR(sp.day) >= 2008 AND YEAR(sp.day) <= 2018;

--SELECT volumeSums.s, volumeSums.y, AVG(volumeSums.v)
--FROM
--(SELECT j.sector AS s, YEAR(j.day) AS y, SUM(j.volume) as v
--FROM j
--GROUP BY j.sector, YEAR(j.day), j.ticker) volumeSums
--GROUP BY volumeSums.s, volumeSums.y;

SELECT s, y, AVG(((j.close - first_join.close) / first_join.close) * 100)
FROM
(SELECT s, y, t, j.day, j.close, r FROM
(SELECT j.sector AS s, YEAR(j.day) AS y, j.ticker AS t, MIN(j.day) AS l, MAX(j.day) AS r
FROM j
GROUP BY j.sector, YEAR(j.day), j.ticker) close_prices
JOIN j ON j.sector = s AND YEAR(j.day) = y AND j.ticker = t AND j.day = l) first_join
JOIN j ON j.sector = s AND YEAR(j.day) = y AND j.ticker = t AND j.day = r
GROUP BY s, y;

--SELECT s.sector, YEAR(sp.day), AVG(sp.close)
--FROM stocks AS s JOIN stock_prices AS sp ON s.ticker = sp.ticker
--GROUP BY s.sector, YEAR(sp.day);