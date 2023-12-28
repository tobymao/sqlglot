WITH y(c) AS (SELECT SUM(a) FROM (SELECT 1 a) AS x HAVING c > 0) SELECT c FROM y;
WITH y(c) AS (SELECT SUM(a) AS c FROM (SELECT 1 AS a) AS x HAVING c > 0) SELECT c FROM y;

WITH y(c) AS (SELECT SUM(a) as d FROM (SELECT 1 a) AS x HAVING c > 0) SELECT c FROM y;
WITH y(c) AS (SELECT SUM(a) AS c FROM (SELECT 1 AS a) AS x HAVING c > 0) SELECT c FROM y;

WITH x(c) AS (SELECT SUM(1) a HAVING c > 0 LIMIT 1) SELECT * FROM x;
WITH x(c) AS (SELECT SUM(1) AS c HAVING c > 0 LIMIT 1) SELECT * FROM x;

-- Invalid statement in Snowflake but checking more complex structures
WITH x(c) AS ((SELECT 1 a) HAVING c > 0) SELECT * FROM x;
WITH x(c) AS ((SELECT 1 AS a) HAVING c > 0) SELECT * FROM x;

-- Invalid statement in Snowflake but checking more complex structures
WITH x(c) AS ((SELECT SUM(1) a) HAVING c > 0 LIMIT 1) SELECT * FROM x;
WITH x(c) AS ((SELECT SUM(1) AS a) HAVING c > 0 LIMIT 1) SELECT * FROM x;

-- Invalid statement in Snowflake but checking that we don't fail
WITH x(c) AS (SELECT SUM(a) FROM x HAVING c > 0 UNION ALL SELECT SUM(a) FROM y HAVING c > 0) SELECT * FROM x;
WITH x(c) AS (SELECT SUM(a) FROM x HAVING c > 0 UNION ALL SELECT SUM(a) FROM y HAVING c > 0) SELECT * FROM x;
