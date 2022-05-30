SELECT x.a AS a FROM (SELECT a FROM x AS x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 AND y.a = 1;
SELECT x.a AS a FROM (SELECT a FROM x AS x WHERE a = 1 AND x.b = 1) AS x JOIN y ON y.a = 1 WHERE TRUE AND TRUE AND TRUE;

WITH x AS (SELECT * FROM y) SELECT * FROM x WHERE x.a = 1;
WITH x AS (SELECT * FROM y WHERE x.a = 1) SELECT * FROM x WHERE TRUE;

SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 OR y.a = 1;
SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 OR y.a = 1;

SELECT x.a FROM (SELECT x.a AS a, x * 1 AS c FROM x) AS x WHERE x.c = 1;
SELECT x.a FROM (SELECT x.a AS a, x * 1 AS c FROM x WHERE x * 1 = 1) AS x WHERE TRUE;
