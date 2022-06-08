--SELECT x.a AS a FROM (SELECT x.a FROM x AS x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 AND y.a = 1;
--SELECT x.a AS a FROM (SELECT x.a FROM x AS x WHERE x.a = 1 AND x.b = 1) AS x JOIN y ON y.a = 1 WHERE TRUE AND TRUE AND TRUE;
--
--WITH x AS (SELECT y.a FROM y) SELECT * FROM x WHERE x.a = 1;
--WITH x AS (SELECT y.a FROM y WHERE y.a = 1) SELECT * FROM x WHERE TRUE;
--
--SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 OR y.a = 1;
--SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 OR y.a = 1;
--
--SELECT x.a FROM (SELECT * FROM x) AS x JOIN y WHERE (x.a = y.a AND x.a = 1 AND x.b = 1) OR x.a = y.a;
--SELECT x.a FROM (SELECT * FROM x) AS x JOIN y ON x.a = y.a OR x.a = y.a WHERE (x.a = y.a AND x.a = 1 AND x.b = 1) OR x.a = y.a;
--
--SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x) AS x WHERE x.c = 1;
--SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x WHERE x.b * 1 = 1) AS x WHERE TRUE;
--
--SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x) AS x WHERE x.c = 1 or x.c = 2;
--SELECT x.a FROM (SELECT x.a AS a, x.b * 1 AS c FROM x WHERE x.b * 1 = 1 OR x.b * 1 = 2) AS x WHERE TRUE;

SELECT x.a AS a FROM (SELECT x.a FROM x AS x) AS x JOIN y WHERE x.a = 1 AND x.b = 1 AND (x.c = 1 OR y.c = 1);
SELECT x.a AS a FROM (SELECT x.a FROM x AS x WHERE x.a = 1 AND x.b = 1) AS x JOIN y ON (x.c = 1 OR y.c = 1) WHERE TRUE AND TRUE AND TRUE;
