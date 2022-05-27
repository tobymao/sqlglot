--------------------------------------
-- Decorrelate Subqueries
--------------------------------------
SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a) = 1;
SELECT * FROM x AS x JOIN (SELECT y.a FROM y AS y WHERE TRUE GROUP BY y.a) AS "_d_0" ON _d_0.a = x.a AND ("_d_0".a) = 1 WHERE TRUE;

SELECT * FROM x AS x WHERE (SELECT MIN(y.a) AS a FROM y AS y WHERE x.b = y.b) = 1;
SELECT * FROM x AS x JOIN (SELECT MIN(y.a) AS a, y.b FROM y AS y WHERE TRUE GROUP BY y.b) AS "_d_0" ON _d_0.b = x.b AND ("_d_0".a) = 1 WHERE TRUE;

SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a OR y.a > 1) = 1;
SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a OR y.a > 1) = 1;

SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a AND y.a > 1) = 1;
SELECT * FROM x AS x JOIN (SELECT y.a FROM y AS y WHERE TRUE AND y.a > 1 GROUP BY y.a) AS "_d_0" ON _d_0.a = x.a AND ("_d_0".a) = 1 WHERE TRUE;

SELECT * FROM x AS x, y AS y WHERE x.a = y.a AND (SELECT y.b FROM y AS y WHERE x.b = y.b) < 0;
SELECT * FROM x AS x JOIN (SELECT y.b FROM y AS y WHERE TRUE GROUP BY y.b) AS "_d_0" ON _d_0.b = x.b AND ("_d_0".b) < 0 JOIN y AS y ON x.a = y.a WHERE TRUE AND TRUE;

SELECT (SELECT MAX(y.c) AS max_c FROM y AS y WHERE y.b = x.b) AS max_c FROM x;
SELECT ("_d_0".max_c) AS max_c FROM x JOIN (SELECT MAX(y.c) AS max_c, y.b FROM y AS y WHERE TRUE GROUP BY y.b) AS "_d_0" ON _d_0.b = x.b;

--------------------------------------
-- Multi Table Selects
--------------------------------------
SELECT * FROM x AS x, y AS y WHERE x.a = y.a;
SELECT * FROM x AS x JOIN y AS y ON x.a = y.a WHERE TRUE;
