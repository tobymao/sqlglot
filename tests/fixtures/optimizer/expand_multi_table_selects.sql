--------------------------------------
-- Multi Table Selects
--------------------------------------
SELECT * FROM x AS x, y AS y WHERE x.a = y.a;
SELECT * FROM x AS x JOIN y AS y ON x.a = y.a WHERE TRUE;

SELECT * FROM x AS x, y AS y WHERE x.a = y.a AND x.a = 1 and y.b = 1;
SELECT * FROM x AS x JOIN y AS y ON y.b = 1 AND x.a = y.a WHERE TRUE AND x.a = 1 AND TRUE;

SELECT * FROM x AS x, y AS y WHERE x.a > y.a;
SELECT * FROM x AS x JOIN y AS y ON x.a > y.a WHERE TRUE;
