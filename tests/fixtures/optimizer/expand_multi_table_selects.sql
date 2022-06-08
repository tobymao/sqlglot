--------------------------------------
-- Multi Table Selects
--------------------------------------
SELECT * FROM x AS x, y AS y WHERE x.a = y.a;
SELECT * FROM x AS x CROSS JOIN y AS y WHERE x.a = y.a;

SELECT * FROM x AS x, y AS y WHERE x.a = y.a AND x.a = 1 and y.b = 1;
SELECT * FROM x AS x CROSS JOIN y AS y WHERE x.a = y.a AND x.a = 1 AND y.b = 1;

SELECT * FROM x AS x, y AS y WHERE x.a > y.a;
SELECT * FROM x AS x CROSS JOIN y AS y WHERE x.a > y.a;
