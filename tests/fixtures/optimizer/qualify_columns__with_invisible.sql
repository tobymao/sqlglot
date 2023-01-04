--------------------------------------
-- Qualify columns
--------------------------------------
SELECT a FROM x;
SELECT x.a AS a FROM x AS x;

SELECT b FROM x;
SELECT x.b AS b FROM x AS x;

--------------------------------------
-- Derived tables
--------------------------------------
SELECT x.a FROM x AS x JOIN (SELECT * FROM x);
SELECT x.a AS a FROM x AS x JOIN (SELECT x.a AS a FROM x AS x) AS _q_0;

SELECT x.b FROM x AS x JOIN (SELECT b FROM x);
SELECT x.b AS b FROM x AS x JOIN (SELECT x.b AS b FROM x AS x) AS _q_0;

--------------------------------------
-- Expand *
--------------------------------------
SELECT * FROM x;
SELECT x.a AS a FROM x AS x;

SELECT * FROM y JOIN z ON y.b = z.b;
SELECT y.b AS b, z.b AS b FROM y AS y JOIN z AS z ON y.b = z.b;

SELECT * FROM y JOIN z ON y.c = z.c;
SELECT y.b AS b, z.b AS b FROM y AS y JOIN z AS z ON y.c = z.c;

SELECT a FROM (SELECT * FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0;

SELECT * FROM (SELECT a FROM x);
SELECT _q_0.a AS a FROM (SELECT x.a AS a FROM x AS x) AS _q_0;
