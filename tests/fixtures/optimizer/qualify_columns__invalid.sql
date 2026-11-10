SELECT z.a FROM x;
SELECT z.* FROM x;
SELECT x FROM x;
SELECT x FROM VALUES (1, 2);
SELECT a FROM x AS z JOIN y AS z;
SELECT a FROM x JOIN (SELECT b FROM y WHERE y.b = x.c);
SELECT a FROM x AS y JOIN (SELECT a FROM y) AS q ON y.a = q.a;
SELECT q.a FROM (SELECT x.b FROM x) AS z JOIN (SELECT a FROM z) AS q ON z.b = q.a;
SELECT b FROM x AS a CROSS JOIN y AS b CROSS JOIN y AS c;
SELECT x.a FROM x JOIN y USING (a);
SELECT a, SUM(b) FROM x GROUP BY 3;
SELECT p FROM (SELECT x from xx) y CROSS JOIN yy CROSS JOIN zz
SELECT a FROM (SELECT * FROM x CROSS JOIN y);
SELECT x FROM tbl AS tbl(a);
SELECT a JOIN b USING (a);
SELECT x.a FROM x INNER JOIN y ON x.a = c INNER JOIN z ON x.a = c;
SELECT b FROM x INNER JOIN y ON x.a = y.c INNER JOIN z ON x.a = z.c;
SELECT unpivotable.north FROM unpivotable UNPIVOT(revenue FOR month IN (jan, feb)) UNPIVOT(headcount FOR region IN (north, south))
SELECT u1.id FROM unpivotable UNPIVOT(zzz FOR m IN (jan, feb)) AS u1 CROSS JOIN x UNPIVOT(w FOR k IN (zzz)) AS u2
SELECT u2.* FROM unpivotable UNPIVOT(revenue FOR month IN (jan, feb)) AS u1 CROSS JOIN x UNPIVOT(w FOR k IN (b)) AS u2
SELECT w.d FROM w JOIN y ON TRUE JOIN z ON b > 0;
SELECT w.d FROM w JOIN y ON TRUE ANTI JOIN z ON b > 0;
SELECT w.d FROM w JOIN y ON TRUE SEMI JOIN z ON b > 0;
SELECT w.d FROM w JOIN (SELECT b FROM y) AS n ON TRUE ANTI JOIN z ON b > 0;

-- Unknown source columns can shadow projection aliases.
# dialect: duckdb
SELECT range + 1 AS range, range AS x FROM range(3);
# dialect: duckdb
SELECT range + 1 AS range FROM range(3) WHERE range > 1;
# dialect: duckdb
SELECT r.range + 1 AS x FROM range(3) AS r WHERE x > 1;
# dialect: duckdb
SELECT r.range + 1 AS x FROM range(3) AS r GROUP BY x;
# dialect: duckdb
SELECT SUM(r.range) AS x FROM range(3) AS r HAVING x > 1;
# dialect: duckdb
SELECT r.range AS x, ROW_NUMBER() OVER () AS n FROM range(3) AS r QUALIFY x > 1;
# dialect: duckdb
SELECT r.x + 1 AS y FROM (SELECT * FROM READ_PARQUET('f')) AS r WHERE y > 1;

-- A source name can also name one of its columns.
# dialect: duckdb
SELECT r FROM range(3) AS r;
# dialect: duckdb
SELECT range + 1 FROM range(3) AS range;
# dialect: duckdb
SELECT r FROM (SELECT * FROM READ_PARQUET('f')) AS r;

-- Unknown local or outer schemas prevent resolving unqualified correlated references.
# dialect: duckdb
SELECT p.range FROM range(10) AS p WHERE EXISTS (SELECT 1 FROM range(1) AS q WHERE range > 5);
# dialect: duckdb
SELECT p.range FROM range(10) AS p WHERE EXISTS (SELECT 1 WHERE range > 5);
# dialect: duckdb
SELECT p.range FROM (VALUES (0), (6)) AS p(range) WHERE EXISTS (SELECT 1 FROM range(1) AS q WHERE range > 5);
