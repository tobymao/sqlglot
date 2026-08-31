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
WITH a AS (SELECT 1 AS x), c AS (SELECT 1 AS k) SELECT * FROM a JOIN external_tbl ON TRUE JOIN c USING (unknown_key);
WITH c AS (SELECT 1 AS k) SELECT missing FROM external_a JOIN external_b ON TRUE JOIN c USING (k);
WITH c AS (SELECT 1 AS k) SELECT c.k FROM external_a JOIN external_b ON k = external_b.x JOIN c USING (k);
WITH c AS (SELECT 1 AS k) SELECT bogus.k FROM external_a JOIN external_b ON TRUE JOIN c USING (k);
WITH c AS (SELECT 1 AS k), external_d AS (SELECT 1 AS k) SELECT c.k FROM external_a JOIN external_b ON TRUE JOIN c USING (k) JOIN external_d ON k = external_d.k;
WITH c AS (SELECT 1 AS a) SELECT c.a FROM external_a JOIN external_b ON TRUE JOIN c USING (a) JOIN x ON a = x.a;
