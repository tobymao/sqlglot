SELECT * FROM x WHERE x.a = (SELECT SUM(y.a) AS a FROM y);
SELECT * FROM x CROSS JOIN (SELECT SUM(y.a) AS a FROM y) AS _u_0 WHERE x.a = _u_0.a;

SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL;

SELECT * FROM x WHERE x.a IN (SELECT y.b AS b FROM y);
SELECT * FROM x LEFT JOIN (SELECT y.b AS b FROM y GROUP BY y.b) AS _u_0 ON x.a = _u_0.b WHERE NOT _u_0.b IS NULL;

SELECT * FROM x WHERE x.a = ANY (SELECT y.a AS a FROM y);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL;

SELECT * FROM x WHERE x.a = (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a);
SELECT * FROM x LEFT JOIN (SELECT SUM(y.b) AS b, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0._u_1 WHERE x.a = _u_0.b;

SELECT * FROM x WHERE x.a > (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a);
SELECT * FROM x LEFT JOIN (SELECT SUM(y.b) AS b, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0._u_1 WHERE x.a > _u_0.b;

SELECT * FROM x WHERE x.a <> ANY (SELECT y.a AS a FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0.a = x.a WHERE x.a <> _u_0.a;

SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0.a = x.a WHERE NOT x.a = _u_0.a;

SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y WHERE y.b = x.a);
SELECT * FROM x LEFT JOIN (SELECT ARRAY_AGG(y.a) AS a, y.b AS _u_1 FROM y WHERE TRUE GROUP BY y.b) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ANY(_u_0.a, _x -> _x = x.a);

SELECT * FROM x WHERE x.a < (SELECT SUM(y.a) AS a FROM y WHERE y.a = x.a and y.a = x.b and y.b <> x.d);
SELECT * FROM x LEFT JOIN (SELECT SUM(y.a) AS a, y.a AS _u_1, ARRAY_AGG(y.b) AS _u_2 FROM y WHERE TRUE AND TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a AND _u_0._u_1 = x.b WHERE (x.a < _u_0.a AND ARRAY_ANY(_u_0._u_2, _x -> _x <> x.d));

SELECT * FROM x WHERE EXISTS (SELECT y.a AS a, y.b AS b FROM y WHERE x.a = y.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL;

SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y LIMIT 10);
SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y LIMIT 10);

SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a FROM y OFFSET 10);
SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a FROM y OFFSET 10);

SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a, y.b AS b FROM y);
SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a, y.b AS b FROM y);

SELECT * FROM x.a WHERE x.a > ANY (SELECT y.a FROM y);
SELECT * FROM x.a WHERE x.a > ANY (SELECT y.a FROM y);

SELECT * FROM x WHERE x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a LIMIT 10);
SELECT * FROM x WHERE x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a LIMIT 10);

SELECT * FROM x WHERE x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a OFFSET 10);
SELECT * FROM x WHERE x.a = (SELECT SUM(y.c) AS c FROM y WHERE y.a = x.a OFFSET 10);

SELECT * FROM x WHERE x.a > ALL (SELECT y.c AS c FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT ARRAY_AGG(y.c) AS c, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ALL(_u_0.c, _x -> x.a > _x);

SELECT * FROM x WHERE x.a > (SELECT COUNT(*) as d FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT COUNT(*) AS d, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE x.a > COALESCE(_u_0.d, 0);

# title: invalid statement left alone
SELECT * FROM x WHERE x.a = SUM(SELECT 1);
SELECT * FROM x WHERE x.a = SUM(SELECT 1);

SELECT * FROM x WHERE x.a IN (SELECT max(y.b) AS b FROM y GROUP BY y.a);
SELECT * FROM x LEFT JOIN (SELECT _q.b AS b FROM (SELECT MAX(y.b) AS b FROM y GROUP BY y.a) AS _q GROUP BY _q.b) AS _u_0 ON x.a = _u_0.b WHERE NOT _u_0.b IS NULL;

SELECT x.a > (SELECT SUM(y.a) AS b FROM y) FROM x;
SELECT x.a > _u_0.b FROM x CROSS JOIN (SELECT SUM(y.a) AS b FROM y) AS _u_0;

SELECT (SELECT MAX(t2.c1) AS c1 FROM t2 WHERE t2.c2 = t1.c2 AND t2.c3 <= TRUNC(t1.c3)) AS c FROM t1;
SELECT _u_0.c1 AS c FROM t1 LEFT JOIN (SELECT MAX(t2.c1) AS c1, t2.c2 AS _u_1, MAX(t2.c3) AS _u_2 FROM t2 WHERE TRUE AND TRUE GROUP BY t2.c2) AS _u_0 ON _u_0._u_1 = t1.c2 WHERE _u_0._u_2 <= TRUNC(t1.c3);

SELECT s.t AS t FROM s WHERE 1 IN (SELECT t.a AS a FROM t WHERE t.b > 1);
SELECT s.t AS t FROM s LEFT JOIN (SELECT t.a AS a FROM t WHERE t.b > 1 GROUP BY t.a) AS _u_0 ON 1 = _u_0.a WHERE NOT _u_0.a IS NULL;

# title: can't create GROUP BY clause with an aggregate
SELECT s.t FROM s WHERE 1 IN (SELECT MAX(t.a) AS t1 FROM t);
SELECT s.t FROM s LEFT JOIN (SELECT MAX(t.a) AS t1 FROM t) AS _u_0 ON 1 = _u_0.t1 WHERE NOT _u_0.t1 IS NULL;

# title: can't create GROUP BY clause with an aggregate (nested)
SELECT s.t FROM s WHERE 1 IN (SELECT MAX(t.a) + 1 AS t1 FROM t);
SELECT s.t FROM s LEFT JOIN (SELECT MAX(t.a) + 1 AS t1 FROM t) AS _u_0 ON 1 = _u_0.t1 WHERE NOT _u_0.t1 IS NULL;

SELECT BIT_COUNT(EXISTS(SELECT 1 WHERE FALSE)) AS col FROM t0;
SELECT BIT_COUNT(EXISTS(SELECT 1 WHERE FALSE)) AS col FROM t0;

# title: EXISTS in SELECT with GROUP BY - empty subquery should return 0, not eliminate rows
SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0 FROM t1, t0 GROUP BY t0.c2;
SELECT NOT MAX(_u_0."1") IS NULL AS ref0 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE GROUP BY t0.c2;

# title: EXISTS in SELECT with GROUP BY - non-empty subquery should return 1
SELECT EXISTS (SELECT 1 WHERE TRUE) AS ref0 FROM t1, t0 GROUP BY t0.c2;
SELECT NOT MAX(_u_0."1") IS NULL AS ref0 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE TRUE) AS _u_0 ON TRUE GROUP BY t0.c2;

# title: Multiple EXISTS in SELECT with GROUP BY
SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0, EXISTS (SELECT 1 WHERE TRUE) AS ref1 FROM t1, t0 GROUP BY t0.c2;
SELECT NOT MAX(_u_0."1") IS NULL AS ref0, NOT MAX(_u_1."1") IS NULL AS ref1 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE LEFT JOIN (SELECT 1 WHERE TRUE) AS _u_1 ON TRUE GROUP BY t0.c2;

# title: EXISTS in SELECT with HAVING clause
SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0 FROM t1 GROUP BY t1.c0 HAVING COUNT(*) > 0;
SELECT NOT MAX(_u_0."1") IS NULL AS ref0 FROM t1 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE GROUP BY t1.c0 HAVING COUNT(*) > 0;
