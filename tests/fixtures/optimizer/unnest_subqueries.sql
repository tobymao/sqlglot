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

# title: correlated NOT IN is not unnested because LEFT-JOIN-anti loses three-valued NULL semantics
SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE NOT x.a IN (SELECT y.a AS a FROM y WHERE y.a = x.a);

SELECT * FROM x WHERE NOT (x.a IN (SELECT y.a AS a FROM y WHERE y.a = x.a));
SELECT * FROM x WHERE NOT (x.a IN (SELECT y.a AS a FROM y WHERE y.a = x.a));

SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y WHERE y.b = x.a);
SELECT * FROM x LEFT JOIN (SELECT ARRAY_AGG(y.a) AS a, y.b AS _u_1 FROM y WHERE TRUE GROUP BY y.b) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ANY(_u_0.a, _x -> _x = x.a);

SELECT * FROM x WHERE x.a < (SELECT SUM(y.a) AS a FROM y WHERE y.a = x.a and y.a = x.b and y.b <> x.d);
SELECT * FROM x LEFT JOIN (SELECT SUM(y.a) AS a, y.a AS _u_1, ARRAY_AGG(y.b) AS _u_2 FROM y WHERE TRUE AND TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a AND _u_0._u_1 = x.b WHERE (x.a < _u_0.a AND ARRAY_ANY(_u_0._u_2, _x -> _x <> x.d));

SELECT * FROM x WHERE EXISTS (SELECT y.a AS a, y.b AS b FROM y WHERE x.a = y.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL;

# title: EXISTS over a scalar aggregate always matches, it returns exactly one row
SELECT * FROM x WHERE EXISTS (SELECT COUNT(*) FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE TRUE;

# title: NOT EXISTS over a scalar aggregate never matches
SELECT * FROM x WHERE NOT EXISTS (SELECT SUM(y.b) FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE NOT TRUE;

# title: EXISTS over a scalar aggregate with a HAVING is not rewritten
SELECT * FROM x WHERE EXISTS (SELECT COUNT(*) FROM y WHERE y.a = x.a HAVING COUNT(*) = 0);
SELECT * FROM x WHERE EXISTS(SELECT COUNT(*) FROM y WHERE y.a = x.a HAVING COUNT(*) = 0);

# title: EXISTS over a windowed aggregate is not a scalar aggregate
SELECT * FROM x WHERE EXISTS (SELECT COUNT(*) OVER () FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE NOT _u_0._u_1 IS NULL;

# title: EXISTS is not folded when the aggregate belongs to a derived table inside it
SELECT * FROM x WHERE EXISTS (SELECT * FROM (SELECT COUNT(*) AS c FROM y WHERE y.a = x.a) AS t WHERE t.c > 5);
SELECT * FROM x WHERE EXISTS(SELECT * FROM (SELECT COUNT(*) AS c FROM y WHERE y.a = x.a) AS t WHERE t.c > 5);

# title: EXISTS is not folded when the aggregate belongs to a CTE inside it
SELECT * FROM x WHERE EXISTS (WITH t AS (SELECT COUNT(*) AS c FROM y WHERE y.a = x.a) SELECT t.c AS c FROM t WHERE t.c > 5);
SELECT * FROM x WHERE EXISTS(WITH t AS (SELECT COUNT(*) AS c FROM y WHERE y.a = x.a) SELECT t.c AS c FROM t WHERE t.c > 5);

# title: EXISTS is not folded when the aggregate is only one branch of a set operation
SELECT * FROM x WHERE EXISTS (SELECT COUNT(*) AS c FROM y WHERE y.a = x.a INTERSECT SELECT z.a AS a FROM z);
SELECT * FROM x WHERE EXISTS(SELECT COUNT(*) AS c FROM y WHERE y.a = x.a INTERSECT SELECT z.a AS a FROM z);

# title: a correlated branch of a set operation is not decorrelated, it can't be hoisted out
SELECT * FROM x WHERE EXISTS (SELECT y.a AS a FROM y WHERE y.a = x.a INTERSECT SELECT z.a AS a FROM z);
SELECT * FROM x WHERE EXISTS(SELECT y.a AS a FROM y WHERE y.a = x.a INTERSECT SELECT z.a AS a FROM z);

# title: EXISTS over a scalar aggregate with a QUALIFY is not rewritten
SELECT * FROM x WHERE EXISTS (SELECT COUNT(*) FROM y WHERE y.a = x.a QUALIFY ROW_NUMBER() OVER () = 2);
SELECT * FROM x WHERE EXISTS(SELECT COUNT(*) FROM y WHERE y.a = x.a QUALIFY ROW_NUMBER() OVER () = 2);

# title: an aggregate in a window spec still groups the subquery into a single row
SELECT * FROM x WHERE EXISTS (SELECT RANK() OVER (ORDER BY SUM(y.b)) FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE TRUE;

# title: a parenthesized aggregate in a window spec still groups the subquery
SELECT * FROM x WHERE EXISTS (SELECT RANK() OVER (ORDER BY (SUM(y.b))) FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE TRUE;

# title: an aggregate in the arguments of a windowed function still groups the subquery
SELECT * FROM x WHERE EXISTS (SELECT LAG(SUM(y.b)) OVER (ORDER BY 1) FROM y WHERE y.a = x.a);
SELECT * FROM x WHERE TRUE;

# title: a FILTER between the window and the aggregate leaves it windowed
SELECT * FROM x WHERE EXISTS (SELECT SUM(y.b) FILTER(WHERE y.b > 1) OVER () FROM y WHERE y.a = x.a);
SELECT * FROM x LEFT JOIN (SELECT y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE NOT _u_0._u_1 IS NULL;

# title: EXISTS over a scalar aggregate is folded inside an outer window function
SELECT COUNT(CASE WHEN EXISTS(SELECT COUNT(*) FROM y WHERE y.a = x.a) THEN 1 END) OVER () FROM x;
SELECT COUNT(CASE WHEN TRUE THEN 1 END) OVER () FROM x;

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

# title: Skip unnesting GENERATE_SERIES
WITH t2 AS (SELECT CAST(t1.c1 AS BIGINT) AS ref1 FROM GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1)) SELECT t2.ref1 AS ref1 FROM t2 AS t2;
WITH t2 AS (SELECT CAST(t1.c1 AS BIGINT) AS ref1 FROM GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1)) SELECT t2.ref1 AS ref1 FROM t2 AS t2;

# title: Skip unnesting UNNEST (same issue as GENERATE_SERIES)
WITH t2 AS (SELECT t1.c1 FROM UNNEST((SELECT ARRAY(x.a) FROM x)) AS t1(c1)) SELECT t2.c1 FROM t2;
WITH t2 AS (SELECT t1.c1 FROM UNNEST((SELECT ARRAY(x.a) FROM x)) AS t1(c1)) SELECT t2.c1 FROM t2;

# title: Skip unnesting GENERATE_SERIES but unnesting the rest in the query
SELECT t1.c1 > (SELECT SUM(y.a) AS b FROM y) FROM x JOIN GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1) ON t1.c1 > x.a;
SELECT t1.c1 > _u_0.b FROM x JOIN GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1) ON t1.c1 > x.a CROSS JOIN (SELECT SUM(y.a) AS b FROM y) AS _u_0;

# title: correlated scalar subquery with EQ + range predicates inside a function in SELECT should not crash (issue #7295)
SELECT COALESCE((SELECT MAX(b.val) FROM t b WHERE b.val < a.val AND b.id = a.id), a.val) AS result FROM t a;
SELECT COALESCE((SELECT MAX(b.val) FROM t AS b WHERE b.val < a.val AND b.id = a.id), a.val) AS result FROM t AS a;

# title: IN with UNION ALL subquery should use derived alias in wrapper SELECT
SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z);
SELECT * FROM x LEFT JOIN (SELECT _u_0.a AS a FROM (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z) AS _u_0 GROUP BY _u_0.a) AS _u_1 ON x.a = _u_1.a WHERE NOT _u_1.a IS NULL;

# title: NOT IN is not unnested because LEFT-JOIN-anti loses three-valued NULL semantics
SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y);
SELECT * FROM x WHERE NOT x.a IN (SELECT y.a AS a FROM y);

SELECT * FROM x WHERE NOT (x.a IN (SELECT y.a AS a FROM y));
SELECT * FROM x WHERE NOT (x.a IN (SELECT y.a AS a FROM y));

# title: NOT IN with UNION ALL subquery is not unnested
SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z);
SELECT * FROM x WHERE NOT x.a IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z);

# title: correlated EXISTS with negated equality is not unnested
SELECT x.id FROM x WHERE EXISTS (SELECT 1 FROM y WHERE NOT (y.id = x.id));
SELECT x.id FROM x WHERE EXISTS(SELECT 1 FROM y WHERE NOT (y.id = x.id));

# title: correlated NOT EXISTS with negated equality is not unnested
SELECT x.id FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE NOT (y.id = x.id));
SELECT x.id FROM x WHERE NOT EXISTS(SELECT 1 FROM y WHERE NOT (y.id = x.id));

# title: positive equality with NOT operand is unnested
SELECT x.flag FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.flag = (NOT x.flag));
SELECT x.flag FROM x LEFT JOIN (SELECT y.flag AS _u_1 FROM y WHERE TRUE GROUP BY y.flag) AS _u_0 ON _u_0._u_1 = (NOT x.flag) WHERE NOT _u_0._u_1 IS NULL;
