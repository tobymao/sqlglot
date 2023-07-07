# title: single table
SELECT 1 FROM z;
SELECT 1 FROM c.db.z AS z;

# title: single table with db
SELECT 1 FROM y.z;
SELECT 1 FROM c.y.z AS z;

# title: single table with db, catalog
SELECT 1 FROM x.y.z;
SELECT 1 FROM x.y.z AS z;

# title: single table with db, catalog, alias
SELECT 1 FROM x.y.z AS z;
SELECT 1 FROM x.y.z AS z;

# title: cte can't be qualified
WITH a AS (SELECT 1 FROM z) SELECT 1 FROM a;
WITH a AS (SELECT 1 FROM c.db.z AS z) SELECT 1 FROM a;

# title: query that yields a single column as projection
SELECT (SELECT y.c FROM y AS y) FROM x;
SELECT (SELECT y.c FROM c.db.y AS y) FROM c.db.x AS x;

# title: pivoted table
SELECT * FROM x PIVOT (SUM(a) FOR b IN ('a', 'b'));
SELECT * FROM c.db.x AS x PIVOT(SUM(a) FOR b IN ('a', 'b')) AS _q_0;

-----------------------------------------------------------
--- Unnest wrapped tables / joins, expand join constructs
-----------------------------------------------------------

-- # title: wrapped table without alias
-- SELECT * FROM (tbl);
-- SELECT * FROM c.db.tbl AS tbl;

-- # title: wrapped table with alias
-- SELECT * FROM (tbl AS tbl);
-- SELECT * FROM c.db.tbl AS tbl;

-- # title: wrapped table with alias and multiple redundant parentheses
-- SELECT * FROM ((((tbl AS tbl))));
-- SELECT * FROM c.db.tbl AS tbl;

-- # title: chained wrapped joins without aliases (1)
-- SELECT * FROM ((a CROSS JOIN b) CROSS JOIN c);
-- SELECT * FROM c.db.a AS a CROSS JOIN c.db.b AS b CROSS JOIN c.db.c AS c;

-- # title: chained wrapped joins without aliases (2)
-- SELECT * FROM (a CROSS JOIN (b CROSS JOIN c));
-- SELECT * FROM c.db.a AS a CROSS JOIN c.db.b AS b CROSS JOIN c.db.c AS c;

-- # title: chained wrapped joins without aliases (3)
-- SELECT * FROM ((a CROSS JOIN ((b CROSS JOIN c) CROSS JOIN d)));
-- SELECT * FROM c.db.a AS a CROSS JOIN c.db.b AS b CROSS JOIN c.db.c AS c CROSS JOIN c.db.d AS d;

-- # title: chained wrapped joins without aliases (4)
-- SELECT * FROM ((a CROSS JOIN ((b CROSS JOIN c) CROSS JOIN (d CROSS JOIN e))));
-- SELECT * FROM c.db.a AS a CROSS JOIN c.db.b AS b CROSS JOIN c.db.c AS c CROSS JOIN c.db.d AS d CROSS JOIN c.db.e AS e;

-- # title: chained wrapped joins with aliases
-- SELECT * FROM ((a AS foo CROSS JOIN b AS bar) CROSS JOIN c AS baz);
-- SELECT * FROM c.db.a AS foo CROSS JOIN c.db.b AS bar CROSS JOIN c.db.c AS baz;

-- # title: wrapped join with subquery without alias
-- SELECT * FROM (tbl1 CROSS JOIN (SELECT * FROM tbl2) AS t1);
-- SELECT * FROM c.db.tbl1 AS tbl1 CROSS JOIN (SELECT * FROM c.db.tbl2 AS tbl2) AS t1;

-- # title: wrapped join with subquery with alias, parentheses can't be omitted because of alias
-- SELECT * FROM (tbl1 CROSS JOIN (SELECT * FROM tbl2) AS t1) AS t2;
-- SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 CROSS JOIN (SELECT * FROM c.db.tbl2 AS tbl2) AS t1) AS t2;

-- # title: join construct as the right operand of a left join
-- SELECT * FROM a LEFT JOIN (b INNER JOIN c ON c.id = b.id) ON b.id = a.id;
-- SELECT * FROM c.db.a AS a LEFT JOIN c.db.b AS b ON b.id = a.id INNER JOIN c.db.c AS c ON c.id = b.id;

-- # title: nested joins converted to canonical form
-- SELECT * FROM a LEFT JOIN b INNER JOIN c ON c.id = b.id ON b.id = a.id;
-- SELECT * FROM c.db.a AS a LEFT JOIN c.db.b AS b ON b.id = a.id INNER JOIN c.db.c AS c ON c.id = b.id;

-- # title: parentheses can't be omitted because alias shadows inner table names
-- SELECT t.a FROM (tbl AS tbl) AS t;
-- SELECT t.a FROM (SELECT * FROM c.db.tbl AS tbl) AS t;

-- # title: outermost set of parentheses can't be omitted due to shadowing (1)
-- SELECT * FROM ((tbl AS tbl)) AS _q_0;
-- SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

-- # title: outermost set of parentheses can't be omitted due to shadowing (2)
-- SELECT * FROM ((((tbl AS tbl)))) AS _q_0;
-- SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

-- # title: join construct with three tables in canonical form
-- SELECT * FROM (tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;
-- SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

-- # title: join construct with three tables in canonical form and redundant set of parentheses
-- SELECT * FROM ((tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3)) AS _q_0;
-- SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

-- # title: nested join construct in canonical form
-- SELECT * FROM (tbl1 AS tbl1 JOIN (tbl2 AS tbl2 JOIN tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
-- SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN (SELECT * FROM c.db.tbl2 AS tbl2 JOIN c.db.tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
