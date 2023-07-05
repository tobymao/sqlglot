SELECT 1 FROM z;
SELECT 1 FROM c.db.z AS z;

SELECT 1 FROM y.z;
SELECT 1 FROM c.y.z AS z;

SELECT 1 FROM x.y.z;
SELECT 1 FROM x.y.z AS z;

SELECT 1 FROM x.y.z AS z;
SELECT 1 FROM x.y.z AS z;

WITH a AS (SELECT 1 FROM z) SELECT 1 FROM a;
WITH a AS (SELECT 1 FROM c.db.z AS z) SELECT 1 FROM a;

SELECT (SELECT y.c FROM y AS y) FROM x;
SELECT (SELECT y.c FROM c.db.y AS y) FROM c.db.x AS x;

SELECT * FROM x PIVOT (SUM(a) FOR b IN ('a', 'b'));
SELECT * FROM c.db.x AS x PIVOT(SUM(a) FOR b IN ('a', 'b')) AS _q_0;

-----------------------------------
-- Unnest wrapped tables / joins
-----------------------------------

# title: redundant parentheses (1)
SELECT * FROM (tbl AS tbl);
SELECT * FROM c.db.tbl AS tbl;

# title: redundant parentheses (2)
SELECT * FROM ((((tbl AS tbl))));
SELECT * FROM c.db.tbl AS tbl;

# title: redundant parentheses around join construct
SELECT * FROM a LEFT JOIN (b INNER JOIN c ON c.id = b.id) ON b.id = a.id;
SELECT * FROM c.db.a AS a LEFT JOIN c.db.b AS b ON b.id = a.id INNER JOIN c.db.c AS c ON c.id = b.id;

# title: chained joins converted to canonical form
SELECT * FROM a LEFT JOIN b INNER JOIN c ON c.id = b.id ON b.id = a.id;
SELECT * FROM c.db.a AS a LEFT JOIN c.db.b AS b ON b.id = a.id INNER JOIN c.db.c AS c ON c.id = b.id;

# title: parentheses can't be omitted because alias shadows inner table names
SELECT * FROM (tbl AS tbl) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

# title: outermost set of parentheses can't be omitted due to shadowing (1)
SELECT * FROM ((tbl AS tbl)) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

# title: outermost set of parentheses can't be omitted due to shadowing (2)
SELECT * FROM (((tbl AS tbl))) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

# title: join construct with three tables in canonical form
SELECT * FROM (tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

# title: join construct with three tables in canonical form and redundant set of parentheses
SELECT * FROM ((tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3)) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

# title: nested join construct in canonical form
SELECT * FROM (tbl1 AS tbl1 JOIN (tbl2 AS tbl2 JOIN tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN (SELECT * FROM c.db.tbl2 AS tbl2 JOIN c.db.tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
