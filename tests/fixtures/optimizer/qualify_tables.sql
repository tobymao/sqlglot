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

----------------------------
-- Expand join constructs
----------------------------

-- This is valid in Trino, so we treat the (tbl AS tbl) as a "join construct" per postgres' terminology.
SELECT * FROM (tbl AS tbl) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

SELECT * FROM ((tbl AS tbl)) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

SELECT * FROM (((tbl AS tbl))) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl AS tbl) AS _q_0;

SELECT * FROM (tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

SELECT * FROM ((tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3)) AS _q_0;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN c.db.tbl2 AS tbl2 ON id1 = id2 JOIN c.db.tbl3 AS tbl3 ON id1 = id3) AS _q_0;

SELECT * FROM (tbl1 AS tbl1 JOIN (tbl2 AS tbl2 JOIN tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
SELECT * FROM (SELECT * FROM c.db.tbl1 AS tbl1 JOIN (SELECT * FROM c.db.tbl2 AS tbl2 JOIN c.db.tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
