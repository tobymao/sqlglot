-- This is valid in Trino, so we treat the (tbl AS tbl) as a "join construct" per postgres' terminology.
SELECT * FROM (tbl AS tbl) AS _q_0;
SELECT * FROM (SELECT * FROM tbl AS tbl) AS _q_0;

SELECT * FROM ((tbl AS tbl)) AS _q_0;
SELECT * FROM (SELECT * FROM tbl AS tbl) AS _q_0;

SELECT * FROM (((tbl AS tbl))) AS _q_0;
SELECT * FROM (SELECT * FROM tbl AS tbl) AS _q_0;

SELECT * FROM (tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;
SELECT * FROM (SELECT * FROM tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;

SELECT * FROM ((tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3)) AS _q_0;
SELECT * FROM (SELECT * FROM tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2 JOIN tbl3 AS tbl3 ON id1 = id3) AS _q_0;

SELECT * FROM (tbl1 AS tbl1 JOIN (tbl2 AS tbl2 JOIN tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
SELECT * FROM (SELECT * FROM tbl1 AS tbl1 JOIN (SELECT * FROM tbl2 AS tbl2 JOIN tbl3 AS tbl3 ON id2 = id3) AS _q_0 ON id1 = id3) AS _q_1;
