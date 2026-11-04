# name: distinct
SELECT DISTINCT cat FROM facts ORDER BY cat;

# name: union
SELECT grp FROM facts WHERE id <= 50 UNION SELECT grp FROM facts WHERE id > 9990 ORDER BY 1;

# name: intersect_except
SELECT k FROM join_left INTERSECT SELECT k FROM join_right ORDER BY 1;

# name: except_all
SELECT k FROM join_left EXCEPT ALL SELECT k FROM join_right ORDER BY 1;

# name: cte
WITH t AS (SELECT grp, COUNT(*) AS c FROM facts GROUP BY grp) SELECT MAX(c), MIN(c), COUNT(*) FROM t;

# name: values
SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, NULL) AS v(x, y) ORDER BY x;

# name: range
SELECT id FROM RANGE(5) ORDER BY id;

# name: limit_offset
SELECT id FROM facts ORDER BY id LIMIT 5 OFFSET 10;

# name: pivot
SELECT * FROM (SELECT grp, cat, qty FROM facts WHERE grp < 3 AND cat IN ('alpha', 'beta')) PIVOT (SUM(qty) FOR cat IN ('alpha' AS alpha, 'beta' AS beta)) ORDER BY grp;

# name: unpivot
SELECT * FROM (SELECT id, v_int2, v_int4 FROM scalars WHERE id <= 4) UNPIVOT (val FOR col IN (v_int2, v_int4)) ORDER BY id, col;

# name: derived_column_aliases
SELECT a, b FROM (SELECT id, grp FROM facts WHERE id <= 5) AS t(a, b) ORDER BY a;

# name: inline
SELECT * FROM INLINE(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))) ORDER BY 1;

# name: recursive_cte
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT SUM(n), MAX(n) FROM r;
