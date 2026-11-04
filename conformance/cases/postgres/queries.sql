# name: distinct_on
SELECT DISTINCT ON (grp) grp, id, qty FROM facts ORDER BY grp, qty DESC, id;

# name: distinct
SELECT DISTINCT cat FROM facts ORDER BY cat;

# name: union
SELECT grp FROM facts WHERE id <= 50 UNION SELECT grp FROM facts WHERE id > 9990 ORDER BY 1;

# name: union_all
SELECT COUNT(*) FROM (SELECT grp FROM facts WHERE id <= 50 UNION ALL SELECT grp FROM facts WHERE id <= 50) AS t;

# name: intersect_except
SELECT k FROM join_left INTERSECT SELECT k FROM join_right ORDER BY 1;

# name: except
SELECT k FROM join_left EXCEPT SELECT k FROM join_right ORDER BY 1;

# name: cte
WITH t AS (SELECT grp, COUNT(*) AS c FROM facts GROUP BY grp) SELECT MAX(c), MIN(c), COUNT(*) FROM t;

# name: recursive_cte
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT SUM(n), MAX(n) FROM r;

# name: values
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS v(x, y) ORDER BY x;

# name: generate_series
SELECT n FROM GENERATE_SERIES(1, 5) AS g(n) ORDER BY n;

# name: generate_series_step
SELECT n FROM GENERATE_SERIES(10, 1, -3) AS g(n) ORDER BY n;

# name: limit_offset
SELECT id FROM facts ORDER BY id LIMIT 5 OFFSET 10;

# name: fetch_first
SELECT id FROM facts ORDER BY id DESC FETCH FIRST 3 ROWS ONLY;

# name: order_by_alias_and_position
SELECT grp AS g, COUNT(*) AS c FROM facts GROUP BY grp ORDER BY c DESC, 1 LIMIT 5;

# name: derived_column_aliases
SELECT a, b FROM (SELECT id, grp FROM facts WHERE id <= 5) AS t(a, b) ORDER BY a;

# name: in_list_and_between
SELECT COUNT(*) FROM facts WHERE grp IN (1, 2, 3) AND qty BETWEEN 10 AND 20;

# name: select_without_from
SELECT 1 + 2 AS three, 'a' AS letter;
