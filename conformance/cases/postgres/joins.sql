# name: inner
SELECT l.id, r.id FROM join_left AS l JOIN join_right AS r ON l.k = r.k ORDER BY 1, 2;

# name: left
SELECT l.id, r.id FROM join_left AS l LEFT JOIN join_right AS r ON l.k = r.k ORDER BY 1, 2;

# name: right
SELECT l.id, r.id FROM join_left AS l RIGHT JOIN join_right AS r ON l.k = r.k ORDER BY 2, 1;

# name: full
SELECT l.id, r.id FROM join_left AS l FULL OUTER JOIN join_right AS r ON l.k = r.k ORDER BY 1, 2;

# name: null_safe
SELECT l.id, r.id FROM join_left AS l JOIN join_right AS r ON l.k IS NOT DISTINCT FROM r.k ORDER BY 1, 2;

# name: using
SELECT k, l.val, r.val FROM join_left AS l JOIN join_right AS r USING (k) ORDER BY 1, 2, 3;

# name: cross
SELECT COUNT(*), SUM(l.id * r.id) FROM join_left AS l CROSS JOIN join_right AS r;

# name: exists
SELECT id FROM join_left AS l WHERE EXISTS (SELECT 1 FROM join_right AS r WHERE r.k = l.k) ORDER BY id;

# name: not_exists
SELECT id FROM join_left AS l WHERE NOT EXISTS (SELECT 1 FROM join_right AS r WHERE r.k = l.k) ORDER BY id;

# name: in_subquery
SELECT id FROM join_left WHERE k IN (SELECT k FROM join_right) ORDER BY id;

# name: not_in_with_nulls
SELECT id FROM join_left WHERE k NOT IN (SELECT k FROM join_right) ORDER BY id;

# name: not_in_without_nulls
SELECT id FROM join_left WHERE k NOT IN (SELECT k FROM join_right WHERE k IS NOT NULL) ORDER BY id;

# name: scalar_subquery
SELECT l.id, (SELECT COUNT(*) FROM join_right AS r WHERE r.k = l.k) FROM join_left AS l ORDER BY l.id;

# name: lateral
SELECT l.id, x.m FROM join_left AS l, LATERAL (SELECT MAX(r.id) AS m FROM join_right AS r WHERE r.k = l.k) AS x ORDER BY l.id;

# name: self_join
SELECT a.id, b.id FROM facts AS a JOIN facts AS b ON a.grp = b.grp AND a.id < b.id WHERE a.id <= 20 AND b.id <= 20 ORDER BY 1, 2;
