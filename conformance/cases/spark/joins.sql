# name: inner
SELECT l.id, r.id FROM join_left AS l JOIN join_right AS r ON l.k = r.k ORDER BY 1, 2;

# name: full
SELECT l.id, r.id FROM join_left AS l FULL OUTER JOIN join_right AS r ON l.k = r.k ORDER BY 1, 2;

# name: null_safe
SELECT l.id, r.id FROM join_left AS l JOIN join_right AS r ON l.k <=> r.k ORDER BY 1, 2;

# name: left_semi
SELECT l.id FROM join_left AS l LEFT SEMI JOIN join_right AS r ON l.k = r.k ORDER BY 1;

# name: left_anti
SELECT l.id FROM join_left AS l LEFT ANTI JOIN join_right AS r ON l.k = r.k ORDER BY 1;

# name: using
SELECT k, l.val, r.val FROM join_left AS l JOIN join_right AS r USING (k) ORDER BY 1, 2, 3;

# name: not_in_with_nulls
SELECT id FROM join_left WHERE k NOT IN (SELECT k FROM join_right) ORDER BY id;

# name: lateral_view_explode
SELECT s.id, x FROM scalars AS s LATERAL VIEW EXPLODE(s.v_int_arr) t AS x ORDER BY s.id, x;

# name: lateral_view_outer
SELECT s.id, x FROM scalars AS s LATERAL VIEW OUTER EXPLODE(s.v_int_arr) t AS x ORDER BY s.id, x;

# name: posexplode
SELECT s.id, pos, x FROM scalars AS s LATERAL VIEW POSEXPLODE(s.v_text_arr) t AS pos, x ORDER BY s.id, pos;
