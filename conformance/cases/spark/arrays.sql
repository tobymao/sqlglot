# name: size
SELECT id, SIZE(v_int_arr), CARDINALITY(v_text_arr), ARRAY_SIZE(v_int_arr) FROM scalars ORDER BY id;

# name: element_access
SELECT id, TRY_ELEMENT_AT(v_int_arr, 1), TRY_ELEMENT_AT(v_int_arr, 5), GET(v_int_arr, 0) FROM scalars ORDER BY id;

# name: subscript
SELECT id, v_int_arr[0], v_text_arr[1] FROM scalars WHERE SIZE(v_text_arr) > 1 AND SIZE(v_int_arr) > 0 ORDER BY id;

# name: contains_position
SELECT id, ARRAY_CONTAINS(v_int_arr, 2), ARRAY_POSITION(v_int_arr, 3), ARRAYS_OVERLAP(v_int_arr, ARRAY(1, 5)) FROM scalars ORDER BY id;

# name: transform
SELECT id, ARRAY_DISTINCT(v_int_arr), ARRAY_SORT(v_int_arr), ARRAY_MAX(v_int_arr), ARRAY_MIN(v_int_arr), REVERSE(v_int_arr) FROM scalars ORDER BY id;

# name: set_operations
SELECT id, ARRAY_UNION(v_int_arr, ARRAY(1, 9)), ARRAY_INTERSECT(v_int_arr, ARRAY(1, 3)), ARRAY_EXCEPT(v_int_arr, ARRAY(1)) FROM scalars ORDER BY id;

# name: higher_order
SELECT id, TRANSFORM(v_int_arr, x -> x * 2), FILTER(v_int_arr, x -> x > 1), AGGREGATE(v_int_arr, 0, (acc, x) -> acc + COALESCE(x, 0)), EXISTS(v_int_arr, x -> x IS NULL) FROM scalars WHERE id <> 6 ORDER BY id;

# name: array_join
SELECT id, ARRAY_JOIN(v_text_arr, ','), ARRAY_JOIN(v_text_arr, ',', '*'), CONCAT_WS('|', v_text_arr) FROM scalars ORDER BY id;

# name: constructors
SELECT ARRAY(1, 2, 3), ARRAY_REPEAT('a', 3), SEQUENCE(1, 5), SEQUENCE(10, 1, -3);

# name: slice_flatten
SELECT id, SLICE(v_int_arr, 1, 2), FLATTEN(ARRAY(v_int_arr, ARRAY(9))) FROM scalars ORDER BY id;

# name: explode_subquery
SELECT x FROM (SELECT EXPLODE(ARRAY(3, 1, 2)) AS x) ORDER BY x;
