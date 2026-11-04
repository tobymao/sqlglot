# name: length
SELECT id, ARRAY_LENGTH(v_int_arr, 1), CARDINALITY(v_int_arr) FROM scalars ORDER BY id;

# name: subscript
SELECT id, v_int_arr[1], v_text_arr[2] FROM scalars ORDER BY id;

# name: subscript_out_of_bounds
SELECT id, v_int_arr[10] FROM scalars ORDER BY id;

# name: constructors
SELECT ARRAY[1, 2, 3], ARRAY['a', NULL], ARRAY[]::INT[], ARRAY[[1, 2], [3, 4]];

# name: to_string
SELECT id, ARRAY_TO_STRING(v_text_arr, ','), ARRAY_TO_STRING(v_int_arr, '-', '*') FROM scalars ORDER BY id;

# name: membership
SELECT id, 2 = ANY(v_int_arr), ARRAY_POSITION(v_int_arr, 3), v_int_arr @> ARRAY[1] FROM scalars ORDER BY id;

# name: concat
SELECT id, ARRAY_CAT(v_int_arr, ARRAY[9]), ARRAY_APPEND(v_int_arr, 9), ARRAY_PREPEND(0, v_int_arr) FROM scalars ORDER BY id;

# name: unnest
SELECT s.id, x FROM scalars AS s, UNNEST(s.v_int_arr) AS x ORDER BY s.id, x;

# name: unnest_column_alias
SELECT s.id, u.x FROM scalars AS s CROSS JOIN UNNEST(s.v_int_arr) AS u(x) ORDER BY s.id, u.x;

# name: unnest_with_ordinality
SELECT s.id, x, n FROM scalars AS s CROSS JOIN UNNEST(s.v_text_arr) WITH ORDINALITY AS u(x, n) ORDER BY s.id, n;

# name: string_to_array
SELECT id, STRING_TO_ARRAY(v_text, ' ') FROM scalars ORDER BY id;

# name: slice
SELECT id, v_int_arr[1:2], v_int_arr[2:] FROM scalars ORDER BY id;
