# name: order_nulls_default
SELECT id, v_int4 FROM scalars ORDER BY v_int4, id;

# name: order_nulls_desc
SELECT id, v_int4 FROM scalars ORDER BY v_int4 DESC, id;

# name: order_nulls_last_explicit
SELECT id, v_text FROM scalars ORDER BY v_text DESC NULLS LAST, id;

# name: order_floats
SELECT id, v_float8 FROM scalars ORDER BY v_float8, id;

# name: null_functions
SELECT id, NVL(v_int4, -1), NVL2(v_int4, 'set', 'unset'), IFNULL(v_text, '?'), NULLIF(v_int4, 42), COALESCE(v_int4, v_int2, 0) FROM scalars ORDER BY id;

# name: if_case
SELECT id, IF(v_int4 > 0, 'positive', 'not positive'), CASE WHEN v_int4 IS NULL THEN 'null' ELSE 'set' END FROM scalars ORDER BY id;

# name: null_safe_equality
SELECT id, v_int4 <=> NULL, v_int4 <=> 42, NULL <=> NULL FROM scalars ORDER BY id;

# name: isnull
SELECT id, ISNULL(v_text), ISNOTNULL(v_text) FROM scalars ORDER BY id;
