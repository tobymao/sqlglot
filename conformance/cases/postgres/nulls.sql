# name: order_nulls_default
SELECT id, v_int4 FROM scalars ORDER BY v_int4, id;

# name: order_nulls_desc
SELECT id, v_int4 FROM scalars ORDER BY v_int4 DESC, id;

# name: order_nulls_explicit
SELECT id, v_text FROM scalars ORDER BY v_text NULLS FIRST, id;

# name: order_floats
SELECT id, v_float8 FROM scalars ORDER BY v_float8, id;

# name: coalesce_nullif
SELECT id, COALESCE(v_int4, -1), NULLIF(v_int4, 42), v_int4 IS NULL, v_int4 IS NOT NULL FROM scalars ORDER BY id;

# name: case
SELECT id, CASE WHEN v_int4 > 0 THEN 'positive' WHEN v_int4 < 0 THEN 'negative' WHEN v_int4 = 0 THEN 'zero' ELSE 'null' END FROM scalars ORDER BY id;

# name: simple_case
SELECT id, CASE v_int4 WHEN 42 THEN 'answer' WHEN 0 THEN 'zero' END FROM scalars ORDER BY id;

# name: three_valued_logic
SELECT NULL = NULL, NULL IS NULL, 1 IS DISTINCT FROM NULL, NULL IS NOT DISTINCT FROM NULL, NULL AND FALSE, NULL OR TRUE, NOT NULL;

# name: boolean_tests
SELECT id, v_bool IS TRUE, v_bool IS NOT FALSE, v_bool IS UNKNOWN FROM scalars ORDER BY id;

# name: null_propagation
SELECT id, v_int4 + NULL, v_text || NULL, UPPER(NULL), v_int4 > NULL FROM scalars WHERE id <= 3 ORDER BY id;
