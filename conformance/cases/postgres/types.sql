-- Reading each column back checks that values and type mappings agree across engines.

# name: boolean
SELECT id, v_bool FROM scalars ORDER BY id;

# name: integers
SELECT id, v_int2, v_int4, v_int8 FROM scalars ORDER BY id;

# name: floats
SELECT id, v_float4, v_float8 FROM scalars ORDER BY id;

# name: decimals
SELECT id, v_dec, v_dec38 FROM scalars ORDER BY id;

# name: strings
SELECT id, v_text, v_varchar, v_char FROM scalars ORDER BY id;

# name: binary
SELECT id, v_bytes FROM scalars ORDER BY id;

# name: temporal
SELECT id, v_date, v_ts, v_tstz FROM scalars ORDER BY id;

# name: arrays
SELECT id, v_int_arr, v_text_arr FROM scalars ORDER BY id;

# name: json
SELECT id, v_json FROM scalars ORDER BY id;

# name: literals
SELECT TRUE, 1, 2147483648, 1.5, 1e10, 'text', DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00', NULL;

# name: typed_literals
SELECT TIMESTAMPTZ '2024-01-15 10:30:00+02', INTERVAL '1 day', E'a\tb', X'0A', B'101';
