# name: int_to_string
SELECT id, CAST(v_int4 AS STRING), CAST(v_int8 AS STRING) FROM scalars ORDER BY id;

# name: decimal_to_int
SELECT id, CAST(v_dec AS INT) FROM scalars WHERE v_dec IS NOT NULL ORDER BY id;

# name: float_to_int
SELECT id, CAST(v_float8 AS INT) FROM scalars WHERE ABS(v_float8) < 1000000 ORDER BY id;

# name: string_to_number
SELECT CAST('123' AS INT), CAST(' 42 ' AS INT), CAST('123.456' AS DECIMAL(10, 2)), CAST('1e3' AS DOUBLE);

# name: try_cast
SELECT TRY_CAST('abc' AS INT), TRY_CAST('12' AS INT), TRY_CAST('2024-13-01' AS DATE);

# name: string_to_temporal
SELECT CAST('2024-01-15' AS DATE), CAST('2024-01-15 10:30:00' AS TIMESTAMP), CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ);

# name: temporal_casts
SELECT id, CAST(v_ts AS DATE), CAST(v_date AS TIMESTAMP), CAST(v_tstz AS DATE), CAST(v_ts AS TIMESTAMP) FROM scalars ORDER BY id;

# name: boolean_casts
SELECT id, CAST(v_bool AS INT), CAST(v_int4 AS BOOLEAN), CAST(v_bool AS STRING) FROM scalars ORDER BY id;

# name: binary_functions
SELECT id, HEX(v_bytes), BASE64(v_bytes), LENGTH(v_bytes) FROM scalars ORDER BY id;

# name: double_to_string
SELECT id, CAST(v_float8 AS STRING) FROM scalars ORDER BY id;

# name: shorthand_casts
SELECT INT('42'), DOUBLE('1.5'), STRING(42), DATE('2024-01-15'), BOOLEAN('true');
