# name: int_to_text
SELECT id, CAST(v_int4 AS TEXT), CAST(v_int8 AS VARCHAR) FROM scalars ORDER BY id;

# name: float_to_text
SELECT id, CAST(v_float8 AS TEXT), CAST(v_float4 AS TEXT) FROM scalars ORDER BY id;

# name: decimal_to_text
SELECT id, CAST(v_dec AS TEXT), CAST(v_dec38 AS TEXT) FROM scalars ORDER BY id;

# name: decimal_to_int
SELECT id, CAST(v_dec AS INTEGER) FROM scalars WHERE v_dec IS NOT NULL ORDER BY id;

# name: float_to_int
SELECT id, CAST(v_float8 AS INTEGER) FROM scalars WHERE ABS(v_float8) < 1000000 ORDER BY id;

# name: text_to_number
SELECT CAST('123' AS INTEGER), CAST(' 42 ' AS INTEGER), CAST('123.456' AS NUMERIC(10, 2)), CAST('-0.005' AS NUMERIC(10, 2)), CAST('1e3' AS DOUBLE PRECISION);

# name: text_to_temporal
SELECT CAST('2024-01-15' AS DATE), CAST('2024-01-15 10:30:00' AS TIMESTAMP), CAST('2024-01-15 10:30:00+02' AS TIMESTAMPTZ);

# name: timestamp_to_date
SELECT id, CAST(v_ts AS DATE), CAST(v_date AS TIMESTAMP) FROM scalars ORDER BY id;

# name: timestamptz_to_timestamp
SELECT id, CAST(v_tstz AS DATE), CAST(v_tstz AS TIMESTAMP), CAST(v_ts AS TIMESTAMPTZ) FROM scalars ORDER BY id;

# name: boolean_casts
SELECT id, CAST(v_bool AS INTEGER), CAST(v_int4 AS BOOLEAN), CAST(v_bool AS TEXT) FROM scalars ORDER BY id;

# name: temporal_to_text
SELECT id, CAST(v_date AS TEXT), CAST(v_ts AS TEXT) FROM scalars ORDER BY id;

# name: double_colon
SELECT '42'::INT + 1, '3.5'::NUMERIC * 2, 'abc'::VARCHAR(2), 7::DOUBLE PRECISION / 2;

# name: binary_to_text
SELECT id, ENCODE(v_bytes, 'hex'), ENCODE(v_bytes, 'base64') FROM scalars ORDER BY id;
