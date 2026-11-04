-- pg_extras only exists on Postgres, so these cases only check the roundtrip.

# name: types
SELECT id, v_uuid, v_time, v_interval, v_numeric, v_json, v_inet, v_int_matrix FROM pg_extras ORDER BY id;

# name: uuid_functions
SELECT id, CAST(v_uuid AS TEXT), v_uuid = '123e4567-e89b-12d3-a456-426614174000' FROM pg_extras ORDER BY id;

# name: interval_functions
SELECT id, EXTRACT(EPOCH FROM v_interval), v_interval * 2, JUSTIFY_HOURS(v_interval) FROM pg_extras ORDER BY id;

# name: time_arithmetic
SELECT id, v_time + INTERVAL '1 hour', EXTRACT(HOUR FROM v_time) FROM pg_extras ORDER BY id;

# name: inet_functions
SELECT id, HOST(v_inet), FAMILY(v_inet), MASKLEN(v_inet) FROM pg_extras ORDER BY id;
