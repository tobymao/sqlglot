-- Spark has no JSON type: these functions operate on JSON strings, which map to Postgres JSONB.

# name: get_json_object
SELECT id, GET_JSON_OBJECT(v_json, '$.a'), GET_JSON_OBJECT(v_json, '$.key[0]'), GET_JSON_OBJECT(v_json, '$.nested.deep') FROM scalars ORDER BY id;

# name: to_json
SELECT TO_JSON(NAMED_STRUCT('a', 1, 'b', 'x')), TO_JSON(ARRAY(1, 2));

# name: json_array_length
SELECT id, JSON_ARRAY_LENGTH(v_json), JSON_OBJECT_KEYS(v_json) FROM scalars ORDER BY id;
