# name: arrow_operators
SELECT id, v_json ->> 'a', v_json -> 'b', v_json -> 'key' -> 0 FROM scalars ORDER BY id;

# name: path_operators
SELECT id, v_json #>> '{key,0}', v_json #> '{nested,deep}' FROM scalars ORDER BY id;

# name: array_elements
SELECT id, v_json ->> 0, v_json -> 1 FROM scalars WHERE id IN (5, 7) ORDER BY id;

# name: build
SELECT JSON_BUILD_OBJECT('a', 1, 'b', 'x'), JSON_BUILD_ARRAY(1, 2, 3), TO_JSON('text'::TEXT);

# name: typeof
SELECT id, JSONB_TYPEOF(v_json) FROM scalars ORDER BY id;

# name: array_length
SELECT id, JSONB_ARRAY_LENGTH(v_json) FROM scalars WHERE id IN (5, 7) ORDER BY id;

# name: key_exists
SELECT id, v_json ? 'a' FROM scalars WHERE v_json IS NOT NULL ORDER BY id;

# name: parse_text
SELECT id, CAST(v_text AS JSON) ->> 'a' FROM scalars WHERE id = 19;
