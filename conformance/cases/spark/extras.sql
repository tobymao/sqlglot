-- spark_extras only exists on Spark, so these cases only check the roundtrip.

# name: types
SELECT id, v_tinyint, v_map, v_struct, v_struct_arr, v_nested_arr, v_map_arr FROM spark_extras ORDER BY id;

# name: map_access
SELECT id, v_map['a'], MAP_KEYS(v_map), MAP_VALUES(v_map), SIZE(v_map), TRY_ELEMENT_AT(v_map, 'b') FROM spark_extras ORDER BY id;

# name: struct_access
SELECT id, v_struct.a, v_struct.b, TRANSFORM(v_struct_arr, s -> s.x) FROM spark_extras ORDER BY id;

# name: map_functions
SELECT id, MAP_CONTAINS_KEY(v_map, 'a'), MAP_FILTER(v_map, (k, v) -> v > 1), TRANSFORM_VALUES(v_map, (k, v) -> v * 10) FROM spark_extras ORDER BY id;

# name: tinyint_arithmetic
SELECT id, v_tinyint + 1, CAST(v_tinyint AS INT) * 2 FROM spark_extras ORDER BY id;

# name: explode_map
SELECT id, k, v FROM spark_extras LATERAL VIEW EXPLODE(v_map) t AS k, v ORDER BY id, k;
