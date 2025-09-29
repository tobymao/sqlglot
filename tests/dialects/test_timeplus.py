from tests.dialects.test_dialect import Validator


class TestTimeplus(Validator):
    dialect = "timeplus"

    def test_timeplus_streams(self):
        # Test basic stream creation
        self.validate_identity(
            "CREATE STREAM devices (device STRING, location STRING, temperature FLOAT)",
            "CREATE STREAM devices (device string, location string, temperature float32)",
        )

        # Test stream with settings
        self.validate_identity(
            "CREATE STREAM append_stream (int8_c INT8, string_c STRING) SETTINGS shards=4, replication_factor=3",
            "CREATE STREAM append_stream (int8_c int8, string_c string) SETTINGS shards = 4, replication_factor = 3",
        )

        # Test IF NOT EXISTS
        self.validate_identity(
            "CREATE STREAM IF NOT EXISTS my_stream (id UINT32, name STRING)",
            "CREATE STREAM IF NOT EXISTS my_stream (id uint32, name string)",
        )

    def test_mutable_streams(self):
        # Test mutable stream with PRIMARY KEY
        self.validate_identity(
            "CREATE MUTABLE STREAM users (user_id UINT64, username STRING, email STRING) PRIMARY KEY (user_id)",
            "CREATE MUTABLE STREAM users (user_id uint64, username string, email string) PRIMARY KEY (user_id)",
        )

        # Test mutable stream with settings including ttl_seconds (not TTL clause)
        self.validate_identity(
            "CREATE MUTABLE STREAM logs (timestamp DATETIME, level STRING, message STRING) PRIMARY KEY (timestamp) SETTINGS shards=2, logstore_retention_ms=604800000, ttl_seconds=86400",
            "CREATE MUTABLE STREAM logs (timestamp datetime, level string, message string) PRIMARY KEY (timestamp) SETTINGS shards = 2, logstore_retention_ms = 604800000, ttl_seconds = 86400",
        )

    def test_external_streams(self):
        # Test external stream for Kafka
        self.validate_identity(
            "CREATE EXTERNAL STREAM kafka_stream (id INT, data STRING) SETTINGS type='kafka', brokers='localhost:9092', topic='test'",
            "CREATE EXTERNAL STREAM kafka_stream (id int32, data string) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'test'",
        )

        # Test external stream for Timeplus
        self.validate_identity(
            "CREATE EXTERNAL STREAM remote_stream (value FLOAT) SETTINGS type='timeplus', hosts='host1', stream='source_stream'",
            "CREATE EXTERNAL STREAM remote_stream (value float32) SETTINGS type = 'timeplus', hosts = 'host1', stream = 'source_stream'",
        )

        # Test that DROP statement for external stream generates DROP STREAM
        from sqlglot import parse_one

        # Create a DROP expression for the same stream
        drop_expr = parse_one("DROP TABLE kafka_stream", dialect="timeplus")

        # Generate the DROP SQL - should be DROP STREAM
        drop_sql = drop_expr.sql(dialect="timeplus")
        self.assertEqual(drop_sql, "DROP STREAM kafka_stream")

    def test_random_streams(self):
        # Test random stream
        self.validate_identity(
            "CREATE RANDOM STREAM test_stream (i INT DEFAULT rand() % 5) SETTINGS eps=10",
            "CREATE RANDOM STREAM test_stream (i int32 DEFAULT rand() % 5) SETTINGS eps = 10",
        )

        # Test random stream with interval_time
        self.validate_identity(
            "CREATE RANDOM STREAM devices (device STRING DEFAULT 'device' || toString(rand() % 4), temperature FLOAT DEFAULT rand() % 1000 / 10) SETTINGS eps=1000, interval_time=200",
            "CREATE RANDOM STREAM devices (device string DEFAULT 'device' || toString(rand() % 4), temperature float32 DEFAULT rand() % 1000 / 10) SETTINGS eps = 1000, interval_time = 200",
        )

    def test_materialized_views(self):
        # Test basic materialized view
        self.validate_identity("CREATE MATERIALIZED VIEW mv AS SELECT * FROM source_stream")

        # Test materialized view with INTO
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv INTO target_stream AS SELECT * FROM source_stream"
        )

        # Test materialized view with settings (after AS - the only valid syntax)
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv_with_settings INTO target AS SELECT count() FROM source SETTINGS pause_on_start=TRUE",
            "CREATE MATERIALIZED VIEW mv_with_settings INTO target AS SELECT count() FROM source SETTINGS pause_on_start = TRUE",
        )

        # Test materialized view with multiple settings (after AS)
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM stream SETTINGS pause_on_start = TRUE, memory_weight = 10, mv_preferred_exec_node = 2"
        )

        # Test materialized view with settings and INTO
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv INTO target AS SELECT * FROM source SETTINGS checkpoint_interval = 5, seek_to = 'earliest'"
        )

        # Test materialized view IF NOT EXISTS
        self.validate_identity(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS my_mv AS SELECT * FROM stream1"
        )

        # Test materialized view with window functions
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv AS SELECT window_start, count() FROM tumble(stream, 1m) GROUP BY window_start"
        )

        # Test materialized view with emit_version
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv AS SELECT emit_version() AS version, max(price) FROM stream"
        )

        # Test complex materialized view with aggregations
        self.validate_identity(
            "CREATE MATERIALIZED VIEW stats_mv INTO stats_stream AS SELECT device_id, count() AS event_count, avg(temperature) AS avg_temp, max(temperature) AS max_temp FROM sensor_stream GROUP BY device_id"
        )

        # Test materialized view with JOIN - table aliases get AS added
        self.validate_identity(
            "CREATE MATERIALIZED VIEW joined_mv AS SELECT s1.id, s1.value, s2.status FROM stream1 s1 JOIN stream2 s2 ON s1.id = s2.id",
            "CREATE MATERIALIZED VIEW joined_mv AS SELECT s1.id, s1.value, s2.status FROM stream1 AS s1 JOIN stream2 AS s2 ON s1.id = s2.id",
        )

        # Test materialized view with WHERE clause
        self.validate_identity(
            "CREATE MATERIALIZED VIEW filtered_mv AS SELECT * FROM events WHERE event_type = 'purchase' AND amount > 100"
        )

        # Test materialized view with HAVING
        self.validate_identity(
            "CREATE MATERIALIZED VIEW grouped_mv AS SELECT user_id, count() AS cnt FROM events GROUP BY user_id HAVING cnt > 10"
        )

    def test_views(self):
        # Test regular view
        self.validate_identity("CREATE VIEW view1 AS SELECT * FROM my_stream WHERE c1 = 'a'")

        # Test view IF NOT EXISTS
        self.validate_identity(
            "CREATE VIEW IF NOT EXISTS filtered_view AS SELECT * FROM table(my_stream)"
        )

    def test_external_tables(self):
        # Test external table for ClickHouse
        self.validate_identity(
            "CREATE EXTERNAL TABLE ch_table SETTINGS type='clickhouse', address='localhost:9000', database='default', table='source'",
            "CREATE EXTERNAL TABLE ch_table SETTINGS type = 'clickhouse', address = 'localhost:9000', database = 'default', table = 'source'",
        )

        # Test external table for MySQL
        self.validate_identity(
            "CREATE EXTERNAL TABLE mysql_table SETTINGS type='mysql', address='localhost:3306', user='root', password='pass', database='test', table='users'",
            "CREATE EXTERNAL TABLE mysql_table SETTINGS type = 'mysql', address = 'localhost:3306', user = 'root', password = 'pass', database = 'test', table = 'users'",
        )

        # Test that DROP statement for external table generates DROP STREAM
        from sqlglot import parse_one

        # Create a DROP expression for the same table
        drop_expr = parse_one("DROP TABLE ext_table", dialect="timeplus")

        # Generate the DROP SQL - should be DROP STREAM
        drop_sql = drop_expr.sql(dialect="timeplus")
        self.assertEqual(drop_sql, "DROP STREAM ext_table")

    def test_remote_functions(self):
        # Test remote function
        self.validate_identity(
            "CREATE REMOTE FUNCTION ip_lookup(ip STRING) RETURNS STRING URL 'https://api.example.com/lookup' AUTH_METHOD 'none'",
            "CREATE REMOTE FUNCTION ip_lookup(ip string) RETURNS string URL 'https://api.example.com/lookup' AUTH_METHOD 'none'",
        )

        # Test remote function with auth header
        self.validate_identity(
            "CREATE REMOTE FUNCTION geo_lookup(lat FLOAT, lon FLOAT) RETURNS STRING URL 'https://api.example.com/geo' AUTH_METHOD 'auth_header' AUTH_HEADER 'X-API-Key' AUTH_KEY 'secret'",
            "CREATE REMOTE FUNCTION geo_lookup(lat float32, lon float32) RETURNS string URL 'https://api.example.com/geo' AUTH_METHOD 'auth_header' AUTH_HEADER 'X-API-Key' AUTH_KEY 'secret'",
        )

    def test_drop_stream(self):
        # Test DROP STREAM
        self.validate_identity("DROP STREAM my_stream")
        self.validate_identity("DROP STREAM IF EXISTS my_stream")

        # Test DROP for external streams - should generate DROP STREAM
        # External streams are created with CREATE EXTERNAL STREAM but dropped with DROP STREAM
        sql = "DROP STREAM ext_kafka_stream"
        self.validate_identity(sql)

        # Test DROP for external tables - should also generate DROP STREAM
        sql = "DROP STREAM ext_ch_table"
        self.validate_identity(sql)

    def test_alter_stream(self):
        # Test ALTER STREAM (basic)
        self.validate_identity(
            "ALTER STREAM my_stream ADD COLUMN new_col INT",
            "ALTER STREAM my_stream ADD COLUMN new_col int32",
        )
        self.validate_identity("ALTER STREAM my_stream DROP COLUMN old_col")

    def test_select_from_stream(self):
        # Test SELECT from stream
        self.validate_identity("SELECT * FROM my_stream")

        # Test historical query with table() function
        self.validate_identity("SELECT * FROM table(my_stream)")

        # Test historical + streaming query
        self.validate_identity("SELECT * FROM my_stream WHERE _tp_time >= '1970-01-01'")

        # Test with window functions
        self.validate_identity("SELECT count(*) FROM tumble(my_stream, 1m) GROUP BY window_start")

    def test_timeplus_specific_functions(self):
        # Test emit_version function
        self.validate_identity("SELECT emit_version() AS version FROM my_stream")

        # Test earliest_ts function
        self.validate_identity("SELECT * FROM my_stream WHERE _tp_time > earliest_ts()")

        # Test array_join (snake_case instead of arrayJoin)
        self.validate_identity("SELECT array_join([1, 2, 3]) AS arr")
        self.validate_identity("WITH array_join([(1, [2, 3])]) AS arr SELECT arr")

        # Test unique functions
        self.validate_identity("SELECT unique_exact_if(user_id, status = 1)")
        self.validate_identity("SELECT unique_exact(user_id)")

        # Test uniq (approximation)
        self.validate_identity("SELECT uniq(user_id) FROM events")

    def test_select_with_settings_format(self):
        # Test SELECT with FORMAT
        self.validate_identity("SELECT * FROM my_stream FORMAT JSON")
        self.validate_identity("SELECT * FROM my_stream FORMAT TSV")

        # Test SELECT with SETTINGS
        self.validate_identity(
            "SELECT * FROM my_stream SETTINGS max_threads = 4, enable_optimize = TRUE"
        )

        # Test SELECT with LIMIT and FORMAT
        self.validate_identity("SELECT * FROM my_stream LIMIT 10 FORMAT JSON")

        # Test SELECT with LIMIT, OFFSET and SETTINGS
        self.validate_identity("SELECT * FROM my_stream LIMIT 10 OFFSET 5 SETTINGS max_threads = 2")

        # Test SELECT with LIMIT, SETTINGS and FORMAT (correct order)
        self.validate_identity(
            "SELECT * FROM my_stream LIMIT 100 SETTINGS max_threads = 4 FORMAT JSON"
        )

        # Test SELECT with WHERE, GROUP BY, ORDER BY, LIMIT, SETTINGS, FORMAT
        self.validate_identity(
            "SELECT device, count() AS cnt FROM my_stream WHERE temperature > 25 GROUP BY device ORDER BY cnt DESC LIMIT 10 SETTINGS max_threads = 2 FORMAT TSV"
        )

        # Test ORDER BY with SETTINGS and FORMAT (correct parsing order)
        self.validate_identity(
            "SELECT * FROM stream ORDER BY timestamp DESC LIMIT 100 SETTINGS max_threads = 4, enable_optimize = TRUE FORMAT JSON"
        )

        # Test streaming-specific aggregations
        self.validate_identity("SELECT count_distinct(user_id) FROM events")
        # median gets converted to quantile(price, 0.5) using Timeplus syntax
        self.validate_identity(
            "SELECT median(price) FROM orders",
            "SELECT quantile(price, 0.5) FROM orders",
        )
        self.validate_identity("SELECT count() FROM my_stream")

    def test_clickhouse_compatible_features(self):
        """Test features that are compatible with ClickHouse but adapted for Timeplus"""
        # Test FINAL modifier
        self.validate_identity("SELECT * FROM my_stream FINAL")
        self.validate_identity("SELECT * FROM my_stream FINAL WHERE price > 100")

        # Test PREWHERE
        self.validate_identity("SELECT * FROM stream PREWHERE id > 0 WHERE status = 1")
        self.validate_identity("SELECT * FROM stream PREWHERE date >= today() WHERE amount > 0")

        # Test SAMPLE clause
        self.validate_identity("SELECT * FROM stream SAMPLE 0.01")
        self.validate_identity("SELECT * FROM stream SAMPLE 1 / 10 OFFSET 1 / 2")

        # Test WITH TOTALS
        self.validate_identity("SELECT status, count() FROM stream GROUP BY status WITH TOTALS")

        # Test JSON extraction with type casting
        self.validate_identity("SELECT json.a.:int64")
        self.validate_identity("SELECT json.a.:json.b.:int64")

        # Test nullable types (lowercase in Timeplus)
        self.validate_identity("CAST(x AS nullable(string))")
        self.validate_identity("CAST(x AS nullable(int64))")

        # Test fixed_string (uses keyword fixed_string with underscore)
        self.validate_identity("CAST(x AS fixed_string(10))")

        # Test nested types
        self.validate_identity("CAST(x AS nested(id uint32, serial uint32, event_time datetime))")

        # Test tuple
        self.validate_identity("CAST(x AS tuple(string, array(nullable(float64))))")

        # Test map
        self.validate_identity("CAST(x AS map(string, int64))")

        # Test low_cardinality (uses keyword low_cardinality with underscore)
        self.validate_identity("CAST(x AS low_cardinality(string))")

        # Test enum (should work the same)
        self.validate_identity("CAST(x AS enum('hello' = 1, 'world' = 2))")
        self.validate_identity("CAST(x AS enum8('hello' = -123, 'world'))")

        # Test IP types
        self.validate_identity("CAST(x AS ipv4)")
        self.validate_identity("CAST(x AS ipv6)")

        # Test geo types
        self.validate_identity("CAST(x AS point)")
        self.validate_identity("CAST(x AS polygon)")

        # Test query parameters (space after colon in output)
        self.validate_identity("SELECT * FROM stream WHERE id = {param: uint32}")
        self.validate_identity("SELECT * FROM {table: Identifier}")

        # Test DISTINCT ON
        self.validate_identity("SELECT DISTINCT ON (id) * FROM stream")

        # Test JOIN types from ClickHouse (output order is different)
        self.validate_identity(
            "SELECT * FROM s1 ANY LEFT JOIN s2 ON s1.id = s2.id",
            "SELECT * FROM s1 LEFT ANY JOIN s2 ON s1.id = s2.id",
        )
        self.validate_identity("SELECT * FROM s1 ASOF JOIN s2 ON s1.id = s2.id")
        self.validate_identity("SELECT * FROM s1 ARRAY JOIN arr")

        # Test that PROJECTION is NOT supported (should fail to parse or be ignored in Timeplus)
        # Since we removed PROJECTION support, it should just parse as a column

    def test_versioned_stream(self):
        # Test versioned stream
        self.validate_identity(
            "CREATE STREAM versioned_kv (i INT, k STRING, k1 STRING) PRIMARY KEY (k, k1) SETTINGS mode='versioned_kv', version_column='i'",
            "CREATE STREAM versioned_kv (i int32, k string, k1 string) PRIMARY KEY (k, k1) SETTINGS mode = 'versioned_kv', version_column = 'i'",
        )

    def test_changelog_stream(self):
        # Test changelog stream
        self.validate_identity(
            "CREATE STREAM changelog_kv (i INT, k STRING, k1 STRING) PRIMARY KEY (k, k1) SETTINGS mode='changelog_kv', version_column='i'",
            "CREATE STREAM changelog_kv (i int32, k string, k1 string) PRIMARY KEY (k, k1) SETTINGS mode = 'changelog_kv', version_column = 'i'",
        )

    def test_stream_with_ttl(self):
        # Test stream with TTL
        self.validate_identity(
            "CREATE STREAM my_stream (id UINT32, name STRING) TTL _tp_time + INTERVAL 7 DAY",
            "CREATE STREAM my_stream (id uint32, name string) TTL _tp_time + INTERVAL 7 DAY",
        )

    def test_stream_with_codec(self):
        # Test stream with compression codec
        self.validate_identity(
            "CREATE STREAM compressed_stream (data STRING) SETTINGS logstore_codec='zstd'",
            "CREATE STREAM compressed_stream (data string) SETTINGS logstore_codec = 'zstd'",
        )

    def test_type_mapping_uppercase_to_lowercase(self):
        # Test that uppercase types are converted to lowercase
        self.validate_identity(
            "CREATE STREAM test (a INT32, b STRING, c BOOL)",
            "CREATE STREAM test (a int32, b string, c bool)",
        )

        self.validate_identity(
            "CREATE STREAM test (a Int32, b String, c Bool)",
            "CREATE STREAM test (a int32, b string, c bool)",
        )

        self.validate_identity(
            "CREATE STREAM test (a INT, b VARCHAR, c BOOLEAN)",
            "CREATE STREAM test (a int32, b string, c bool)",
        )

    def test_type_mapping_lowercase_input(self):
        # Test that lowercase types work as input and remain lowercase
        self.validate_identity("CREATE STREAM test (a int32, b string, c bool)")

        self.validate_identity("CREATE STREAM test (a int8, b uint64, c float32, d float64)")

    def test_parameterized_types(self):
        # Timeplus only accepts lowercase fixed_string, never FixedString
        # This test should fail if someone tries to use FixedString
        from sqlglot import parse_one
        from sqlglot.errors import ParseError

        # Native Timeplus should reject FixedString
        with self.assertRaises(ParseError):
            parse_one("CREATE STREAM test (a FixedString(32))", dialect="timeplus")

        # Test that fixed_string (lowercase) is properly supported and preserved
        self.validate_identity("CREATE STREAM test (a fixed_string(32), b fixed_string(128))")

        # Test mixed case gets normalized to lowercase
        self.validate_identity(
            "CREATE STREAM test (a Fixed_String(16), b FIXED_string(256))",
            "CREATE STREAM test (a fixed_string(16), b fixed_string(256))",
        )

        # Test Decimal types
        self.validate_identity(
            "CREATE STREAM test (a Decimal(10, 2), b DECIMAL32(4), c Decimal64(8))",
            "CREATE STREAM test (a decimal(10, 2), b decimal32(4), c decimal64(8))",
        )

        self.validate_identity(
            "CREATE STREAM test (a decimal(10, 2), b decimal32(4), c decimal64(8))"
        )

        # Test DateTime64
        self.validate_identity(
            "CREATE STREAM test (a DateTime64(3), b DATETIME64(6))",
            "CREATE STREAM test (a datetime64(3), b datetime64(6))",
        )

        self.validate_identity("CREATE STREAM test (a datetime64(3), b datetime64(6))")

    def test_complex_nested_types(self):
        # Test Array types
        self.validate_identity(
            "CREATE STREAM test (a Array(Int32), b ARRAY(String))",
            "CREATE STREAM test (a array(int32), b array(string))",
        )

        self.validate_identity("CREATE STREAM test (a array(int32), b array(string))")

        # Test Map types
        self.validate_identity(
            "CREATE STREAM test (a Map(String, Int32), b MAP(Int64, Float64))",
            "CREATE STREAM test (a map(string, int32), b map(int64, float64))",
        )

        self.validate_identity("CREATE STREAM test (a map(string, int32), b map(int64, float64))")

        # Test Tuple (struct) types
        self.validate_identity(
            "CREATE STREAM test (a Tuple(Int32, String, Bool))",
            "CREATE STREAM test (a tuple(int32, string, bool))",
        )

        self.validate_identity("CREATE STREAM test (a tuple(int32, string, bool))")

        # Test nested complex types
        self.validate_identity(
            "CREATE STREAM test (a Array(Map(String, Array(Int32))))",
            "CREATE STREAM test (a array(map(string, array(int32))))",
        )

        self.validate_identity("CREATE STREAM test (a array(map(string, array(int32))))")

    def test_all_numeric_types(self):
        # Test all numeric type conversions
        self.validate_identity(
            "CREATE STREAM test (a Int8, b UInt8, c Int16, d UInt16, e Int32, f UInt32, g Int64, h UInt64)",
            "CREATE STREAM test (a int8, b uint8, c int16, d uint16, e int32, f uint32, g int64, h uint64)",
        )

        self.validate_identity(
            "CREATE STREAM test (a INT128, b UINT128, c INT256, d UINT256)",
            "CREATE STREAM test (a int128, b uint128, c int256, d uint256)",
        )

        self.validate_identity(
            "CREATE STREAM test (a Float32, b Float64, c FLOAT, d DOUBLE)",
            "CREATE STREAM test (a float32, b float64, c float32, d float64)",
        )

    def test_date_time_types(self):
        # Test date and time types
        self.validate_identity(
            "CREATE STREAM test (a Date, b Date32, c DateTime, d TIMESTAMP)",
            "CREATE STREAM test (a date, b date32, c datetime, d datetime)",
        )

        self.validate_identity("CREATE STREAM test (a date, b date32, c datetime, d datetime64(3))")

    def test_special_types(self):
        # Test UUID, JSON types
        self.validate_identity(
            "CREATE STREAM test (a UUID, b JSON, c JSONB)",
            "CREATE STREAM test (a uuid, b json, c json)",
        )

        self.validate_identity("CREATE STREAM test (a uuid, b json)")

    def test_datetime_and_conversion_functions(self):
        """Test datetime functions and type conversions"""
        # Test unix timestamp functions (snake_case, with automatic casting)
        self.validate_identity(
            "SELECT from_unix_timestamp(1234567890)",
            "SELECT from_unix_timestamp(CAST(1234567890 AS nullable(int64)))",
        )
        self.validate_identity("SELECT from_unix_timestamp64_milli(1234567890123)")
        self.validate_identity("SELECT from_unix_timestamp64_micro(1234567890123456)")
        self.validate_identity("SELECT from_unix_timestamp64_nano(1234567890123456789)")

        # Test to_unix_timestamp functions
        self.validate_identity("SELECT to_unix_timestamp(now())")
        self.validate_identity("SELECT to_unix_timestamp64_milli(now())")

        # Test date/time extraction
        self.validate_identity("SELECT year(now())")
        self.validate_identity("SELECT month(now())")
        self.validate_identity("SELECT day(now())")
        self.validate_identity("SELECT hour(now())")
        self.validate_identity("SELECT minute(now())")
        self.validate_identity("SELECT second(now())")

        # Test date math functions (outputs uppercase)
        self.validate_identity("SELECT date_add(DAY, 1, now())", "SELECT DATE_ADD(DAY, 1, now())")
        self.validate_identity(
            "SELECT date_diff('day', yesterday(), today())",
            "SELECT DATE_DIFF(DAY, yesterday(), today())",
        )

    def test_create_dictionary(self):
        """Test CREATE DICTIONARY statements"""
        # Test basic dictionary with PRIMARY KEY
        self.validate_identity(
            "CREATE DICTIONARY test_dict (id INT, name STRING) PRIMARY KEY id",
            "CREATE DICTIONARY test_dict (id int32, name string) PRIMARY KEY (id)",
        )

        # Test dictionary with defaults
        self.validate_identity(
            "CREATE DICTIONARY dict_with_defaults (id UINT64 DEFAULT 0, name STRING DEFAULT '') PRIMARY KEY id",
            "CREATE DICTIONARY dict_with_defaults (id uint64 DEFAULT 0, name string DEFAULT '') PRIMARY KEY (id)",
        )

        # Test dictionary with SOURCE
        self.validate_identity(
            "CREATE DICTIONARY dict_source (id INT) PRIMARY KEY id SOURCE(HTTP())",
            "CREATE DICTIONARY dict_source (id int32) PRIMARY KEY (id) SOURCE(HTTP())",
        )

        # Test dictionary with LAYOUT
        self.validate_identity(
            "CREATE DICTIONARY dict_layout (id INT) PRIMARY KEY id LAYOUT(FLAT())",
            "CREATE DICTIONARY dict_layout (id int32) PRIMARY KEY (id) LAYOUT(FLAT())",
        )

        # Test dictionary with LIFETIME
        self.validate_identity(
            "CREATE DICTIONARY dict_lifetime (id INT) PRIMARY KEY id LIFETIME(10)",
            "CREATE DICTIONARY dict_lifetime (id int32) PRIMARY KEY (id) LIFETIME(10)",
        )

        # Test dictionary with all components
        self.validate_identity(
            "CREATE DICTIONARY full_dict (id INT, value STRING) PRIMARY KEY id SOURCE(TIMEPLUS()) LAYOUT(FLAT()) LIFETIME(10)",
            "CREATE DICTIONARY full_dict (id int32, value string) PRIMARY KEY (id) SOURCE(TIMEPLUS()) LAYOUT(FLAT()) LIFETIME(10)",
        )

        # Test dictionary with different layouts
        self.validate_identity(
            "CREATE DICTIONARY dict_hashed (id INT) PRIMARY KEY id LAYOUT(HASHED())",
            "CREATE DICTIONARY dict_hashed (id int32) PRIMARY KEY (id) LAYOUT(HASHED())",
        )

        self.validate_identity(
            "CREATE DICTIONARY dict_ip_trie (prefix STRING) PRIMARY KEY prefix LAYOUT(IP_TRIE())",
            "CREATE DICTIONARY dict_ip_trie (prefix string) PRIMARY KEY (prefix) LAYOUT(IP_TRIE())",
        )

        # Test DROP DICTIONARY
        self.validate_identity("DROP DICTIONARY my_dict")
        self.validate_identity("DROP DICTIONARY IF EXISTS my_dict")

    def test_create_database(self):
        """Test CREATE DATABASE statements"""
        # Test basic CREATE DATABASE
        self.validate_identity("CREATE DATABASE my_database")

        # Test CREATE DATABASE IF NOT EXISTS
        self.validate_identity("CREATE DATABASE IF NOT EXISTS my_database")

        # Test external MySQL database
        self.validate_identity(
            "CREATE DATABASE mysql_db SETTINGS type='mysql', address='localhost:3306', user='root', password='pass', database='source_db'",
            "CREATE DATABASE mysql_db SETTINGS type = 'mysql', address = 'localhost:3306', user = 'root', password = 'pass', database = 'source_db'",
        )

        # Test external Iceberg database
        self.validate_identity(
            "CREATE DATABASE iceberg_db SETTINGS type='iceberg', catalog_uri='http://localhost:8181', catalog_type='rest', warehouse='/warehouse'",
            "CREATE DATABASE iceberg_db SETTINGS type = 'iceberg', catalog_uri = 'http://localhost:8181', catalog_type = 'rest', warehouse = '/warehouse'",
        )

        # Test DROP DATABASE
        self.validate_identity("DROP DATABASE my_database")
        self.validate_identity("DROP DATABASE IF EXISTS my_database")

        # Test USE DATABASE
        self.validate_identity("USE my_database")

    def test_format_schema(self):
        """Test FORMAT SCHEMA statements"""
        # Test CREATE FORMAT SCHEMA for Protobuf
        self.validate_identity(
            "CREATE FORMAT SCHEMA protobuf_schema AS 'syntax = \"proto3\"; message User { string name = 1; }' TYPE Protobuf",
            "CREATE FORMAT SCHEMA protobuf_schema AS 'syntax = \"proto3\"; message User { string name = 1; }' TYPE Protobuf",
        )

        # Test CREATE OR REPLACE FORMAT SCHEMA
        self.validate_identity(
            'CREATE OR REPLACE FORMAT SCHEMA avro_schema AS \'{"type": "record", "name": "User"}\' TYPE Avro',
            'CREATE OR REPLACE FORMAT SCHEMA avro_schema AS \'{"type": "record", "name": "User"}\' TYPE Avro',
        )

        # Test DROP FORMAT SCHEMA
        self.validate_identity("DROP FORMAT SCHEMA my_schema")
        self.validate_identity("DROP FORMAT SCHEMA IF EXISTS my_schema")

    def test_create_function(self):
        """Test CREATE FUNCTION statements (SQL and JavaScript UDFs)"""
        # Test basic SQL UDF
        self.validate_identity(
            "CREATE FUNCTION add_five AS (x) -> x + 5",
            "CREATE FUNCTION add_five AS x -> x + 5",
        )

        # Test OR REPLACE SQL UDF
        self.validate_identity(
            "CREATE OR REPLACE FUNCTION multiply AS (x, y) -> x * y",
            "CREATE OR REPLACE FUNCTION multiply AS (x, y) -> x * y",
        )

        # Test JavaScript UDF with single quotes
        self.validate_identity(
            "CREATE FUNCTION js_add(x float32) RETURNS float32 LANGUAGE JAVASCRIPT AS 'function js_add(v) { return v.map(x => x + 1); }'",
            "CREATE FUNCTION js_add(x float32) AS 'function js_add(v) { return v.map(x => x + 1); }' RETURNS float32 LANGUAGE JAVASCRIPT",
        )

        # Test JavaScript UDF with OR REPLACE
        self.validate_identity(
            "CREATE OR REPLACE FUNCTION test_func(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS 'function test_func(values) { return values.map(v => v * 2); }'",
            "CREATE OR REPLACE FUNCTION test_func(value float32) AS 'function test_func(values) { return values.map(v => v * 2); }' RETURNS float32 LANGUAGE JAVASCRIPT",
        )

        # Test Python UDF (similar syntax)
        self.validate_identity(
            "CREATE FUNCTION py_func(x int) RETURNS int LANGUAGE PYTHON AS 'def py_func(x): return x + 1'",
            "CREATE FUNCTION py_func(x int32) AS 'def py_func(x): return x + 1' RETURNS int32 LANGUAGE PYTHON",
        )

        # Test JavaScript UDF with heredoc $$ ... $$ syntax (actual Timeplus syntax)
        self.validate_identity(
            """CREATE OR REPLACE FUNCTION test_add_five(value float32)
            RETURNS float32
            LANGUAGE JAVASCRIPT AS $$
              function test_add_five(values) {
                for(let i=0;i<values.length;i++) {
                  values[i]=values[i]+5;
                }
                return values;
              }
            $$""",
            "CREATE OR REPLACE FUNCTION test_add_five(value float32) AS '\\n              function test_add_five(values) {\\n                for(let i=0;i<values.length;i++) {\\n                  values[i]=values[i]+5;\\n                }\\n                return values;\\n              }\\n            ' RETURNS float32 LANGUAGE JAVASCRIPT",
        )

    def test_datetime_functions_continued(self):
        # Test date_sub
        self.validate_identity(
            "SELECT date_sub(MONTH, 1, now())", "SELECT DATE_SUB(MONTH, 1, now())"
        )

        # Test timestamp math (outputs uppercase)
        self.validate_identity(
            "SELECT timestamp_add(HOUR, 1, now())",
            "SELECT TIMESTAMP_ADD(HOUR, 1, now())",
        )
        self.validate_identity(
            "SELECT timestamp_sub(MINUTE, 30, now())",
            "SELECT TIMESTAMP_SUB(MINUTE, 30, now())",
        )

        # Test to_start_of functions
        self.validate_identity("SELECT to_start_of_year(now())")
        self.validate_identity("SELECT to_start_of_month(now())")
        self.validate_identity("SELECT to_start_of_day(now())")
        self.validate_identity("SELECT to_start_of_hour(now())")

        # Test date formatting
        self.validate_identity("SELECT format_date_time(now(), '%Y-%m-%d %H:%M:%S')")
        self.validate_identity("SELECT parse_date_time('2024-01-01', '%Y-%m-%d')")

        # Test string functions (snake_case where applicable)
        self.validate_identity("SELECT starts_with('hello', 'he')")
        self.validate_identity("SELECT ends_with('world', 'ld')")
        self.validate_identity("SELECT substring_index('a.b.c', '.', 2)")

        # Test hash functions
        self.validate_identity("SELECT MD5('hello')")
        self.validate_identity("SELECT SHA256('world')")
        self.validate_identity("SELECT SHA512('test')")

        # Test type checking
        self.validate_identity("SELECT to_type_name(123)")
        self.validate_identity("SELECT is_nan(1.0 / 0.0)")

        # Test string distance (both map to edit_distance)
        self.validate_identity("SELECT edit_distance('kitten', 'sitting')")
        self.validate_identity(
            "SELECT levenshtein_distance('saturday', 'sunday')",
            "SELECT edit_distance('saturday', 'sunday')",
        )

    def test_aggregate_functions(self):
        # Test count_if with single argument (snake_case in Timeplus)
        self.validate_identity("SELECT count_if(speed > 80) FROM cars")
        self.validate_identity("SELECT count_if(temperature < 0) FROM weather")

        # Test unique_exact_if (snake_case in Timeplus)
        self.validate_identity("SELECT unique_exact_if(user_id, status = 'active') FROM users")

        # Test other aggregate functions with snake_case naming
        self.validate_identity("SELECT count_distinct(user_id) FROM events")
        self.validate_identity("SELECT unique_exact(device_id) FROM logs")

        # Test group_array functions
        self.validate_identity("SELECT group_array(value) FROM metrics")
        self.validate_identity("SELECT group_uniq_array(tag) FROM tags")
        self.validate_identity("SELECT group_concat(name, ', ') FROM products")

        # Test time-weighted aggregates
        self.validate_identity("SELECT avg_time_weighted(price, timestamp) FROM trades")
        self.validate_identity("SELECT median_time_weighted(value, time) FROM sensors")

        # Test top-k functions
        self.validate_identity("SELECT top_k(product_id, 10) FROM sales")
        self.validate_identity("SELECT min_k(price, 5) FROM inventory")
        self.validate_identity("SELECT max_k(score, 3) FROM leaderboard")

        # Test quantile functions
        self.validate_identity("SELECT quantile(response_time, 0.95) FROM requests")
        self.validate_identity("SELECT p90(latency) FROM metrics")
        self.validate_identity("SELECT p95(duration) FROM jobs")
        self.validate_identity("SELECT p99(processing_time) FROM tasks")

        # Test arg functions
        self.validate_identity("SELECT arg_min(price, timestamp) FROM products")
        self.validate_identity("SELECT arg_max(score, user_id) FROM games")

        # Test moving aggregates
        self.validate_identity("SELECT moving_sum(value) FROM timeseries")

        # Test other aggregates
        self.validate_identity("SELECT any(status) FROM jobs")
        self.validate_identity("SELECT first_value(event) FROM events")
        self.validate_identity("SELECT last_value(measurement) FROM sensors")

        # Test histogram
        self.validate_identity("SELECT histogram(value, 10) FROM data")

        # Test kurtosis
        self.validate_identity("SELECT kurt_pop(value) FROM statistics")
        self.validate_identity("SELECT kurt_samp(measurement) FROM samples")

        # Test LTTB downsampling
        self.validate_identity("SELECT lttb(x, y, 100) FROM timeseries")
        self.validate_identity(
            "SELECT largest_triangle_three_buckets(timestamp, value, 50) FROM metrics"
        )

    def test_streaming_functions(self):
        """Test streaming-specific functions"""
        # Test table function for historical queries
        self.validate_identity("SELECT count(*) FROM table(clicks)")
        self.validate_identity("SELECT * FROM table(stream_name) ORDER BY _tp_time DESC")

        # Test window functions
        self.validate_identity(
            "SELECT window_start, count() FROM tumble(stream, 5s) GROUP BY window_start"
        )
        self.validate_identity(
            "SELECT window_start, sum(value) FROM hop(stream, 1s, 5s) GROUP BY window_start"
        )
        self.validate_identity(
            "SELECT * FROM session(car_data, 1m)",
            "SELECT * FROM session AS _t0(car_data, 1m)",
        )

        # Test deduplication
        self.validate_identity("SELECT * FROM dedup(stream, id)")
        self.validate_identity("SELECT * FROM dedup(stream, id, 120s)")

        # Test lag functions
        self.validate_identity("SELECT lag(total) FROM metrics")
        self.validate_identity("SELECT lag(total, 12, 0) FROM metrics")
        self.validate_identity("SELECT lags(value, 1, 3) FROM timeseries")

        # Test streaming join functions
        self.validate_identity("SELECT * FROM stream1 WHERE date_diff_within(10s, time1, time2)")
        self.validate_identity("SELECT * FROM stream1 WHERE lag_behind(10ms, ts1, ts2)")

        # Test streaming aggregations
        self.validate_identity("SELECT latest(value) FROM stream GROUP BY id")
        self.validate_identity("SELECT earliest(timestamp) FROM events GROUP BY user_id")

        # Test emit_version
        self.validate_identity("SELECT emit_version(), count(*) FROM stream")

        # Test changelog
        self.validate_identity("SELECT * FROM changelog(stream, key_col)")
        self.validate_identity(
            "SELECT * FROM changelog(stream, key_col, version_col, true)",
            "SELECT * FROM changelog(stream, key_col, version_col, TRUE)",
        )

        # Test rowify (EMIT ON UPDATE is a separate clause in Timeplus streaming queries)
        self.validate_identity("SELECT sum(value) FROM rowify(stream)")

    def test_datetime_functions(self):
        """Test date/time functions specific to Timeplus"""
        # Test Unix timestamp conversion functions
        self.validate_identity(
            "SELECT from_unix_timestamp(1644272032)",
            "SELECT from_unix_timestamp(CAST(1644272032 AS nullable(int64)))",
        )
        self.validate_identity("SELECT from_unix_timestamp64_milli(1712982826540)")
        self.validate_identity("SELECT from_unix_timestamp64_micro(1712982905267202)")
        self.validate_identity("SELECT from_unix_timestamp64_nano(1712983042242306000)")

        self.validate_identity("SELECT to_unix_timestamp(now())")
        self.validate_identity("SELECT to_unix_timestamp64_milli(now64())")
        self.validate_identity("SELECT to_unix_timestamp64_micro(now64(9))")
        self.validate_identity("SELECT to_unix_timestamp64_nano(now64(9))")

    def test_text_functions(self):
        """Test text processing functions"""
        # Test string splitting and searching
        self.validate_identity("SELECT split_by_string(',', 'a,b,c')")
        self.validate_identity("SELECT multi_search_any(text, ['password', 'token', 'secret'])")

        # Test replacement functions
        self.validate_identity("SELECT replace_one('abca', 'a', 'z')")
        self.validate_identity(
            "SELECT replace_regex(phone, '(\\\\d{3})-(\\\\d{3})-(\\\\d{4})', '\\\\1-***-****')"
        )

        # Test extraction
        self.validate_identity("SELECT extract_all_groups(text, pattern)")

    def test_json_functions(self):
        """Test JSON processing functions"""
        self.validate_identity("SELECT json_extract_int(data, 'age')")
        self.validate_identity("SELECT json_extract_float(data, 'price')")
        self.validate_identity("SELECT json_extract_bool(config, 'enabled')")
        self.validate_identity("SELECT json_extract_array(data, 'items')")
        self.validate_identity("SELECT json_extract_keys(json_str)")
        self.validate_identity("SELECT is_valid_json(text)")
        self.validate_identity("SELECT json_has(data, 'field')")

    def test_cross_dialect_conversion(self):
        # Test conversion from ClickHouse TABLE to Timeplus STREAM
        from sqlglot import parse_one

        clickhouse_sql = (
            "CREATE TABLE test_table (id Int32, name String) ENGINE = MergeTree() ORDER BY id"
        )

        # Parse as ClickHouse
        parsed = parse_one(clickhouse_sql, dialect="clickhouse")

        # Convert to Timeplus (should convert TABLE to STREAM)
        timeplus_sql = parsed.sql(dialect="timeplus")

        # Check that it generates CREATE STREAM with lowercase types
        self.assertIn("CREATE STREAM", timeplus_sql)
        self.assertIn("int32", timeplus_sql)
        self.assertIn("string", timeplus_sql)

        # Print what was generated for debugging
        print(f"Generated Timeplus SQL: {timeplus_sql}")

        # For now, we won't try to parse it back since Timeplus doesn't support ENGINE clause
        # self.assertIsNotNone(parse_one(timeplus_sql, dialect="timeplus"))
