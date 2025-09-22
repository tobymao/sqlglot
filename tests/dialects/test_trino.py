from tests.dialects.test_dialect import Validator


class TestTrino(Validator):
    dialect = "trino"

    def test_trino(self):
        self.validate_identity("JSON_QUERY(m.properties, 'lax $.area' OMIT QUOTES NULL ON ERROR)")
        self.validate_identity("JSON_EXTRACT(content, json_path)")
        self.validate_identity("JSON_QUERY(content, 'lax $.HY.*')")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH ARRAY WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITH UNCONDITIONAL WRAPPER)")
        self.validate_identity("JSON_QUERY(content, 'strict $.HY.*' WITHOUT CONDITIONAL WRAPPER)")
        self.validate_identity("JSON_QUERY(description, 'strict $.comment' KEEP QUOTES)")
        self.validate_identity(
            "JSON_QUERY(description, 'strict $.comment' OMIT QUOTES ON SCALAR STRING)"
        )
        self.validate_identity(
            "JSON_QUERY(content, 'strict $.HY.*' WITH UNCONDITIONAL WRAPPER KEEP QUOTES)"
        )
        self.validate_identity(
            "SELECT TIMESTAMP '2012-10-31 01:00 -2'",
            "SELECT CAST('2012-10-31 01:00 -2' AS TIMESTAMP WITH TIME ZONE)",
        )
        self.validate_identity(
            "SELECT TIMESTAMP '2012-10-31 01:00 +2'",
            "SELECT CAST('2012-10-31 01:00 +2' AS TIMESTAMP WITH TIME ZONE)",
        )

        self.validate_all(
            "SELECT TIMESTAMP '2012-10-31 01:00:00 +02:00'",
            write={
                "duckdb": "SELECT CAST('2012-10-31 01:00:00 +02:00' AS TIMESTAMPTZ)",
                "trino": "SELECT CAST('2012-10-31 01:00:00 +02:00' AS TIMESTAMP WITH TIME ZONE)",
            },
        )
        self.validate_all(
            "SELECT FORMAT('%s', 123)",
            write={
                "duckdb": "SELECT FORMAT('{}', 123)",
                "snowflake": "SELECT TO_CHAR(123)",
                "trino": "SELECT FORMAT('%s', 123)",
            },
        )

    def test_listagg(self):
        self.validate_identity(
            "SELECT LISTAGG(DISTINCT col, ',') WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE '...' WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col, '; ' ON OVERFLOW TRUNCATE '...' WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl"
        )
        self.validate_identity(
            "SELECT LISTAGG(col) WITHIN GROUP (ORDER BY col DESC) FROM tbl",
            "SELECT LISTAGG(col, ',') WITHIN GROUP (ORDER BY col DESC) FROM tbl",
        )

    def test_trim(self):
        self.validate_identity("SELECT TRIM('!' FROM '!foo!')")
        self.validate_identity("SELECT TRIM(BOTH '$' FROM '$var$')")
        self.validate_identity("SELECT TRIM(TRAILING 'ER' FROM UPPER('worker'))")
        self.validate_identity(
            "SELECT TRIM(LEADING FROM '  abcd')",
            "SELECT LTRIM('  abcd')",
        )
        self.validate_identity(
            "SELECT TRIM('!foo!', '!')",
            "SELECT TRIM('!' FROM '!foo!')",
        )

    def test_ddl(self):
        self.validate_identity("ALTER TABLE users RENAME TO people")
        self.validate_identity("ALTER TABLE IF EXISTS users RENAME TO people")
        self.validate_identity("ALTER TABLE users ADD COLUMN zip VARCHAR")
        self.validate_identity("ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip VARCHAR")
        self.validate_identity("ALTER TABLE users DROP COLUMN zip")
        self.validate_identity("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip")
        self.validate_identity("ALTER TABLE users RENAME COLUMN id TO user_id")
        self.validate_identity("ALTER TABLE IF EXISTS users RENAME COLUMN IF EXISTS id TO user_id")
        self.validate_identity("ALTER TABLE users ALTER COLUMN id SET DATA TYPE BIGINT")
        self.validate_identity("ALTER TABLE users ALTER COLUMN id DROP NOT NULL")
        self.validate_identity(
            "ALTER TABLE people SET AUTHORIZATION alice", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET AUTHORIZATION ROLE PUBLIC", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES x = 'y'", check_command_warning=True
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES foo = 123, 'foo bar' = 456",
            check_command_warning=True,
        )
        self.validate_identity(
            "ALTER TABLE people SET PROPERTIES x = DEFAULT", check_command_warning=True
        )
        self.validate_identity("ALTER VIEW people RENAME TO users")
        self.validate_identity(
            "ALTER VIEW people SET AUTHORIZATION alice", check_command_warning=True
        )
        self.validate_identity("CREATE SCHEMA foo WITH (LOCATION='s3://bucket/foo')")
        self.validate_identity(
            "CREATE TABLE foo.bar WITH (LOCATION='s3://bucket/foo/bar') AS SELECT 1"
        )

        # Hive connector syntax (partitioned_by)
        self.validate_identity(
            "CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONED_BY=ARRAY['a', 'b'])"
        )
        self.validate_identity(
            'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONED_BY=ARRAY[\'a\', \'b\'])',
            identify=True,
        )

        # Iceberg connector syntax (partitioning, can contain Iceberg transform expressions)
        self.validate_identity(
            "CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONING=ARRAY['a', 'bucket(4, b)', 'month(c)'])",
        )
        self.validate_identity(
            'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONING=ARRAY[\'a\', \'bucket(4, b)\', \'month(c)\'])',
            identify=True,
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE tbl")
        self.validate_identity("ANALYZE tbl WITH (prop1=val1, prop2=val2)")

    def test_json_value(self):
        self.validate_identity(
            "JSON_VALUE(jl.extra_attributes, 'lax $.amount_source' RETURNING VARCHAR)"
        )

        json_doc = """'{"item": "shoes", "price": "49.95"}'"""
        self.validate_identity(f"""SELECT JSON_VALUE({json_doc}, 'strict $.price')""")
        self.validate_identity(
            f"""SELECT JSON_VALUE({json_doc}, 'lax $.price' RETURNING DECIMAL(4, 2))"""
        )

        for on_option in ("NULL", "ERROR", "DEFAULT 1"):
            self.validate_identity(
                f"""SELECT JSON_VALUE({json_doc}, 'lax $.price' RETURNING DECIMAL(4, 2) {on_option} ON EMPTY {on_option} ON ERROR) AS price"""
            )
