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

    def test_analyze(self):
        self.validate_identity("ANALYZE tbl")
        self.validate_identity("ANALYZE tbl WITH (prop1=val1, prop2=val2)")
