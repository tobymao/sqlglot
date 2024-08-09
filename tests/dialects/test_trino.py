from tests.dialects.test_dialect import Validator


class TestTrino(Validator):
    dialect = "trino"

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
