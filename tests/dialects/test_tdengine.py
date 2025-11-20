from sqlglot import transpile
from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestTDengine(Validator):
    dialect = "tdengine"

    def test_timetruncate(self):
        self.validate_identity("SELECT TIMETRUNCATE(col, 1s, 0) FROM t")
        self.validate_identity("SELECT TIMETRUNCATE(col, 1d) FROM t")
        self.validate_identity(
            "SELECT TIMETRUNCATE(ts, 1d) AS ts, AVG(current) AS `AVG(current)` FROM t"
        )

    def test_count(self):
        self.validate_identity("SELECT COUNT(a) FROM x")
        self.validate_identity("SELECT COUNT(*) FROM x")

        with self.assertRaisesRegex(
            UnsupportedError, "TDengine does not support COUNT\(DISTINCT \.\.\.\)"
        ):
            transpile(
                "SELECT COUNT(DISTINCT a) FROM x",
                write="tdengine",
            )
