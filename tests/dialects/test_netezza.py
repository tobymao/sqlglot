from sqlglot import Dialect, Dialects
from tests.dialects.test_dialect import Validator


class TestNetezza(Validator):
    dialect = "netezza"

    def test_identity(self):
        self.validate_identity("SELECT CURRENT_DATE")
        self.validate_identity("SELECT CAST(a AS VARCHAR(255)) FROM x")
        self.validate_identity("SELECT * FROM t LIMIT 10")
        self.validate_identity("CREATE TABLE t (id INT) DISTRIBUTE ON RANDOM")
        self.validate_identity("CREATE TABLE t (id INT, name VARCHAR(10)) DISTRIBUTE ON (id)")
        self.validate_identity("CREATE TABLE t (id INT, name VARCHAR(10)) DISTRIBUTE ON HASH(id)")
        self.validate_identity("CREATE TABLE t (id INT, name VARCHAR(10)) ORGANIZE ON (id, name)")
        self.validate_identity("CREATE TABLE t (id INT) ORGANIZE ON NONE")
        self.validate_identity(
            "CREATE TABLE t (id INT, name VARCHAR(10)) DISTRIBUTE ON HASH(id) ORGANIZE ON (name)"
        )

    def test_registry(self):
        self.assertIsNotNone(Dialect.get_or_raise("netezza"))
        self.assertEqual(Dialects.NETEZZA.value, "netezza")
