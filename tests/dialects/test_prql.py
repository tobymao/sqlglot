from tests.dialects.test_dialect import Validator


class TestPRQL(Validator):
    dialect = "prql"

    def test_prql(self):
        self.validate_identity("FROM x", "SELECT * FROM x")
        self.validate_identity("FROM x DERIVE a + 1", "SELECT *, a + 1 FROM x")
        self.validate_identity("FROM x DERIVE x = a + 1", "SELECT *, a + 1 AS x FROM x")
        self.validate_identity("FROM x DERIVE {a + 1}", "SELECT *, a + 1 FROM x")
        self.validate_identity("FROM x DERIVE {x = a + 1, b}", "SELECT *, a + 1 AS x, b FROM x")
        self.validate_identity(
            "FROM x DERIVE {x = a + 1, b} SELECT {y = x, 2}", "SELECT a + 1 AS y, 2 FROM x"
        )
