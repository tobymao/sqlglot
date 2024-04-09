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
        self.validate_identity("FROM x TAKE 10", "SELECT * FROM x LIMIT 10")
        self.validate_identity("FROM x TAKE 10 TAKE 5", "SELECT * FROM x LIMIT 5")
        self.validate_identity("FROM x FILTER age > 25", "SELECT * FROM x WHERE age > 25")
        self.validate_identity(
            "FROM x DERIVE {x = a + 1, b} FILTER age > 25",
            "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
        )
        self.validate_identity("FROM x FILTER dept != 'IT'", "SELECT * FROM x WHERE dept <> 'IT'")
        self.validate_identity(
            "FROM x FILTER p == 'product' SELECT { a, b }", "SELECT a, b FROM x WHERE p = 'product'"
        )
        self.validate_identity(
            "FROM x FILTER age > 25 FILTER age < 27", "SELECT * FROM x WHERE age > 25 AND age < 27"
        )
        self.validate_identity("FROM x APPEND y", "SELECT * FROM x UNION ALL SELECT * FROM y")
