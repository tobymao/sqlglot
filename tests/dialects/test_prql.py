from tests.dialects.test_dialect import Validator


class TestPRQL(Validator):
    dialect = "prql"

    def test_prql(self):
        self.validate_identity("from x", "SELECT * FROM x")
        self.validate_identity("from x derive a + 1", "SELECT *, a + 1 FROM x")
        self.validate_identity("from x derive x = a + 1", "SELECT *, a + 1 AS x FROM x")
        self.validate_identity("from x derive {a + 1}", "SELECT *, a + 1 FROM x")
        self.validate_identity("from x derive {x = a + 1, b}", "SELECT *, a + 1 AS x, b FROM x")
        self.validate_identity(
            "from x derive {x = a + 1, b} select {y = x, 2}", "SELECT a + 1 AS y, 2 FROM x"
        )
        self.validate_identity("from x take 10", "SELECT * FROM x LIMIT 10")
        self.validate_identity("from x take 10 take 5", "SELECT * FROM x LIMIT 5")
        self.validate_identity("from x filter age > 25", "SELECT * FROM x WHERE age > 25")
        self.validate_identity(
            "from x derive {x = a + 1, b} filter age > 25",
            "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
        )
        self.validate_identity("from x filter dept != 'IT'", "SELECT * FROM x WHERE dept <> 'IT'")
        self.validate_identity(
            "from x filter p == 'product' select { a, b }", "SELECT a, b FROM x WHERE p = 'product'"
        )
        self.validate_identity(
            "from x filter age > 25 filter age < 27", "SELECT * FROM x WHERE age > 25 AND age < 27"
        )
        self.validate_identity(
            "from x filter (age > 25 && age < 27)", "SELECT * FROM x WHERE (age > 25 AND age < 27)"
        )
        self.validate_identity(
            "from x filter (age > 25 || age < 27)", "SELECT * FROM x WHERE (age > 25 OR age < 27)"
        )
        self.validate_identity(
            "from x filter (age > 25 || age < 22) filter age > 26 filter age < 27",
            "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
        )
        self.validate_identity(
            "from x sort age",
            "SELECT * FROM x ORDER BY age",
        )
        self.validate_identity(
            "from x sort {-age}",
            "SELECT * FROM x ORDER BY age DESC",
        )
        self.validate_identity(
            "from x sort {age, name}",
            "SELECT * FROM x ORDER BY age, name",
        )
        self.validate_identity(
            "from x sort {-age, +name}",
            "SELECT * FROM x ORDER BY age DESC, name",
        )
        self.validate_identity("from x append y", "SELECT * FROM x UNION ALL SELECT * FROM y")
        self.validate_identity("from x remove y", "SELECT * FROM x EXCEPT ALL SELECT * FROM y")
        self.validate_identity(
            "from x intersect y", "SELECT * FROM x INTERSECT ALL SELECT * FROM y"
        )
        self.validate_identity(
            "from x filter a == null filter null != b",
            "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
        )
        self.validate_identity(
            "from x filter (a > 1 || null != b || c != null)",
            "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
        )
        self.validate_identity("from a aggregate { average x }", "SELECT AVG(x) FROM a")
        self.validate_identity(
            "from a aggregate { average x, min y, ct = sum z }",
            "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
        )
        self.validate_identity(
            "from a aggregate { average x, min y, sum z }",
            "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
        )
        self.validate_identity(
            "from a aggregate { min y, b = stddev x, max z }",
            "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
        )
