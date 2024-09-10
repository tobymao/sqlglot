from tests.dialects.test_dialect import Validator


class TestPRQL(Validator):
    dialect = "prql"

    def test_prql(self):
        self.validate_all(
            "from x",
            write={
                "": "SELECT * FROM x",
            },
        )
        self.validate_all(
            "from x derive a + 1",
            write={
                "": "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive x = a + 1",
            write={
                "": "SELECT *, a + 1 AS x FROM x",
            },
        )
        self.validate_all(
            "from x derive {a + 1}",
            write={
                "": "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b}",
            write={
                "": "SELECT *, a + 1 AS x, b FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b} select {y = x, 2}",
            write={"": "SELECT a + 1 AS y, 2 FROM x"},
        )
        self.validate_all(
            "from x take 10",
            write={
                "": "SELECT * FROM x LIMIT 10",
            },
        )
        self.validate_all(
            "from x take 10 take 5",
            write={
                "": "SELECT * FROM x LIMIT 5",
            },
        )
        self.validate_all(
            "from x filter age > 25",
            write={
                "": "SELECT * FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b} filter age > 25",
            write={
                "": "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x filter dept != 'IT'",
            write={
                "": "SELECT * FROM x WHERE dept <> 'IT'",
            },
        )
        self.validate_all(
            "from x filter p == 'product' select { a, b }",
            write={"": "SELECT a, b FROM x WHERE p = 'product'"},
        )
        self.validate_all(
            "from x filter age > 25 filter age < 27",
            write={"": "SELECT * FROM x WHERE age > 25 AND age < 27"},
        )
        self.validate_all(
            "from x filter (age > 25 && age < 27)",
            write={"": "SELECT * FROM x WHERE (age > 25 AND age < 27)"},
        )
        self.validate_all(
            "from x filter (age > 25 || age < 27)",
            write={"": "SELECT * FROM x WHERE (age > 25 OR age < 27)"},
        )
        self.validate_all(
            "from x filter (age > 25 || age < 22) filter age > 26 filter age < 27",
            write={
                "": "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
            },
        )
        self.validate_all(
            "from x sort age",
            write={
                "": "SELECT * FROM x ORDER BY age",
            },
        )
        self.validate_all(
            "from x sort {-age}",
            write={
                "": "SELECT * FROM x ORDER BY age DESC",
            },
        )
        self.validate_all(
            "from x sort {age, name}",
            write={
                "": "SELECT * FROM x ORDER BY age, name",
            },
        )
        self.validate_all(
            "from x sort {-age, +name}",
            write={
                "": "SELECT * FROM x ORDER BY age DESC, name",
            },
        )
        self.validate_all(
            "from x append y",
            write={
                "": "SELECT * FROM x UNION ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x remove y",
            write={
                "": "SELECT * FROM x EXCEPT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x intersect y",
            write={"": "SELECT * FROM x INTERSECT ALL SELECT * FROM y"},
        )
        self.validate_all(
            "from x filter a == null filter null != b",
            write={
                "": "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
            },
        )
        self.validate_all(
            "from x filter (a > 1 || null != b || c != null)",
            write={
                "": "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
            },
        )
        self.validate_all(
            "from a aggregate { average x }",
            write={
                "": "SELECT AVG(x) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { average x, min y, ct = sum z }",
            write={
                "": "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { average x, min y, sum z }",
            write={
                "": "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate { min y, b = stddev x, max z }",
            write={
                "": "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
            },
        )
