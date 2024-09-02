from tests.dialects.test_dialect import Validator


class TestPRQL(Validator):
    dialect = "prql"

    def test_prql(self):
        self.validate_all(
            "from x",
            read={
                None: "SELECT * FROM x",
            },
            write={
                None: "SELECT * FROM x",
            },
        )
        self.validate_all(
            "from x derive a + 1",
            write={
                None: "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1}",
            write={
                None: "SELECT *, a + 1 AS x FROM x",
            },
        )
        self.validate_all(
            # "Parentheses are not required for expressions that do not contain function calls, for example: foo + bar."
            # https://prql-lang.org/book/reference/syntax/operators.html?highlight=parenth#parentheses
            "from x derive a + 1",
            write={
                None: "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b}",
            write={
                None: "SELECT *, a + 1 AS x, b FROM x",
            },
        )
        self.validate_all(
            # TODO: Fix this test: `derive` should add columns.
            # https://prql-lang.org/book/reference/stdlib/transforms/derive.html
            "from x select {y = a + 1, 2}",
            write={
                None: "SELECT a + 1 AS y, 2 FROM x",
            },
        )
        self.validate_all(
            "from x take 10",
            read={
                None: "SELECT * FROM x LIMIT 10",
            },
            write={
                None: "SELECT * FROM x LIMIT 10",
            },
        )
        self.validate_all(
            "from x take 5",
            read={
                None: "SELECT * FROM x LIMIT 5",
                "prql": "from x take 10 take 5",
            },
            write={
                None: "SELECT * FROM x LIMIT 5",
            },
        )
        self.validate_all(
            "from x filter age > 25",
            read={
                None: "SELECT * FROM x WHERE age > 25",
            },
            write={
                None: "SELECT * FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x filter age > 25 derive {x = a + 1, b}",
            write={
                None: "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x filter dept != 'IT'",
            write={
                None: "SELECT * FROM x WHERE dept <> 'IT'",
            },
        )
        self.validate_all(
            "from x filter p == 'product' select {a, b}",
            write={
                None: "SELECT a, b FROM x WHERE p = 'product'",
            },
        )
        self.validate_all(
            "from x filter age > 25 && age < 27",
            write={
                None: "SELECT * FROM x WHERE age > 25 AND age < 27",
            },
        )
        self.validate_all(
            "from x filter (age > 25 && age < 27)",
            write={
                None: "SELECT * FROM x WHERE (age > 25 AND age < 27)",
            },
        )
        self.validate_all(
            "from x filter (age > 25 || age < 27)",
            write={
                None: "SELECT * FROM x WHERE (age > 25 OR age < 27)",
            },
        )
        self.validate_all(
            "from x filter ((age > 25 || age < 22) && age > 26) && age < 27",
            write={
                None: "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
            },
        )
        self.validate_all(
            "from x sort age",
            write={
                None: "SELECT * FROM x ORDER BY age",
            },
        )
        self.validate_all(
            # "from x sort {-age}", # TODO: Fix test to be this.
            "from x sort -age",
            write={
                None: "SELECT * FROM x ORDER BY age DESC",
            },
        )
        self.validate_all(
            "from x sort {age, name}",
            write={
                None: "SELECT * FROM x ORDER BY age, name",
            },
        )
        self.validate_all(
            "from x sort {-age, name}",
            write={
                None: "SELECT * FROM x ORDER BY age DESC, name",
            },
        )
        self.validate_all(
            "from x append y",
            write={
                None: "SELECT * FROM x UNION ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x remove y",
            write={
                None: "SELECT * FROM x EXCEPT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x intersect y",
            write={
                None: "SELECT * FROM x INTERSECT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x filter a == null && b != null",
            write={
                None: "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
            },
        )
        self.validate_all(
            "from x filter (a > 1 || b != null || c != null)",
            write={
                None: "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
            },
        )
        self.validate_all(
            "from a aggregate {average x}",
            write={
                None: "SELECT AVG(x) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {average x, min y, ct = sum z}",
            write={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {average x, min y, sum z}",
            write={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {min y, b = stddev x, max z}",
            write={
                None: "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
            },
        )
        self.validate_all(
            "from x sort age",
            write={
                None: "SELECT * FROM x ORDER BY age",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html
            "from a append b",
            write={
                None: "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html#remove
            "from a remove b",
            write={
                None: "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html#intersection
            "from a intersect b",
            write={
                None: "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/group.html?#group
            "from x sort a take 1",
            write={
                None: "SELECT * FROM x ORDER BY a LIMIT 1",
            },
        )
