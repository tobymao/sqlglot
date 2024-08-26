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
            read={
                None: "SELECT *, a + 1 FROM x",
            },
            write={
                None: "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1}",
            read={
                None: "SELECT *, a + 1 AS x FROM x",
            },
            write={
                None: "SELECT *, a + 1 AS x FROM x",
            },
        )
        self.validate_all(
            # "Parentheses are not required for expressions that do not contain function calls, for example: foo + bar."
            # https://prql-lang.org/book/reference/syntax/operators.html?highlight=parenth#parentheses
            "from x derive a + 1",
            read={
                None: "SELECT *, a + 1 FROM x",
                "prql": "from x derive {a + 1}",
            },
            write={
                None: "SELECT *, a + 1 FROM x",
            },
        )
        self.validate_all(
            "from x derive {x = a + 1, b}",
            read={
                None: "SELECT *, a + 1 AS x, b FROM x",
            },
            write={
                None: "SELECT *, a + 1 AS x, b FROM x",
            },
        )
        self.validate_all(
            # TODO: Fix this test: `derive` should add columns.
            # https://prql-lang.org/book/reference/stdlib/transforms/derive.html
            "from x select {y = a + 1, 2}",
            read={
                None: "SELECT a + 1 AS y, 2 FROM x",
                "prql": "from x derive {x = a + 1, b} select {y = x, 2}",
            },
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
            read={
                None: "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
            },
            write={
                None: "SELECT *, a + 1 AS x, b FROM x WHERE age > 25",
            },
        )
        self.validate_all(
            "from x filter dept != 'IT'",
            read={
                None: "SELECT * FROM x WHERE dept <> 'IT'",
            },
            write={
                None: "SELECT * FROM x WHERE dept <> 'IT'",
            },
        )
        self.validate_all(
            "from x filter p == 'product' select {a, b}",
            read={
                None: "SELECT a, b FROM x WHERE p = 'product'",
                "prql": "from x filter p == 'product' select { a, b }",
            },
            write={
                None: "SELECT a, b FROM x WHERE p = 'product'",
            },
        )
        self.validate_all(
            "from x filter age > 25 && age < 27",
            read={
                None: "SELECT * FROM x WHERE age > 25 AND age < 27",
                "prql": "from x filter age > 25 filter age < 27",
            },
            write={
                None: "SELECT * FROM x WHERE age > 25 AND age < 27",
            },
        )
        self.validate_all(
            "from x filter (age > 25 && age < 27)",
            read={
                None: "SELECT * FROM x WHERE (age > 25 AND age < 27)",
            },
            write={
                None: "SELECT * FROM x WHERE (age > 25 AND age < 27)",
            },
        )
        self.validate_all(
            "from x filter (age > 25 || age < 27)",
            read={
                None: "SELECT * FROM x WHERE (age > 25 OR age < 27)",
            },
            write={
                None: "SELECT * FROM x WHERE (age > 25 OR age < 27)",
            },
        )
        self.validate_all(
            "from x filter ((age > 25 || age < 22) && age > 26) && age < 27",
            read={
                None: "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
                "prql": "from x filter (age > 25 || age < 22) filter age > 26 filter age < 27",
            },
            write={
                None: "SELECT * FROM x WHERE ((age > 25 OR age < 22) AND age > 26) AND age < 27",
            },
        )
        self.validate_all(
            "from x sort age",
            read={
                None: "SELECT * FROM x ORDER BY age",
            },
            write={
                None: "SELECT * FROM x ORDER BY age",
            },
        )
        self.validate_all(
            # "from x sort {-age}", # TODO: Fix test to be this.
            "from x sort -age",
            read={
                None: "SELECT * FROM x ORDER BY age DESC",
                "prql": "from x sort {-age}",
            },
            write={
                None: "SELECT * FROM x ORDER BY age DESC",
            },
        )
        self.validate_all(
            "from x sort {age, name}",
            read={
                None: "SELECT * FROM x ORDER BY age, name",
            },
            write={
                None: "SELECT * FROM x ORDER BY age, name",
            },
        )
        self.validate_all(
            "from x sort {-age, name}",
            read={
                None: "SELECT * FROM x ORDER BY age DESC, name",
                "prql": "from x sort {-age, +name}",
            },
            write={
                None: "SELECT * FROM x ORDER BY age DESC, name",
            },
        )
        self.validate_all(
            "from x append y",
            read={
                None: "SELECT * FROM x UNION ALL SELECT * FROM y",
            },
            write={
                None: "SELECT * FROM x UNION ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x remove y",
            read={
                None: "SELECT * FROM x EXCEPT ALL SELECT * FROM y",
            },
            write={
                None: "SELECT * FROM x EXCEPT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x intersect y",
            read={
                None: "SELECT * FROM x INTERSECT ALL SELECT * FROM y",
            },
            write={
                None: "SELECT * FROM x INTERSECT ALL SELECT * FROM y",
            },
        )
        self.validate_all(
            "from x filter a == null && b != null",
            read={
                None: "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
                "prql": "from x filter a == null filter null != b",
            },
            write={
                None: "SELECT * FROM x WHERE a IS NULL AND NOT b IS NULL",
            },
        )
        self.validate_all(
            "from x filter (a > 1 || b != null || c != null)",
            read={
                None: "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
                "prql": "from x filter (a > 1 || null != b || c != null)",
            },
            write={
                None: "SELECT * FROM x WHERE (a > 1 OR NOT b IS NULL OR NOT c IS NULL)",
            },
        )
        self.validate_all(
            "from a aggregate {average x}",
            read={
                None: "SELECT AVG(x) FROM a",
                "prql": "from a aggregate { average x }",
            },
            write={
                None: "SELECT AVG(x) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {average x, min y, ct = sum z}",
            read={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
                "prql": "from a aggregate { average x, min y, ct = sum z }",
            },
            write={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) AS ct FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {average x, min y, sum z}",
            read={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
                "prql": "from a aggregate { average x, min y, sum z }",
            },
            write={
                None: "SELECT AVG(x), MIN(y), COALESCE(SUM(z), 0) FROM a",
            },
        )
        self.validate_all(
            "from a aggregate {min y, b = stddev x, max z}",
            read={
                None: "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
                "prql": "from a aggregate { min y, b = stddev x, max z }"
            },
            write={
                None: "SELECT MIN(y), STDDEV(x) AS b, MAX(z) FROM a",
            },
        )
        self.validate_all(
            "from x sort age",
            read={
                None: "SELECT * FROM x ORDER BY age",
                "prql": "from x sort {+age}",
            },
            write={
                None: "SELECT * FROM x ORDER BY age",
            },
        )
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/syntax/operators.html?#parentheses
            # TODO: Check test correctness: Parenthesis not needed iff func call is RHS of tuple.
            "from a derive b = sum x", # TODO: Implement.
            read={
                None: "SELECT *, SUM(x) OVER() AS b FROM a",
                "prql": "from a derive b = (sum x)", # TODO: Implement.
            },
            write={
                None: "SELECT *, SUM(x) OVER() AS b FROM a",
            },
        )
        """
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/window.html
            "from a group x (sort y window rolling:5 (derive b = sum z))", # TODO: Implement.
            read={
                None: "SELECT *, SUM(z) OVER(PARTITION BY x ORDER BY y ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS b FROM a",
                "prql": "from a group x (sort y window rolling:5 (derive b = sum z))", # TODO: Implement.
            },
            write={
                None: "SELECT *, SUM(z) OVER(PARTITION BY x ORDER BY y ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS b FROM a",
            },
        )
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join b (a.c == b.c)",
            read={
                None: "SELECT a.*, b.* FROM a INNER JOIN b ON a.c = b.c",
                "prql": "from a join side:inner b (a.c == b.c)",
            },
            write={
                None: "SELECT a.*, b.* FROM a INNER JOIN b ON a.c = b.c",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join b (a.c == b.c)",
            read={
                None: "SELECT a.*, b.* FROM a JOIN b ON a.c = b.c",
                "prql": "from a join b (a.c == b.c)",
            },
            write={
                None: "SELECT a.*, b.* FROM a INNER JOIN b ON a.c = b.c",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join side:left b (a.c == b.c && a.d == 'MATCH_STR')",
            read={
                None: "SELECT a.*, b.* FROM a LEFT JOIN b ON a.c = b.c AND a.d = 'MATCH_STR'",
                "prql": "from a join side:left b (a.c == b.c && a.d == 'MATCH_STR')",
            },
            write={
                None: "SELECT a.*, b.* FROM a LEFT JOIN b ON a.c = b.c AND a.d = 'MATCH_STR'",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join side:right b (a.c == b.c && b.d == 'MATCH_STR')",
            read={
                None: "SELECT a.*, b.* FROM a RIGHT JOIN b ON a.c = b.c AND b.d = 'MATCH_STR'",
                "prql": "from a join side:right b (a.c == b.c && b.d == 'MATCH_STR')",
            },
            write={
                None: "SELECT a.*, b.* FROM a RIGHT JOIN b ON a.c = b.c AND b.d = 'MATCH_STR'",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join side:full b (a.c == b.c)",
            read={
                None: "SELECT a.*, b.* FROM a FULL JOIN b ON a.c = b.c",
                "prql": "from a join side:full b (a.c == b.c)",
            },
            write={
                None: "SELECT a.*, b.* FROM a FULL JOIN b ON a.c = b.c",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/join.html
            "from a join side:left b (a.c == b.c && a.c == 'MATCH_STR')",
            read={
                None: "SELECT a.*, b.* FROM a LEFT JOIN b ON a.c = b.c AND a.c = 'MATCH_STR'",
                "prql": "from a join side:left b (a.c == b.c && a.c == 'MATCH_STR')",
            },
            write={
                None: "SELECT a.*, b.* FROM a LEFT JOIN b ON a.c = b.c AND a.c = 'MATCH_STR'",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html
            "from a append b",
            read={
                None: "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
            write={
                None: "SELECT * FROM a UNION ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html#remove
            "from a remove b",
            read={
                None: "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            },
            write={
                None: "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/append.html#intersection
            "from a intersect b",
            read={
                None: "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            },
            write={
                None: "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
            },
        )
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/aggregate.html
            "from x aggregate {avg a, b = count a, sum c}", # TODO: Implement: Count.
            read={
                None: "SELECT AVG(a), COUNT(*) AS b, SUM(c) FROM x",
            },
            write={
                None: "SELECT AVG(a), COUNT(*) AS b, SUM(c) FROM x",
            },
        )
        """
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/aggregate.html
            "from x group {a, b} (aggregate {sum c, e = average d})",
            read={
                None: "SELECT a, b, SUM(c), AVG(d) AS e FROM x GROUP BY a, b",
                "prql": "from x group {a, b} (aggregate {sum c, e = average d})", # TODO: Implement.
            },
            write={
                None: "SELECT a, b, SUM(c), AVG(d) AS e FROM x GROUP BY a, b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/aggregate.html
            # https://prql-lang.org/book/reference/stdlib/transforms/select.html#excluding-columns
            "from x select !{b} group {a, b} (aggregate {sum c, e = average d})", # TODO: Implement: Exclusion of columns.
            read={
                None: "SELECT a, SUM(c), AVG(d) AS e FROM x GROUP BY a, b",
                "prql": "from x select !{b} group {a, b} (aggregate {sum c, e = average d})", # TODO: Implement.
            },
            write={
                None: "SELECT a, SUM(c), AVG(d) AS e FROM x GROUP BY a, b",
            },
        )
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/group.html?#group
            "from x group {a, b} (aggregate {average c, d = count c})", # TODO: Implement.
            read={
                None: "SELECT a, b, AVG(c), COUNT(*) AS d FROM x GROUP BY a, b",
            },
            write={
                None: "SELECT a, b, AVG(c), COUNT(*) AS d FROM x GROUP BY a, b",
            },
        )
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/group.html?#group
            "from x sort a take 1",
            read={
                None: "SELECT * FROM x ORDER BY a LIMIT 1",
            },
            write={
                None: "SELECT * FROM x ORDER BY a LIMIT 1",
            },
        )
        """
        self.validate_all(
            # https://prql-lang.org/book/reference/stdlib/transforms/group.html?#group
            "from x group a (sort b take 2)", # TODO: Implement.
            read={
                None: "WITH y AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY a ORDER BY b) AS z FROM x) SELECT * FROM y WHERE z <= 1",
            },
            write={
                None: "WITH y AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY a ORDER BY b) AS z FROM x) SELECT * FROM y WHERE z <= 1",
            },
        )
        """
