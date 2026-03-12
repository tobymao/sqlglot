from tests.dialects.test_dialect import Validator


class TestDB2(Validator):
    dialect = "db2"

    def test_db2(self):
        # Test basic identity
        self.validate_identity("SELECT FROM table1")
        self.validate_identity("SELECT a, b, c FROM table1")

        # Test DB2 specific data types
        self.validate_identity("CREATE TABLE t (a SMALLINT, b INT, c BIGINT)")
        self.validate_identity("CREATE TABLE t (a CHAR(10), b VARCHAR(100))")
        self.validate_identity("CREATE TABLE t (a DECIMAL(10, 2))")
        self.validate_identity("CREATE TABLE t (a TIMESTAMP)")

        # Test FETCH FIRST syntax
        self.validate_identity("SELECT * FROM t FETCH FIRST 10 ROWS ONLY")
        self.validate_identity("SELECT * FROM t FETCH FIRST ROW ONLY")

        # Test OFFSET syntax
        self.validate_identity("SELECT * FROM t OFFSET 5 ROWS")
        self.validate_identity("SELECT * FROM t OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")

        # Test CURRENT_DATE and CURRENT_TIMESTAMP
        self.validate_all(
            "SELECT CURRENT_DATE",
            write={
                "db2": "SELECT CURRENT DATE",
            },
        )
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP",
            write={
                "db2": "SELECT CURRENT TIMESTAMP",
            },
        )

        # Test concatenation with ||
        self.validate_identity("SELECT a || b FROM t")
        self.validate_identity("SELECT a || b || c FROM t")

        # Test POSSTR function (DB2's string position function)
        self.validate_all(
            "SELECT STRPOS(haystack, needle)",
            write={
                "db2": "SELECT POSSTR(haystack, needle)",
            },
        )

        # Test boolean conversion (DB2 uses 0/1 for boolean)
        self.validate_all(
            "SELECT TRUE, FALSE",
            write={
                "db2": "SELECT 1, 0",
            },
        )

        # Test CAST to CHAR
        self.validate_all(
            "CAST(value AS CHAR)",
            write={
                "db2": "CHAR(value)",
            },
        )

        # Test date arithmetic - skip for now as it needs more complex handling
        # self.validate_all(
        #     "SELECT date_col + INTERVAL '5' DAY",
        #     write={
        #         "db2": "SELECT date_col + 5 DAY",
        #     },
        # )

        # Test DAYOFWEEK and DAYOFYEAR extracts
        self.validate_all(
            "SELECT EXTRACT(DAYOFWEEK FROM date_col)",
            write={
                "db2": "SELECT DAYOFWEEK(date_col)",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(DAYOFYEAR FROM date_col)",
            write={
                "db2": "SELECT DAYOFYEAR(date_col)",
            },
        )

        # Test VARCHAR_FORMAT (DB2's time to string function)
        self.validate_all(
            "SELECT TIME_TO_STR(timestamp_col, 'YYYY-MM-DD')",
            write={
                "db2": "SELECT VARCHAR_FORMAT(timestamp_col, 'YYYY-MM-DD')",
            },
        )

        # Test DATEDIFF conversion
        self.validate_all(
            "SELECT DATEDIFF(date1, date2)",
            write={
                "db2": "SELECT DAYS(date1) - DAYS(date2)",
            },
        )

        # Test joins
        self.validate_identity("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id")
        self.validate_identity("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id")
        self.validate_identity("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")

        # Test subqueries
        self.validate_identity("SELECT * FROM (SELECT a, b FROM t1) AS subq")

        # Test aggregations
        self.validate_identity("SELECT COUNT(*) FROM t")
        self.validate_identity("SELECT SUM(amount) FROM t")
        self.validate_identity("SELECT AVG(amount) FROM t")
        self.validate_identity("SELECT MIN(amount), MAX(amount) FROM t")

        # Test GROUP BY and HAVING
        self.validate_identity("SELECT category, COUNT(*) FROM t GROUP BY category")
        self.validate_identity(
            "SELECT category, COUNT(*) FROM t GROUP BY category HAVING COUNT(*) > 5"
        )

        # Test ORDER BY
        self.validate_identity("SELECT * FROM t ORDER BY a")
        self.validate_identity("SELECT * FROM t ORDER BY a DESC")
        self.validate_identity("SELECT * FROM t ORDER BY a, b DESC")

        # Test CASE expressions
        self.validate_identity("SELECT CASE WHEN a > 0 THEN 'positive' ELSE 'negative' END FROM t")
        self.validate_identity(
            "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t"
        )

        # Test IN clause
        self.validate_identity("SELECT * FROM t WHERE a IN (1, 2, 3)")
        self.validate_all(
            "SELECT * FROM t WHERE a NOT IN (1, 2, 3)",
            write={
                "db2": "SELECT * FROM t WHERE NOT a IN (1, 2, 3)",
            },
        )

        # Test BETWEEN
        self.validate_identity("SELECT * FROM t WHERE a BETWEEN 1 AND 10")

        # Test LIKE
        self.validate_identity("SELECT * FROM t WHERE name LIKE 'John%'")

        # Test NULL handling
        self.validate_identity("SELECT * FROM t WHERE a IS NULL")
        self.validate_all(
            "SELECT * FROM t WHERE a IS NOT NULL",
            write={
                "db2": "SELECT * FROM t WHERE NOT a IS NULL",
            },
        )
        self.validate_identity("SELECT COALESCE(a, b, c) FROM t")

        # Test UNION
        self.validate_identity("SELECT a FROM t1 UNION SELECT a FROM t2")
        self.validate_identity("SELECT a FROM t1 UNION ALL SELECT a FROM t2")

        # Test WITH (CTE)
        self.validate_identity("WITH cte AS (SELECT * FROM t1) SELECT * FROM cte")

        # Test INSERT
        self.validate_identity("INSERT INTO t (a, b) VALUES (1, 2)")

        # Test UPDATE
        self.validate_identity("UPDATE t SET a = 1 WHERE b = 2")

        # Test DELETE
        self.validate_identity("DELETE FROM t WHERE a = 1")

        # Test CREATE TABLE
        self.validate_identity("CREATE TABLE t (id INT, name VARCHAR(100))")

        # Test DROP TABLE
        self.validate_identity("DROP TABLE t")

        # Test MAX/MIN with GREATEST/LEAST
        self.validate_all(
            "SELECT MAX(a, b, c)",
            write={
                "db2": "SELECT GREATEST(a, b, c)",
            },
        )

        self.validate_all(
            "SELECT MIN(a, b, c)",
            write={
                "db2": "SELECT LEAST(a, b, c)",
            },
        )
