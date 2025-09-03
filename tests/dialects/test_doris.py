from tests.dialects.test_dialect import Validator


class TestDoris(Validator):
    dialect = "doris"

    def test_doris(self):
        self.validate_all(
            "SELECT TO_DATE('2020-02-02 00:00:00')",
            write={
                "doris": "SELECT TO_DATE('2020-02-02 00:00:00')",
                "oracle": "SELECT CAST('2020-02-02 00:00:00' AS DATE)",
            },
        )
        self.validate_all(
            "SELECT MAX_BY(a, b), MIN_BY(c, d)",
            read={
                "clickhouse": "SELECT argMax(a, b), argMin(c, d)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            read={
                "clickhouse": "SELECT arraySum(x -> x*x, [2, 3])",
            },
            write={
                "clickhouse": "SELECT arraySum(x -> x * x, [2, 3])",
                "doris": "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            },
        )
        self.validate_all(
            "MONTHS_ADD(d, n)",
            read={
                "oracle": "ADD_MONTHS(d, n)",
            },
            write={
                "doris": "MONTHS_ADD(d, n)",
                "oracle": "ADD_MONTHS(d, n)",
            },
        )
        self.validate_all(
            """SELECT JSON_EXTRACT(CAST('{"key": 1}' AS JSONB), '$.key')""",
            read={
                "postgres": """SELECT '{"key": 1}'::jsonb ->> 'key'""",
            },
            write={
                "doris": """SELECT JSON_EXTRACT(CAST('{"key": 1}' AS JSONB), '$.key')""",
                "postgres": """SELECT JSON_EXTRACT_PATH(CAST('{"key": 1}' AS JSONB), 'key')""",
            },
        )
        self.validate_all(
            "SELECT GROUP_CONCAT('aa', ',')",
            read={
                "doris": "SELECT GROUP_CONCAT('aa', ',')",
                "mysql": "SELECT GROUP_CONCAT('aa' SEPARATOR ',')",
                "postgres": "SELECT STRING_AGG('aa', ',')",
            },
        )
        self.validate_all(
            "SELECT LAG(1, 1, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LAG(1, 1, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LAG(1) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LAG(1, 2, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LAG(1, 2, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LAG(1, 2) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LEAD(1, 1, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LEAD(1, 1, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LEAD(1) OVER (ORDER BY 1)",
            },
        )
        self.validate_all(
            "SELECT LEAD(1, 2, NULL) OVER (ORDER BY 1)",
            read={
                "doris": "SELECT LEAD(1, 2, NULL) OVER (ORDER BY 1)",
                "postgres": "SELECT LEAD(1, 2) OVER (ORDER BY 1)",
            },
        )
        self.validate_identity("""JSON_TYPE('{"foo": "1" }', '$.foo')""")

        self.validate_identity("L2_DISTANCE(x, y)")

    def test_identity(self):
        self.validate_identity("CREATE TABLE t (c INT) PROPERTIES ('x'='y')")
        self.validate_identity("CREATE TABLE t (c INT) COMMENT 'c'")
        self.validate_identity("COALECSE(a, b, c, d)")
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS example_tbl_unique (user_id BIGINT NOT NULL, user_name VARCHAR(50) NOT NULL, city VARCHAR(20), age SMALLINT, sex TINYINT) UNIQUE KEY (user_id, user_name) DISTRIBUTED BY HASH (user_id) BUCKETS 10 PROPERTIES ('enable_unique_key_merge_on_write'='true')"
        )
        self.validate_identity("INSERT OVERWRITE TABLE test PARTITION(p1, p2) VALUES (1, 2)")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
        self.validate_identity("DATE_TRUNC(event_date, 'DAY')")
        self.validate_identity("DATE_TRUNC('2010-12-02 19:28:30', 'HOUR')")
        self.validate_identity("CURRENT_DATE()")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl")
        self.validate_identity("ANALYZE DATABASE db")
        self.validate_identity("ANALYZE TABLE TBL(c1, c2)")

    def test_key(self):
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) UNIQUE KEY (c1)")
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) DUPLICATE KEY (c1)")
        self.validate_identity("CREATE MATERIALIZED VIEW test_table (c1 INT, c2 INT) KEY (c1)")

    def test_distributed(self):
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 INT) UNIQUE KEY (c1) DISTRIBUTED BY HASH (c1)"
        )
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 INT) DISTRIBUTED BY RANDOM")
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 INT) DISTRIBUTED BY RANDOM BUCKETS 1"
        )

    def test_partition(self):
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (PARTITION `p201701` VALUES LESS THAN ('2017-02-01'), PARTITION `p201702` VALUES LESS THAN ('2017-03-01'))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (PARTITION `p201701` VALUES [('2017-01-01'), ('2017-02-01')), PARTITION `other` VALUES LESS THAN (MAXVALUE))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY RANGE (`c2`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR)"
        )
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY (c2)")
        self.validate_identity("CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY (c1, c2)")
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT, c2 DATE) PARTITION BY (DATE_TRUNC(c2, 'MONTH'))"
        )
        self.validate_identity(
            "CREATE TABLE test_table (c1 INT) PARTITION BY LIST (`c1`) (PARTITION p1 VALUES IN (1, 2), PARTITION p2 VALUES IN (3))"
        )

    def test_table_alias_conversion(self):
        """Test conversion from postgres to Doris for DELETE/UPDATE statements with table aliases."""

        # Test cases for DELETE statements with table aliases
        self.validate_all(
            "DELETE FROM sales s WHERE s.id = 1",
            read={
                "postgres": "DELETE FROM sales AS s WHERE s.id = 1",
            },
            write={
                "doris": "DELETE FROM sales s WHERE s.id = 1",
                "postgres": "DELETE FROM sales AS s WHERE s.id = 1",
            },
        )

        # DELETE with multiple table references
        self.validate_all(
            "DELETE FROM orders o WHERE o.customer_id IN (SELECT c.id FROM customers AS c WHERE c.status_code = 'inactive')",
            read={
                "postgres": "DELETE FROM orders AS o WHERE o.customer_id IN (SELECT c.id FROM customers AS c WHERE c.status_code = 'inactive')",
            },
            write={
                "doris": "DELETE FROM orders o WHERE o.customer_id IN (SELECT c.id FROM customers AS c WHERE c.status_code = 'inactive')",
                "postgres": "DELETE FROM orders AS o WHERE o.customer_id IN (SELECT c.id FROM customers AS c WHERE c.status_code = 'inactive')",
            },
        )

        # DELETE with EXISTS clause
        self.validate_all(
            "DELETE FROM temp_data t WHERE NOT EXISTS(SELECT 1 FROM main_data AS m WHERE m.id = t.id)",
            read={
                "postgres": "DELETE FROM temp_data AS t WHERE NOT EXISTS(SELECT 1 FROM main_data AS m WHERE m.id = t.id)",
            },
            write={
                "doris": "DELETE FROM temp_data t WHERE NOT EXISTS(SELECT 1 FROM main_data AS m WHERE m.id = t.id)",
                "postgres": "DELETE FROM temp_data AS t WHERE NOT EXISTS(SELECT 1 FROM main_data AS m WHERE m.id = t.id)",
            },
        )

        # UPDATE statements with table aliases
        self.validate_all(
            "UPDATE employees e SET e.salary = e.salary * 1.1 WHERE e.department = 'IT'",
            read={
                "postgres": "UPDATE employees AS e SET e.salary = e.salary * 1.1 WHERE e.department = 'IT'",
            },
            write={
                "doris": "UPDATE employees e SET e.salary = e.salary * 1.1 WHERE e.department = 'IT'",
                "postgres": "UPDATE employees AS e SET e.salary = e.salary * 1.1 WHERE e.department = 'IT'",
            },
        )

        # UPDATE with multiple columns
        self.validate_all(
            "UPDATE accounts a SET a.balance = a.balance + 100, a.status_code = 'active' WHERE a.account_type = 'savings'",
            read={
                "postgres": "UPDATE accounts AS a SET a.balance = a.balance + 100, a.status_code = 'active' WHERE a.account_type = 'savings'",
            },
            write={
                "doris": "UPDATE accounts a SET a.balance = a.balance + 100, a.status_code = 'active' WHERE a.account_type = 'savings'",
                "postgres": "UPDATE accounts AS a SET a.balance = a.balance + 100, a.status_code = 'active' WHERE a.account_type = 'savings'",
            },
        )

        # UPDATE with multiple table references in subquery
        self.validate_all(
            "UPDATE prices p SET p.amount = p.amount * 0.9 WHERE p.product_id IN (SELECT pr.id FROM products AS pr JOIN categories AS c ON pr.category_id = c.id WHERE c.foo = 'Electronics')",
            read={
                "postgres": "UPDATE prices AS p SET p.amount = p.amount * 0.9 WHERE p.product_id IN (SELECT pr.id FROM products AS pr JOIN categories AS c ON pr.category_id = c.id WHERE c.foo = 'Electronics')",
            },
            write={
                "doris": "UPDATE prices p SET p.amount = p.amount * 0.9 WHERE p.product_id IN (SELECT pr.id FROM products AS pr JOIN categories AS c ON pr.category_id = c.id WHERE c.foo = 'Electronics')",
                "postgres": "UPDATE prices AS p SET p.amount = p.amount * 0.9 WHERE p.product_id IN (SELECT pr.id FROM products AS pr JOIN categories AS c ON pr.category_id = c.id WHERE c.foo = 'Electronics')",
            },
        )

    def test_rename_table(self):
        self.validate_all(
            "ALTER TABLE db.t1 RENAME TO db.t2",
            write={
                "snowflake": "ALTER TABLE db.t1 RENAME TO db.t2",
                "duckdb": "ALTER TABLE db.t1 RENAME TO t2",
                "doris": "ALTER TABLE db.t1 RENAME t2",
            },
        )

    def test_materialized_view_properties(self):
        # BUILD modes
        self.validate_identity("CREATE MATERIALIZED VIEW mv BUILD IMMEDIATE AS SELECT 1")
        self.validate_identity("CREATE MATERIALIZED VIEW mv BUILD DEFERRED AS SELECT 1")

        # REFRESH methods with triggers
        self.validate_identity("CREATE MATERIALIZED VIEW mv REFRESH COMPLETE ON MANUAL AS SELECT 1")
        self.validate_identity("CREATE MATERIALIZED VIEW mv REFRESH AUTO ON COMMIT AS SELECT 1")
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE STARTS '2025-01-01 00:00:00' AS SELECT 1"
        )

        # Combined BUILD and REFRESH
        self.validate_identity(
            "CREATE MATERIALIZED VIEW mv BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 MINUTE AS SELECT 1"
        )
