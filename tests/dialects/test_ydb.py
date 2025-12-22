from sqlglot import parse_one
from sqlglot.dialects.ydb import make_db_name_lower, table_names_to_lower_case, eliminate_join_marks
from tests.dialects.test_dialect import Validator


class TestYDB(Validator):
    maxDiff = None
    dialect = "ydb"

    def test_datetrunc_year(self):
        sql = "SELECT DATE_TRUNC('year',dt) from table"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql, "SELECT DateTime::MakeDate(DateTime::StartOfYear(dt)) FROM `table`"
        )

    def test_datetrunc_month(self):
        sql = "SELECT DATE_TRUNC('month',dt) from table"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql, "SELECT DateTime::MakeDate(DateTime::StartOfMonth(dt)) FROM `table`"
        )

    def test_extract(self):
        sql = "SELECT EXTRACT(YEAR FROM dt) from table"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT DateTime::GetYear(dt) FROM `table`")

    def test_parse(self):
        sql = "SELECT to_date('29.03.2023', 'DD.MM.YYYY') from table"
        parsed = parse_one(sql, dialect="oracle")
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql,
            "SELECT DateTime::MakeTimestamp(DateTime::Parse('%d.%m.%Y')(\"29.03.2023\")) FROM `table`",
        )

    def test_subselect(self):
        sql = "SELECT * FROM (select * from b) T"
        parsed = parse_one(sql)
        self.assertEqual(parsed.sql(dialect="ydb"), "SELECT * FROM (SELECT * FROM `b`) AS T")

    def test_full_qualified_alias(self):
        sql = "SELECT a.a FROM T"
        parsed = parse_one(sql)
        self.assertEqual(parsed.sql(dialect="ydb"), "SELECT a.a AS a FROM `T`")

    def test_cte(self):
        sql = "with ct as (select * from b) SELECT * from ct"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertIn("$ct = (SELECT * FROM `b`);\n\nSELECT * FROM $ct AS ct", generated_sql)

    def test_embedded_cte(self):
        sql = "SELECT * from (with ct as (select * from b) select * from ct)"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql, "$ct = (SELECT * FROM `b`);\n\nSELECT * FROM (SELECT * FROM $ct AS ct)"
        )

    def test_array_any(self):
        sql = "SELECT * FROM TABLE WHERE ARRAY_ANY(arr, x -> x)"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql,
            "SELECT * FROM `TABLE` WHERE ListHasItems(ListFilter(($x) -> {RETURN $x}))",
        )

    def test_concat(self):
        sql = "SELECT CONCAT(A,B) FROM data"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT A || B FROM `data`")

    def test_nullif_null(self):
        sql = "SELECT NULLIF('a','a') FROM data"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT IF('a' = 'a', NULL, 'a') FROM `data`")

    def test_if(self):
        sql = "SELECT IF(10 > 20, 'TRUE', 'FALSE') FROM data"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT IF(10 > 20, 'TRUE', 'FALSE') FROM `data`")

    def test_basic(self):
        sql = "select * from a, b where a.id(+) = b.id"
        parsed = parse_one(sql, dialect="oracle")
        new_parsed = eliminate_join_marks(parsed)
        self.assertEqual(new_parsed.sql(), "SELECT * FROM b LEFT JOIN a ON a.id = b.id")

    def test_between(self):
        sql = "SELECT * FROM T WHERE SYSDATE BETWEEN A.valid_from_dttm(+) AND A.valid_to_dttm(+)"
        parsed = parse_one(sql, dialect="oracle")
        new_parsed = eliminate_join_marks(parsed)
        self.assertEqual(
            new_parsed.sql(),
            "SELECT * FROM T WHERE CURRENT_TIMESTAMP() BETWEEN A.valid_from_dttm AND A.valid_to_dttm",
        )

    def test_table_name_lower_case(self):
        sql = "SELECT * FROM B"
        parsed = parse_one(sql)
        parsed_new = table_names_to_lower_case(parsed)
        self.assertEqual(parsed_new.sql(), "SELECT * FROM b")

    def test_tables_name_lower_case(self):
        sql = "SELECT * FROM B, (SELECT * from D) as E"
        parsed = parse_one(sql)
        parsed_new = table_names_to_lower_case(parsed)
        self.assertEqual(parsed_new.sql(), "SELECT * FROM b, (SELECT * FROM d) AS E")

    def test_column_name_escape(self):
        sql = """SELECT B.*
                 FROM B"""
        parsed = parse_one(sql)
        self.assertEqual(parsed.sql(dialect="ydb"), "SELECT B.* FROM `B`")

    def test_date_add_month(self):
        sql = "select date_add('2025-01-01', interval 2 month)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT DateTime::MakeDate(DateTime::ShiftMonths(CAST('2025-01-01' AS DATE), 2))",
        )

    def test_date_sub_month(self):
        sql = "select date_add('2025-01-01', interval -2 month)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT DateTime::MakeDate(DateTime::ShiftMonths(CAST('2025-01-01' AS DATE), -2))",
        )

    def test_date_add_year(self):
        sql = "select date_add('2025-01-01', interval 2 years)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT DateTime::MakeDate(DateTime::ShiftYears(CAST('2025-01-01' AS DATE), 2))",
        )

    def test_date_add_day(self):
        sql = "select date_add('2025-01-01', interval 1 day)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromDays(1)",
        )

    def test_date_add_hour(self):
        sql = "select date_add('2025-01-01', interval 1 hour)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromHours(1)",
        )

    def test_date_add_minute(self):
        sql = "select date_add('2025-01-01', interval 1 minute)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromMinutes(1)",
        )

    def test_date_add_second(self):
        sql = "select date_add('2025-01-01', interval 1 second)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT CAST('2025-01-01' AS DATE) + DateTime::IntervalFromSeconds(1)",
        )

    def test_date_add_datetime_second(self):
        sql = "select date_add('2025-01-01 01:01:01', interval 1 second)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT DateTime::MakeDateTime(DateTime::ParseIso8601('2025-01-01T01:01:01')) + DateTime::IntervalFromSeconds(1)",
        )

    def test_date_sub_day(self):
        sql = "select date_sub('2025-01-01', interval 1 day)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT CAST('2025-01-01' AS DATE) - DateTime::IntervalFromDays(1)",
        )

    def test_bit(self):
        sql = "SELECT CAST(1 as BIT)"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT CAST(1 AS Uint8)")

    def test_db_lower_case(self):
        sql = "SELECT * FROM 'A'.'B'"
        parsed = make_db_name_lower(parse_one(sql))
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, "SELECT * FROM `a/B`")

    def test_decorrelate_scalar_subquery(self):
        sql = """SELECT a.id, (SELECT MAX(b.value) FROM b WHERE b.id = a.id) as max_value
                 FROM a"""

        expected = """SELECT a.id AS id, _u_0._u_2 AS max_value FROM `a` LEFT JOIN (SELECT MAX(b.value) AS _u_2, b.id AS _u_1 FROM `b` WHERE TRUE GROUP BY (id AS id)) AS _u_0 ON a.id = _u_0._u_1"""
        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_decorrelate_exists_subquery(self):
        sql = """SELECT a.id
                 FROM a
                 WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)"""

        expected = """SELECT a.id AS id FROM `a` LEFT JOIN (SELECT a_id AS _u_1, 1 AS _exists_flag FROM `b` WHERE TRUE GROUP BY (a_id AS _u_1)) AS _u_0 ON a.id = _u_0._u_1 WHERE NOT (_u_0._u_1 IS NULL)"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_decorrelate_in_subquery(self):
        sql = """SELECT a.id \
                 FROM a \
                 WHERE a.id IN (SELECT b.a_id FROM b WHERE b.value > 10)"""

        expected = """SELECT a.id AS id FROM `a` LEFT JOIN (SELECT b.a_id AS a_id FROM `b` WHERE b.value > 10 GROUP BY (a_id AS a_id)) AS _u_0 ON a.id = _u_0.a_id WHERE NOT (_u_0.a_id IS NULL)"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_decorrelate_multiple_subqueries(self):
        sql = """SELECT a.id, \
                        (SELECT COUNT(*) FROM b WHERE b.a_id = a.id)     as cnt, \
                        (SELECT AVG(b.value) FROM b WHERE b.a_id = a.id) as avg_val
                 FROM a"""

        expected = """SELECT a.id AS id, COALESCE(_u_0._u_2, 0) AS cnt, _u_3._u_5 AS avg_val FROM `a` LEFT JOIN (SELECT COUNT(*) AS _u_2, b.a_id AS _u_1 FROM `b` WHERE TRUE GROUP BY (a_id AS a_id)) AS _u_0 ON a.id = _u_0._u_1 LEFT JOIN (SELECT AVG(b.value) AS _u_5, b.a_id AS _u_4 FROM `b` WHERE TRUE GROUP BY (a_id AS a_id)) AS _u_3 ON a.id = _u_3._u_4"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")

        self.assertEqual(result, expected)

    def test_decorrelate_nested_subqueries(self):
        sql = """SELECT a.id \
                 FROM a
                 WHERE EXISTS (SELECT 1 \
                               FROM b \
                               WHERE b.a_id = a.id \
                                 AND EXISTS (SELECT 1 FROM c WHERE c.b_id = b.id))"""

        expected = """SELECT a.id AS id FROM `a` LEFT JOIN (SELECT a_id AS _u_3, 1 AS _exists_flag FROM `b` LEFT JOIN (SELECT b_id AS _u_1, 1 AS _exists_flag FROM `c` WHERE TRUE GROUP BY (b_id AS _u_1)) AS _u_0 ON b.id = _u_0._u_1 WHERE TRUE AND NOT (_u_0._u_1 IS NULL) GROUP BY (a_id AS a_id)) AS _u_2 ON a.id = _u_2._u_3 WHERE NOT (_u_2._u_3 IS NULL)"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_unnest_scalar_subquery(self):
        sql = """SELECT * \
                 FROM x \
                 WHERE (SELECT y.a FROM y WHERE x.a = y.a) = 1"""

        expected = """SELECT * FROM `x` LEFT JOIN (SELECT y.a AS a, y.a AS _u_1 FROM `y` WHERE TRUE GROUP BY (a AS a)) AS _u_0 ON x.a = _u_0._u_1 WHERE _u_0.a = 1"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_unnest_any_subquery(self):
        sql = """SELECT * \
                 FROM x \
                 WHERE x.a > ANY (SELECT y.a FROM y WHERE y.b = x.b)"""

        expected = """SELECT * FROM `x` LEFT JOIN (SELECT y.a AS a, y.b AS _u_1 FROM `y` WHERE TRUE GROUP BY (b AS b)) AS _u_0 ON x.b = _u_0._u_1 WHERE x.a > ListHasItems(($_x, $p_0)->(ListFilter($_x, ($_x) -> {RETURN $p_0 > $_x}))(a, x.a))"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")
        self.assertEqual(result, expected)

    def test_unnest_in_subquery(self):
        sql = """SELECT * \
                 FROM x \
                 WHERE x.a IN (SELECT y.a FROM y WHERE y.b = x.b)"""
        expected = """SELECT * FROM `x` LEFT JOIN (SELECT y.a AS a, y.b AS _u_1 FROM `y` WHERE TRUE GROUP BY (b AS b)) AS _u_0 ON x.b = _u_0._u_1 WHERE ListHasItems(($_x, $p_0) - > (ListFilter($_x, ($_x) -> {RETURN $_x = $p_0}))(a, x.a))"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")

        self.assertEqual(result, expected)

    def test_unnest_aggregate_subquery(self):
        sql = """SELECT * \
                 FROM x \
                 WHERE (SELECT MAX(y.value) FROM y WHERE y.x_id = x.id) > 100"""
        expected = """SELECT * FROM `x` LEFT JOIN (SELECT MAX(y.value) AS _u_2, y.x_id AS _u_1 FROM `y` WHERE TRUE GROUP BY (x_id AS x_id)) AS _u_0 ON x.id = _u_0._u_1 WHERE _u_0._u_2 > 100"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")

        self.assertEqual(result, expected)

    def test_unnest_correlated_count_subquery(self):
        sql = """SELECT x.id, (SELECT COUNT(*) FROM y WHERE y.x_id = x.id) as y_count \
                 FROM x"""

        expected = """SELECT x.id AS id, COALESCE(_u_0._u_2, 0) AS y_count FROM `x` LEFT JOIN (SELECT COUNT(*) AS _u_2, y.x_id AS _u_1 FROM `y` WHERE TRUE GROUP BY (x_id AS x_id)) AS _u_0 ON x.id = _u_0._u_1"""

        parsed = parse_one(sql)
        result = parsed.sql(dialect="ydb")

        self.assertEqual(result, expected)

    def test_case_with_subquery_in_then(self):
        sql = """
              SELECT CASE \
                         WHEN a = 1 THEN (SELECT MAX(b) FROM t2 WHERE t2.a = t1.a) \
                         ELSE 0 \
                         END as val
              FROM t1 \
              """
        expected = "SELECT CASE WHEN a = 1 THEN _u_1._col0 ELSE 0 END AS val FROM `t1`, (SELECT MAX(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1"

        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, expected)

    def test_case_with_subquery_in_else(self):
        sql = """
              SELECT CASE \
                         WHEN a = 1 THEN 100 \
                         ELSE (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a) \
                         END as val
              FROM t1 \
              """
        expected = "SELECT CASE WHEN a = 1 THEN 100 ELSE _u_1._col0 END AS val FROM `t1`, (SELECT MIN(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1"

        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, expected)

    def test_nested_case(self):
        sql = """
              SELECT CASE \
                         WHEN a > 0 THEN \
                             CASE \
                                 WHEN b > 0 THEN 'A' \
                                 ELSE 'B' \
                                 END \
                         ELSE 'C' \
                         END as result
              FROM t \
              """
        expected = "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 'A' ELSE 'B' END ELSE 'C' END AS result FROM `t`"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, expected)

    def test_if_with_subquery_in_true(self):
        sql = """
              SELECT IF(
                             a > 10,
                             (SELECT SUM(b) FROM t2 WHERE t2.a = t1.a),
                             0
                     ) as res
              FROM t1 \
              """
        expected = "SELECT IF(a > 10, _u_1._col0, 0) AS res FROM `t1`, (SELECT SUM(b) AS _col0 FROM `t2` WHERE t2.a = t1.a) AS _u_1"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(generated_sql, expected)

    def test_validate_identity_select(self):
        self.validate_identity("SELECT * FROM `table`")

    def test_validate_identity_where(self):
        self.validate_identity("SELECT * FROM `table` WHERE id = 1")

    def test_validate_identity_order_by(self):
        self.validate_identity("SELECT * FROM `table` ORDER BY id DESC")

    def test_validate_identity_limit(self):
        self.validate_identity("SELECT * FROM `table` LIMIT 10")

    def test_validate_identity_offset(self):
        self.validate_identity("SELECT * FROM `table` LIMIT 10 OFFSET 5")

    def test_validate_identity_distinct(self):
        self.validate_identity("SELECT DISTINCT id FROM `table`")

    def test_validate_identity_join(self):
        self.validate_identity("SELECT * FROM `a` INNER JOIN `b` ON a.id = b.id")

    def test_validate_identity_left_join(self):
        self.validate_identity("SELECT * FROM `a` LEFT JOIN `b` ON a.id = b.id")

    def test_validate_identity_right_join(self):
        self.validate_identity("SELECT * FROM `a` RIGHT JOIN `b` ON a.id = b.id")

    def test_validate_identity_full_join(self):
        self.validate_identity("SELECT * FROM `a` FULL JOIN `b` ON a.id = b.id")

    def test_validate_identity_cross_join(self):
        self.validate_identity("SELECT * FROM `a`, `b`")

    def test_validate_identity_union(self):
        self.validate_identity("SELECT * FROM `a` UNION SELECT * FROM `b`")

    def test_validate_identity_union_all(self):
        self.validate_identity("SELECT * FROM `a` UNION ALL SELECT * FROM `b`")

    def test_validate_identity_intersect(self):
        self.validate_identity("SELECT * FROM `a` INTERSECT SELECT * FROM `b`")

    def test_validate_identity_except(self):
        self.validate_identity("SELECT * FROM `a` EXCEPT SELECT * FROM `b`")

    def test_validate_identity_case(self):
        self.validate_identity(
            "SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM `table`"
        )

    def test_validate_identity_coalesce(self):
        self.validate_identity("SELECT COALESCE(name, 'unknown') FROM `table`")

    def test_validate_identity_nullif(self):
        self.validate_identity("SELECT IF(value = 0, NULL, value) FROM `table`")

    def test_validate_identity_cast(self):
        self.validate_identity("SELECT CAST(id AS Utf8) FROM `table`")

    def test_validate_identity_aggregates(self):
        self.validate_identity(
            "SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM `table`"
        )

    def test_validate_identity_window_functions(self):
        self.validate_identity("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM `table`")

    def test_validate_identity_partition_by(self):
        self.validate_identity(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) FROM `table`"
        )

    def test_validate_identity_like(self):
        self.validate_identity("SELECT * FROM `table` WHERE name LIKE 'test%'")

    def test_validate_identity_between(self):
        self.validate_identity("SELECT * FROM `table` WHERE id BETWEEN 1 AND 10")

    def test_validate_identity_is_null(self):
        self.validate_identity("SELECT * FROM `table` WHERE name IS NULL")

    def test_validate_identity_arithmetic(self):
        self.validate_identity("SELECT a + b, a - b, a * b, a / b FROM `table`")

    def test_validate_identity_logical(self):
        self.validate_identity("SELECT * FROM `table` WHERE a > 0 AND b < 10 OR c = 5")

    def test_validate_identity_comparison(self):
        self.validate_identity(
            "SELECT * FROM `table` WHERE a = b AND a <> c AND a > d AND a < e AND a >= f AND a <= g"
        )

    def test_ydb_struct_access(self):
        sql = "SELECT struct.field, struct.subfield.subsub FROM table"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT struct.field AS field, struct.subfield.subsub AS subsub FROM `table`",
        )

    def test_ydb_json_functions(self):
        sql = "SELECT JSON_VALUE(data, '$.path'), JSON_QUERY(data, '$.path') FROM table"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT JSON_VALUE(data, '$.path'), JSON_QUERY(data, '$.path') FROM `table`",
        )

    def test_ydb_typed_parameters(self):
        sql = "SELECT * FROM table WHERE id = CAST($id AS Uint64)"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"), "SELECT * FROM `table` WHERE id = CAST($id AS Uint64)"
        )

    def test_ydb_escape_quotes(self):
        sql = "SELECT 'it''s a test' FROM table"
        parsed = parse_one(sql)
        self.assertEqual(parsed.sql(dialect="ydb"), "SELECT 'it''s a test' FROM `table`")

    def test_ydb_multiple_cte(self):
        sql = """
              WITH cte1 AS (SELECT * FROM table1), \
                   cte2 AS (SELECT * FROM table2)
              SELECT * \
              FROM cte1 \
                       JOIN cte2 ON cte1.id = cte2.id \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            """$cte1 = (SELECT * FROM `table1`);

$cte2 = (SELECT * FROM `table2`);

SELECT * FROM $cte1 AS cte1 JOIN $cte2 AS cte2 ON cte1.id = cte2.id""",
            generated_sql,
        )

    def test_ydb_recursive_cte(self):
        sql = """
              WITH RECURSIVE cte AS (SELECT 1 as level \
                                     UNION ALL \
                                     SELECT level + 1 \
                                     FROM cte \
                                     WHERE level < 10)
              SELECT * \
              FROM cte \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            """$cte = (SELECT 1 AS level UNION ALL SELECT level + 1 FROM $cte AS cte WHERE level < 10);

SELECT * FROM $cte AS cte""",
            generated_sql,
        )

    def test_ydb_alter_table(self):
        sql = "ALTER TABLE table ADD COLUMN new_column String"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"), "ALTER TABLE `table` ADD COLUMN new_column Utf8"
        )

    def test_ydb_create_table(self):
        sql = """
              CREATE TABLE table \
              ( \
                  id         Uint64 NOT NULL, \
                  name       String, \
                  created_at Timestamp, \
                  PRIMARY KEY (id)
              ) \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            """CREATE TABLE `table` (id Uint64 NOT NULL, name Utf8, created_at Timestamp, PRIMARY KEY(`id`))
PARTITION BY HASH (`id`);""",
            generated_sql,
        )

    def test_ydb_drop_table(self):
        sql = "DROP TABLE table"
        parsed = parse_one(sql)
        self.assertEqual(parsed.sql(dialect="ydb"), "DROP TABLE `table`")

    def test_ydb_comment(self):
        sql = """
              -- This is a comment
              SELECT * \
              FROM table
              /* Multi-line
                 comment */
              WHERE id = 1 \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            """/* This is a comment */ SELECT * FROM `table` /* Multi-line
                 comment */ WHERE id = 1""",
            generated_sql,
        )
        self.assertIn("SELECT * FROM `table`", generated_sql)
        self.assertIn("WHERE id = 1", generated_sql)

    def test_ydb_nested_functions(self):
        sql = "SELECT COALESCE(NULLIF(name, ''), 'default') FROM table"
        parsed = parse_one(sql)
        self.assertEqual(
            parsed.sql(dialect="ydb"),
            "SELECT COALESCE(IF(name = '', NULL, name), 'default') FROM `table`",
        )

    def test_ydb_timestamp_functions(self):
        sql = "SELECT UNIX_TIMESTAMP(), FROM_UNIXTIME(1234567890) FROM table"
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertTrue("UNIX_TIMESTAMP" in generated_sql)

    def test_create_table_simple_types(self):
        sql = """
              CREATE TABLE users
              (
                  id         Uint64 NOT NULL,
                  username   Utf8   NOT NULL,
                  email      String,
                  age        Int32,
                  height     Float,
                  weight Double,
                  created_at Timestamp,
                  balance    DECIMAL(6,1),
                  data_bytes Bytes,
                  PRIMARY KEY (id)
              ) \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")
        self.assertEqual(
            generated_sql,
            "CREATE TABLE `users` (id Uint64 NOT NULL, username Utf8 NOT NULL, email Utf8, age INT32, height Float, weight Double, created_at Timestamp, balance Decimal(6, 1), data_bytes Bytes, PRIMARY KEY(`id`))\nPARTITION BY HASH (`id`);",
        )


    def test_create_table_complex_types(self):
        sql = """
              CREATE TABLE users
              (
                  id          Uint64 NOT NULL,
                  username    Utf8   NOT NULL,
                  email       String,
                  age         Int32,
                  height      Float,
                  weight Double,
                  created_at  Timestamp,
                  balance     DECIMAL(6, 1),
                  small_id    SMALLINT,
                  tiny_id     TINYINT,
                  big_id      BIGINT,
                  first_name  VARCHAR(50),
                  last_name   NVARCHAR(100),
                  description TEXT,
                  long_text   LONGTEXT,
                  price       DECIMAL(10, 2),
                  amount      NUMERIC(8),
                  rating      DECIMAL(3, 1),
                  birth_date  DATE,
                  event_time  DATETIME,
                  image_data  BLOB,    
                  PRIMARY KEY (id)
              ) \
              """
        parsed = parse_one(sql)
        generated_sql = parsed.sql(dialect="ydb")

        self.assertEqual("""CREATE TABLE `users` (id Uint64 NOT NULL, username Utf8 NOT NULL, email Utf8, age INT32, height Float, weight Double, created_at Timestamp, balance Decimal(6, 1), small_id INT16, tiny_id INT8, big_id INT64, first_name Utf8, last_name Utf8, description Utf8, long_text String, price Decimal(10, 2), amount Decimal(8, 0), rating Decimal(3, 1), birth_date DATE, event_time DATETIME, image_data String, PRIMARY KEY(`id`))
PARTITION BY HASH (`id`);""", generated_sql)