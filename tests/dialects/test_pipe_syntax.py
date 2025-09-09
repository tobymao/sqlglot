from tests.dialects.test_dialect import Validator


class TestPipeSyntax(Validator):
    def test_select(self):
        self.validate_identity("FROM x", "SELECT * FROM x")
        self.validate_identity(
            "FROM x |> SELECT x1, x2", "WITH __tmp1 AS (SELECT x1, x2 FROM x) SELECT * FROM __tmp1"
        )
        self.validate_identity(
            "FROM x |> SELECT x.x1, x.x2",
            "WITH __tmp1 AS (SELECT x.x1, x.x2 FROM x) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> SELECT x1 as c1, x2 as c2",
            "WITH __tmp1 AS (SELECT x1 AS c1, x2 AS c2 FROM x) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> SELECT x1 + 1 as x1_a, x2 - 1 as x2_a |> WHERE x1_a > 1",
            "WITH __tmp1 AS (SELECT x1 + 1 AS x1_a, x2 - 1 AS x2_a FROM x) SELECT * FROM __tmp1 WHERE x1_a > 1",
        )
        self.validate_identity(
            "FROM x |> SELECT x1 + 1 as x1_a, x2 - 1 as x2_a |> WHERE x1_a > 1 |> SELECT x2_a",
            "WITH __tmp1 AS (SELECT x1 + 1 AS x1_a, x2 - 1 AS x2_a FROM x), __tmp2 AS (SELECT x2_a FROM __tmp1 WHERE x1_a > 1) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "FROM x |> WHERE x1 > 0 OR x2 > 0 |> WHERE x3 > 1 AND x4 > 1 |> SELECT x1, x4",
            "WITH __tmp1 AS (SELECT x1, x4 FROM x WHERE (x1 > 0 OR x2 > 0) AND (x3 > 1 AND x4 > 1)) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> WHERE x1 > 1 |> WHERE x2 > 2 |> SELECT x1 as gt1, x2 as gt2",
            "WITH __tmp1 AS (SELECT x1 AS gt1, x2 AS gt2 FROM x WHERE x1 > 1 AND x2 > 2) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> WHERE x1 > 1 AND x2 > 2 |> SELECT x1 as gt1, x2 as gt2 |> SELECT gt1 * 2 + gt2 * 2 AS gt2_2",
            "WITH __tmp1 AS (SELECT x1 AS gt1, x2 AS gt2 FROM x WHERE x1 > 1 AND x2 > 2), __tmp2 AS (SELECT gt1 * 2 + gt2 * 2 AS gt2_2 FROM __tmp1) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "SELECT 1 AS y, 2 AS x |> SELECT x, y",
            "WITH __tmp1 AS (SELECT x, y FROM (SELECT 1 AS y, 2 AS x)) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "SELECT x1, x2, x3 FROM x |> AS a_x |> WHERE a_x.x1 > 0",
            "WITH a_x AS (SELECT x1, x2, x3 FROM x) SELECT * FROM a_x WHERE a_x.x1 > 0",
        )
        self.validate_identity(
            "SELECT x,y FROM (SELECT 1 as x, 2 as y) |> SELECT x, y",
            "WITH __tmp1 AS (SELECT x, y FROM (SELECT 1 AS x, 2 AS y)) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "SELECT 'foo1' AS item1, 2 AS item2 UNION ALL SELECT 'foo2' AS item1, 5 AS item2 |> EXTEND SUM(item2) OVER() AS item2_sum",
            "WITH __tmp1 AS (SELECT *, SUM(item2) OVER () AS item2_sum FROM (SELECT 'foo1' AS item1, 2 AS item2 UNION ALL SELECT 'foo2' AS item1, 5 AS item2)) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "SELECT x, x1 FROM (FROM (SELECT 1 as x, 2 as x1) |> AGGREGATE SUM(x1) as xx GROUP BY x,x1) |> SELECT x",
            "WITH __tmp2 AS (SELECT x FROM (SELECT * FROM (WITH __tmp1 AS (SELECT SUM(x1) AS xx, x, x1 FROM (SELECT 1 AS x, 2 AS x1) GROUP BY x, x1) SELECT * FROM __tmp1))) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "FROM (SELECT 1 as x1) AS x |> SELECT x.x1 |> UNION ALL (FROM (SELECT 1 AS c) |> SELECT c) |> SELECT x1",
            "SELECT * FROM (WITH __tmp1 AS (SELECT x.x1 FROM (SELECT 1 AS x1) AS x), __tmp3 AS (SELECT * FROM __tmp1), __tmp4 AS (SELECT * FROM __tmp3 UNION ALL SELECT * FROM (WITH __tmp2 AS (SELECT c FROM (SELECT 1 AS c)) SELECT * FROM __tmp2)), __tmp5 AS (SELECT x1 FROM __tmp4) SELECT * FROM __tmp5)",
        )
        self.validate_identity(
            "FROM (SELECT x1 FROM (SELECT 1 as x1) |> SELECT x1) |> SELECT x1",
            "SELECT * FROM (WITH __tmp2 AS (SELECT x1 FROM ((WITH __tmp1 AS (SELECT x1 FROM (SELECT 1 AS x1)) SELECT * FROM __tmp1))) SELECT * FROM __tmp2)",
        )
        self.validate_identity(
            "SELECT * FROM (FROM t2 |> SELECT id)",
            "SELECT * FROM (WITH __tmp1 AS (SELECT id FROM t2) SELECT * FROM __tmp1)",
        )
        self.validate_identity(
            "SELECT * FROM t1 LEFT JOIN (FROM t2 |> SELECT id) ON TRUE",
            "SELECT * FROM t1 LEFT JOIN (WITH __tmp1 AS (SELECT id FROM t2) SELECT * FROM __tmp1) ON TRUE",
        )

    def test_order_by(self):
        self.validate_identity("FROM x |> ORDER BY x1", "SELECT * FROM x ORDER BY x1")
        self.validate_identity(
            "FROM x |> ORDER BY x1 |> ORDER BY x2", "SELECT * FROM x ORDER BY x2"
        )
        self.validate_identity(
            "FROM x |> ORDER BY x1 |> WHERE x1 > 0 OR x1 != 1 |> ORDER BY x2 |> WHERE x2 > 0 AND x2 != 1 |> SELECT x1, x2",
            "WITH __tmp1 AS (SELECT x1, x2 FROM x WHERE (x1 > 0 OR x1 <> 1) AND (x2 > 0 AND x2 <> 1) ORDER BY x2) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> ORDER BY x1 |> WHERE x1 > 0 |> SELECT x1",
            "WITH __tmp1 AS (SELECT x1 FROM x WHERE x1 > 0 ORDER BY x1) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> WHERE x1 > 0 |> SELECT x1 |> ORDER BY x1",
            "WITH __tmp1 AS (SELECT x1 FROM x WHERE x1 > 0) SELECT * FROM __tmp1 ORDER BY x1",
        )
        self.validate_identity(
            "FROM x |> SELECT x1, x2, x3 |> ORDER BY x1 DESC NULLS FIRST, x2 ASC NULLS LAST, x3",
            "WITH __tmp1 AS (SELECT x1, x2, x3 FROM x) SELECT * FROM __tmp1 ORDER BY x1 DESC NULLS FIRST, x2 ASC NULLS LAST, x3",
        )

    def test_limit(self):
        for option in ("LIMIT 1", "LIMIT 1 OFFSET 2"):
            with self.subTest(f"Testing pipe syntax LIMIT and OFFSET option: {option}"):
                self.validate_identity(f"FROM x |> {option}", f"SELECT * FROM x {option}")
                self.validate_identity(f"FROM x |> {option}", f"SELECT * FROM x {option}")
                self.validate_identity(
                    f"FROM x |> {option} |> SELECT x1, x2 |> WHERE x1 > 0 |> WHERE x2 > 0  |> ORDER BY x1, x2",
                    f"WITH __tmp1 AS (SELECT x1, x2 FROM x {option}) SELECT * FROM __tmp1 WHERE x1 > 0 AND x2 > 0 ORDER BY x1, x2",
                )
                self.validate_identity(
                    f"FROM x |> SELECT x1, x2 |> WHERE x1 > 0 |> WHERE x2 > 0  |> ORDER BY x1, x2 |> {option}",
                    f"WITH __tmp1 AS (SELECT x1, x2 FROM x) SELECT * FROM __tmp1 WHERE x1 > 0 AND x2 > 0 ORDER BY x1, x2 {option}",
                )
        self.validate_identity(
            "FROM x |> SELECT x1, x2 |> LIMIT 2 |> LIMIT 4",
            "WITH __tmp1 AS (SELECT x1, x2 FROM x) SELECT * FROM __tmp1 LIMIT 2",
        )
        self.validate_identity(
            "FROM x |> SELECT x1, x2 |> LIMIT 2 OFFSET 2 |> LIMIT 4 OFFSET 2",
            "WITH __tmp1 AS (SELECT x1, x2 FROM x) SELECT * FROM __tmp1 LIMIT 2 OFFSET 4",
        )

    def test_aggregate(self):
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1), MAX(x2), MIN(x3)",
            "WITH __tmp1 AS (SELECT SUM(x1), MAX(x2), MIN(x3) FROM x) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1) AS s_x1 |> SELECT s_x1",
            "WITH __tmp1 AS (SELECT SUM(x1) AS s_x1 FROM x), __tmp2 AS (SELECT s_x1 FROM __tmp1) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1), MAX(x2), MIN(x3) GROUP BY x4, x5",
            "WITH __tmp1 AS (SELECT SUM(x1), MAX(x2), MIN(x3), x4, x5 FROM x GROUP BY x4, x5) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1), MAX(x2), MIN(x3) GROUP BY x4 AS a_x4, x5 AS a_x5",
            "WITH __tmp1 AS (SELECT SUM(x1), MAX(x2), MIN(x3), x4 AS a_x4, x5 AS a_x5 FROM x GROUP BY a_x4, a_x5) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1) as s_x1 GROUP BY x1 |> SELECT s_x1, x1 as ss_x1",
            "WITH __tmp1 AS (SELECT SUM(x1) AS s_x1, x1 FROM x GROUP BY x1), __tmp2 AS (SELECT s_x1, x1 AS ss_x1 FROM __tmp1) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1) GROUP",
            "WITH __tmp1 AS (SELECT SUM(x1) AS GROUP FROM x) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> AGGREGATE SUM(x1) as s_x1 GROUP BY x2 as g_x2 |> WHERE s_x1 > 0",
            "WITH __tmp1 AS (SELECT SUM(x1) AS s_x1, x2 AS g_x2 FROM x GROUP BY g_x2) SELECT * FROM __tmp1 WHERE s_x1 > 0",
        )
        for order_option in ("ASC", "DESC", "ASC NULLS LAST", "DESC NULLS FIRST"):
            with self.subTest(f"Testing pipe syntax AGGREGATE for order option: {order_option}"):
                self.validate_all(
                    f"WITH __tmp1 AS (SELECT SUM(x1) AS x_s FROM x ORDER BY x_s {order_option}) SELECT * FROM __tmp1",
                    read={
                        "bigquery": f"FROM x |> AGGREGATE SUM(x1) AS x_s {order_option}",
                    },
                )
                self.validate_all(
                    f"WITH __tmp1 AS (SELECT SUM(x1) AS x_s, x1 AS g_x1 FROM x GROUP BY g_x1 ORDER BY x_s {order_option}) SELECT * FROM __tmp1",
                    read={
                        "bigquery": f"FROM x |> AGGREGATE SUM(x1) AS x_s {order_option} GROUP BY x1 AS g_x1",
                    },
                )
            with self.subTest(
                f"Testing pipe syntax AGGREGATE with GROUP AND ORDER BY for order option: {order_option}"
            ):
                self.validate_all(
                    f"WITH __tmp1 AS (SELECT SUM(x1) AS x_s, x1 AS g_x1 FROM x GROUP BY g_x1 ORDER BY g_x1 {order_option}), __tmp2 AS (SELECT g_x1, x_s FROM __tmp1) SELECT * FROM __tmp2",
                    read={
                        "bigquery": f"FROM x |> AGGREGATE SUM(x1) AS x_s GROUP AND ORDER BY x1 AS g_x1 {order_option} |> SELECT g_x1, x_s",
                    },
                )

    def test_set_operators(self):
        self.validate_identity(
            "FROM x |> SELECT x.x1 |> UNION ALL (SELECT 1 AS c)",
            "WITH __tmp1 AS (SELECT x.x1 FROM x), __tmp2 AS (SELECT * FROM __tmp1), __tmp3 AS (SELECT * FROM __tmp2 UNION ALL SELECT 1 AS c) SELECT * FROM __tmp3",
        )

        for op_operator in (
            "UNION ALL",
            "UNION DISTINCT",
            "INTERSECT DISTINCT",
            "EXCEPT DISTINCT",
        ):
            with self.subTest(f"Testing pipe syntax SET OPERATORS: {op_operator}"):
                self.validate_all(
                    f"FROM x|> {op_operator} (SELECT y1 FROM y), (SELECT z1 FROM z)",
                    write={
                        "bigquery": f"WITH __tmp1 AS (SELECT * FROM x), __tmp2 AS (SELECT * FROM __tmp1 {op_operator} SELECT y1 FROM y {op_operator} SELECT z1 FROM z) SELECT * FROM __tmp2"
                    },
                )

        for op_prefix in ("LEFT OUTER", "FULL OUTER"):
            for op_operator in (
                "UNION ALL",
                "UNION DISTINCT",
                "INTERSECT DISTINCT",
                "EXCEPT DISTINCT",
            ):
                with self.subTest(f"Testing pipe syntax SET OPERATORS: {op_prefix} {op_operator}"):
                    self.validate_all(
                        f"FROM x|> SELECT x1, x2 |> {op_prefix} {op_operator} BY NAME (SELECT y1, y2 FROM y), (SELECT z1, z2 FROM z)",
                        write={
                            "bigquery": f"WITH __tmp1 AS (SELECT x1, x2 FROM x), __tmp2 AS (SELECT * FROM __tmp1), __tmp3 AS (SELECT * FROM __tmp2 {op_prefix} {op_operator} BY NAME SELECT y1, y2 FROM y {op_prefix} {op_operator} BY NAME SELECT z1, z2 FROM z) SELECT * FROM __tmp3",
                        },
                    )

        self.validate_identity(
            "FROM d.x |> SELECT x.x1 |> UNION (SELECT 2 AS a1) |> SELECT x1 |> UNION (SELECT 3 as a2) |> SELECT x1 |> WHERE x1 > 100",
            """WITH __tmp1 AS (
  SELECT
    x.x1
  FROM d.x
), __tmp2 AS (
  SELECT
    *
  FROM __tmp1
), __tmp3 AS (
  SELECT
    *
  FROM __tmp2
  UNION
  SELECT
    2 AS a1
), __tmp4 AS (
  SELECT
    x1
  FROM __tmp3
), __tmp5 AS (
  SELECT
    *
  FROM __tmp4
), __tmp6 AS (
  SELECT
    *
  FROM __tmp5
  UNION
  SELECT
    3 AS a2
), __tmp7 AS (
  SELECT
    x1
  FROM __tmp6
)
SELECT
  *
FROM __tmp7
WHERE
  x1 > 100""",
            pretty=True,
        )
        self.validate_identity(
            "FROM c.x |> UNION ALL (SELECT 2 AS a1, '2' as a2) |> AGGREGATE AVG(x1) as m_x1 |> SELECT * |> UNION ALL (SELECT y1 FROM c.y) |> SELECT m_x1",
            """WITH __tmp1 AS (
  SELECT
    *
  FROM c.x
), __tmp2 AS (
  SELECT
    *
  FROM __tmp1
  UNION ALL
  SELECT
    2 AS a1,
    '2' AS a2
), __tmp3 AS (
  SELECT
    AVG(x1) AS m_x1
  FROM __tmp2
), __tmp4 AS (
  SELECT
    *
  FROM __tmp3
), __tmp5 AS (
  SELECT
    *
  FROM __tmp4
), __tmp6 AS (
  SELECT
    *
  FROM __tmp5
  UNION ALL
  SELECT
    y1
  FROM c.y
), __tmp7 AS (
  SELECT
    m_x1
  FROM __tmp6
)
SELECT
  *
FROM __tmp7""",
            pretty=True,
        )

        self.validate_identity(
            "FROM c.x |> UNION ALL (SELECT 2 AS a1, '2' as a2) |> UNION ALL (SELECT y1 FROM c.y) |> WHERE x > 200",
            """WITH __tmp1 AS (
  SELECT
    *
  FROM c.x
), __tmp2 AS (
  SELECT
    *
  FROM __tmp1
  UNION ALL
  SELECT
    2 AS a1,
    '2' AS a2
), __tmp3 AS (
  SELECT
    *
  FROM __tmp2
), __tmp4 AS (
  SELECT
    *
  FROM __tmp3
  UNION ALL
  SELECT
    y1
  FROM c.y
)
SELECT
  *
FROM __tmp4
WHERE
  x > 200""",
            pretty=True,
        )

    def test_join(self):
        self.validate_identity("FROM x |> CROSS JOIN y", "SELECT * FROM x CROSS JOIN y")
        for join_type in (
            "JOIN",
            "INNER JOIN",
            "FULL JOIN",
            "FULL OUTER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "RIGHT OUTER JOIN",
        ):
            with self.subTest(f"Testing pipe syntax no projecton with JOIN : {join_type}"):
                self.validate_identity(
                    f"FROM x |> {join_type} y ON x.id = y.id",
                    f"SELECT * FROM x {join_type} y ON x.id = y.id",
                )
            with self.subTest(f"Testing pipe syntax projection with JOIN: {join_type}"):
                self.validate_identity(
                    f"FROM x |> SELECT id |> {join_type} y ON x.id = y.id",
                    f"WITH __tmp1 AS (SELECT id FROM x) SELECT * FROM __tmp1 {join_type} y ON x.id = y.id",
                )
            with self.subTest(f"Testing pipe syntax complex queries with JOIN: {join_type}"):
                self.validate_identity(
                    f"FROM x |> {join_type} y ON x.id = y.id |> SELECT x1 as a_x1, x2 |> UNION ALL (SELECT 1, 2) |> WHERE a_x1 > 0",
                    f"""WITH __tmp1 AS (
  SELECT
    x1 AS a_x1,
    x2
  FROM x
  {join_type} y
    ON x.id = y.id
), __tmp2 AS (
  SELECT
    *
  FROM __tmp1
), __tmp3 AS (
  SELECT
    *
  FROM __tmp2
  UNION ALL
  SELECT
    1,
    2
)
SELECT
  *
FROM __tmp3
WHERE
  a_x1 > 0""",
                    pretty=True,
                )

    def test_pivot_unpivot(self):
        self.validate_identity(
            "FROM x |> PIVOT(SUM(x1) FOR quarter IN ('foo1', 'foo2'))",
            "WITH __tmp1 AS (SELECT * FROM x PIVOT(SUM(x1) FOR quarter IN ('foo1', 'foo2'))) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> JOIN y on x.id = y.id |> PIVOT(SUM(x1) FOR quarter IN ('foo1', 'foo2'))",
            "WITH __tmp1 AS (SELECT * FROM x PIVOT(SUM(x1) FOR quarter IN ('foo1', 'foo2')) JOIN y ON x.id = y.id) SELECT * FROM __tmp1",
        )

        self.validate_identity(
            "FROM x |> UNPIVOT(col FOR item IN (foo1, foo2))",
            "WITH __tmp1 AS (SELECT * FROM x UNPIVOT(col FOR item IN (foo1, foo2))) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> JOIN y on x.id = y.id |> UNPIVOT(col FOR item IN (foo1, foo2))",
            "WITH __tmp1 AS (SELECT * FROM x UNPIVOT(col FOR item IN (foo1, foo2)) JOIN y ON x.id = y.id) SELECT * FROM __tmp1",
        )

    def test_as(self):
        self.validate_identity(
            "FROM x |> AS a_x |> WHERE a_x.x1 > 0",
            "WITH a_x AS (SELECT * FROM x) SELECT * FROM a_x WHERE a_x.x1 > 0",
        )
        self.validate_identity(
            "FROM x AS t |> AGGREGATE SUM(x1) AS s_x1 GROUP BY id, x2 |> AS t1 |> JOIN y AS t2 ON t1.id = t2.id |> SELECT t2.id, s_x1",
            "WITH __tmp1 AS (SELECT SUM(x1) AS s_x1, id, x2 FROM x AS t GROUP BY id, x2), t1 AS (SELECT * FROM __tmp1), __tmp2 AS (SELECT t2.id, s_x1 FROM t1 JOIN y AS t2 ON t1.id = t2.id) SELECT * FROM __tmp2",
        )
        self.validate_identity(
            "FROM x |> JOIN y ON x.x1 = y.y1 |> AS a |> WHERE a.x2 > 1",
            "WITH a AS (SELECT * FROM x JOIN y ON x.x1 = y.y1) SELECT * FROM a WHERE a.x2 > 1",
        )

    def test_extend(self):
        self.validate_identity(
            "FROM x |> EXTEND id IN (1, 2) AS is_1_2, id + 1 as a_id",
            "WITH __tmp1 AS (SELECT *, id IN (1, 2) AS is_1_2, id + 1 AS a_id FROM x) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> SELECT x.x1, x.x2 |> EXTEND x1 + 1 as x1_1, x2 + 1 as x2_1 |> WHERE x1_1 > 0 AND x2_1 > 0",
            "WITH __tmp1 AS (SELECT x.x1, x.x2 FROM x), __tmp2 AS (SELECT *, x1 + 1 AS x1_1, x2 + 1 AS x2_1 FROM __tmp1) SELECT * FROM __tmp2 WHERE x1_1 > 0 AND x2_1 > 0",
        )
        self.validate_identity(
            "FROM (SELECT 'foo1' AS item1, 2 AS item2 UNION ALL SELECT 'foo2' AS item1, 5 AS item2) |> EXTEND SUM(item2) OVER() AS item2_sum",
            "SELECT * FROM (WITH __tmp1 AS (SELECT *, SUM(item2) OVER () AS item2_sum FROM (SELECT 'foo1' AS item1, 2 AS item2 UNION ALL SELECT 'foo2' AS item1, 5 AS item2)) SELECT * FROM __tmp1)",
        )

    def test_tablesample(self):
        self.validate_identity(
            "FROM x |> TABLESAMPLE SYSTEM (1 PERCENT)",
            "SELECT * FROM x TABLESAMPLE SYSTEM (1 PERCENT)",
        )
        self.validate_identity(
            "FROM x |> SELECT x.x1 |> TABLESAMPLE SYSTEM (1 PERCENT)",
            "WITH __tmp1 AS (SELECT x.x1 FROM x TABLESAMPLE SYSTEM (1 PERCENT)) SELECT * FROM __tmp1",
        )
        self.validate_identity(
            "FROM x |> TABLESAMPLE SYSTEM (1 PERCENT) |> WHERE x.x1 > 0 |> SELECT x1, x2",
            "WITH __tmp1 AS (SELECT x1, x2 FROM x WHERE x.x1 > 0 TABLESAMPLE SYSTEM (1 PERCENT)) SELECT * FROM __tmp1",
        )
