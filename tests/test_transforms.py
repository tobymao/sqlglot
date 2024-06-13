import unittest

from sqlglot import parse_one
from sqlglot.transforms import (
    eliminate_distinct_on,
    eliminate_join_marks,
    eliminate_qualify,
    remove_precision_parameterized_types,
    unalias_group,
)


class TestTransforms(unittest.TestCase):
    maxDiff = None

    def validate(self, transform, sql, target, dialect=None):
        with self.subTest(f"{dialect} - {sql}"):
            self.assertEqual(
                parse_one(sql, dialect=dialect).transform(transform).sql(dialect=dialect), target
            )

    def test_unalias_group(self):
        self.validate(
            unalias_group,
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, b, x.c, 4",
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, 2, x.c, 4",
        )
        self.validate(
            unalias_group,
            "SELECT TO_DATE(the_date) AS the_date, CUSTOM_UDF(other_col) AS other_col, last_col AS aliased_last, COUNT(*) AS the_count FROM x GROUP BY TO_DATE(the_date), CUSTOM_UDF(other_col), aliased_last",
            "SELECT TO_DATE(the_date) AS the_date, CUSTOM_UDF(other_col) AS other_col, last_col AS aliased_last, COUNT(*) AS the_count FROM x GROUP BY TO_DATE(the_date), CUSTOM_UDF(other_col), 3",
        )
        self.validate(
            unalias_group,
            "SELECT SOME_UDF(TO_DATE(the_date)) AS the_date, COUNT(*) AS the_count FROM x GROUP BY SOME_UDF(TO_DATE(the_date))",
            "SELECT SOME_UDF(TO_DATE(the_date)) AS the_date, COUNT(*) AS the_count FROM x GROUP BY SOME_UDF(TO_DATE(the_date))",
        )
        self.validate(
            unalias_group,
            "SELECT SOME_UDF(TO_DATE(the_date)) AS new_date, COUNT(*) AS the_count FROM x GROUP BY new_date",
            "SELECT SOME_UDF(TO_DATE(the_date)) AS new_date, COUNT(*) AS the_count FROM x GROUP BY 1",
        )
        self.validate(
            unalias_group,
            "SELECT the_date AS the_date, COUNT(*) AS the_count FROM x GROUP BY the_date",
            "SELECT the_date AS the_date, COUNT(*) AS the_count FROM x GROUP BY 1",
        )
        self.validate(
            unalias_group,
            "SELECT a AS a FROM x GROUP BY DATE(a)",
            "SELECT a AS a FROM x GROUP BY DATE(a)",
        )

    def test_eliminate_distinct_on(self):
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (a) a, b FROM x ORDER BY c DESC",
            "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (a) a, b FROM x",
            "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY a) AS _row_number FROM x) AS _t WHERE _row_number = 1",
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (a, b) a, b FROM x ORDER BY c DESC",
            "SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1",
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT a, b FROM x ORDER BY c DESC",
            "SELECT DISTINCT a, b FROM x ORDER BY c DESC",
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (_row_number) _row_number FROM x ORDER BY c DESC",
            "SELECT _row_number FROM (SELECT _row_number, ROW_NUMBER() OVER (PARTITION BY _row_number ORDER BY c DESC) AS _row_number_2 FROM x) AS _t WHERE _row_number_2 = 1",
        )

    def test_eliminate_qualify(self):
        self.validate(
            eliminate_qualify,
            "SELECT i, a + 1 FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p) = 1",
            "SELECT i, _c FROM (SELECT i, a + 1 AS _c, ROW_NUMBER() OVER (PARTITION BY p) AS _w, p FROM qt) AS _t WHERE _w = 1",
        )
        self.validate(
            eliminate_qualify,
            "SELECT i FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1 AND p = 0",
            "SELECT i FROM (SELECT i, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w, p, o FROM qt) AS _t WHERE _w = 1 AND p = 0",
        )
        self.validate(
            eliminate_qualify,
            "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1",
            "SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1",
        )
        self.validate(
            eliminate_qualify,
            "SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt QUALIFY row_num = 1",
            "SELECT i, p, o, row_num FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt) AS _t WHERE row_num = 1",
        )
        self.validate(
            eliminate_qualify,
            "SELECT * FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1",
            "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1",
        )
        self.validate(
            eliminate_qualify,
            "SELECT c2, SUM(c3) OVER (PARTITION BY c2) AS r FROM t1 WHERE c3 < 4 GROUP BY c2, c3 HAVING SUM(c1) > 3 QUALIFY r IN (SELECT MIN(c1) FROM test GROUP BY c2 HAVING MIN(c1) > 3)",
            "SELECT c2, r FROM (SELECT c2, SUM(c3) OVER (PARTITION BY c2) AS r, c1 FROM t1 WHERE c3 < 4 GROUP BY c2, c3 HAVING SUM(c1) > 3) AS _t WHERE r IN (SELECT MIN(c1) FROM test GROUP BY c2 HAVING MIN(c1) > 3)",
        )
        self.validate(
            eliminate_qualify,
            "SELECT x FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY p)",
            "SELECT x FROM (SELECT x, ROW_NUMBER() OVER (PARTITION BY p) AS _w, p FROM y) AS _t WHERE _w",
        )
        self.validate(
            eliminate_qualify,
            "SELECT x AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY z)",
            "SELECT z FROM (SELECT x AS z, ROW_NUMBER() OVER (PARTITION BY x) AS _w, x FROM y) AS _t WHERE _w",
        )
        self.validate(
            eliminate_qualify,
            "SELECT SOME_UDF(x) AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY z)",
            "SELECT z FROM (SELECT SOME_UDF(x) AS z, ROW_NUMBER() OVER (PARTITION BY x ORDER BY SOME_UDF(x)) AS _w, x FROM y) AS _t WHERE _w",
        )
        self.validate(
            eliminate_qualify,
            "SELECT x, t, x || t AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY z DESC)",
            "SELECT x, t, z FROM (SELECT x, t, x || t AS z, ROW_NUMBER() OVER (PARTITION BY x ORDER BY x || t DESC) AS _w FROM y) AS _t WHERE _w",
        )
        self.validate(
            eliminate_qualify,
            "SELECT y.x AS x, y.t AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY x DESC, z)",
            "SELECT x, z FROM (SELECT y.x AS x, y.t AS z, ROW_NUMBER() OVER (PARTITION BY y.x ORDER BY y.x DESC, y.t) AS _w, y.t FROM y) AS _t WHERE _w",
        )

    def test_remove_precision_parameterized_types(self):
        self.validate(
            remove_precision_parameterized_types,
            "SELECT CAST(1 AS DECIMAL(10, 2)), CAST('13' AS VARCHAR(10))",
            "SELECT CAST(1 AS DECIMAL), CAST('13' AS VARCHAR)",
        )

    def test_eliminate_join_marks(self):
        for dialect in ("oracle", "redshift"):
            self.validate(
                eliminate_join_marks,
                "SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y (+) > 5",
                "SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x AND T2.y > 5",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x (+) = T2.x and T2.y > 5",
                "SELECT T1.d, T2.c FROM T2 LEFT JOIN T1 ON T1.x = T2.x WHERE T2.y > 5",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y (+) IS NULL",
                "SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x AND T2.y IS NULL",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y IS NULL",
                "SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x WHERE T2.y IS NULL",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T1.Z > 4",
                "SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x WHERE T1.Z > 4",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT * FROM table1, table2 WHERE table1.col = table2.col(+)",
                "SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT * FROM table1, table2, table3, table4 WHERE table1.col = table2.col(+) and table2.col >= table3.col(+) and table1.col = table4.col(+)",
                "SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col LEFT JOIN table3 ON table2.col >= table3.col LEFT JOIN table4 ON table1.col = table4.col",
                dialect,
            )
            self.validate(
                eliminate_join_marks,
                "SELECT * FROM table1, table2, table3 WHERE table1.col = table2.col(+) and table2.col >= table3.col(+)",
                "SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col LEFT JOIN table3 ON table2.col >= table3.col",
                dialect,
            )
            # 2 join marks on one side of predicate
            self.validate(
                eliminate_join_marks,
                "SELECT * FROM table1, table2 WHERE table1.col = table2.col1(+) + table2.col2(+)",
                "SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col1 + table2.col2",
                dialect,
            )
            # join mark and expression
            self.validate(
                eliminate_join_marks,
                "SELECT * FROM table1, table2 WHERE table1.col = table2.col1(+) + 25",
                "SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col1 + 25",
                dialect,
            )

            alias = "AS " if dialect != "oracle" else ""
            self.validate(
                eliminate_join_marks,
                "SELECT table1.id, table2.cloumn1, table3.id FROM table1, table2, (SELECT tableInner1.id FROM tableInner1, tableInner2 WHERE tableInner1.id = tableInner2.id(+)) AS table3 WHERE table1.id = table2.id(+) and table1.id = table3.id(+)",
                f"SELECT table1.id, table2.cloumn1, table3.id FROM table1 LEFT JOIN table2 ON table1.id = table2.id LEFT JOIN (SELECT tableInner1.id FROM tableInner1 LEFT JOIN tableInner2 ON tableInner1.id = tableInner2.id) {alias}table3 ON table1.id = table3.id",
                dialect,
            )
