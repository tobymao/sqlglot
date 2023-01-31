import unittest

from sqlglot import parse_one
from sqlglot.transforms import (
    eliminate_distinct_on,
    remove_precision_parameterized_types,
    unalias_group,
)


class TestTime(unittest.TestCase):
    maxDiff = None

    def validate(self, transform, sql, target):
        with self.subTest(sql):
            self.assertEqual(parse_one(sql).transform(transform).sql(), target)

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
            'SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) WHERE "_row_number" = 1',
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (a) a, b FROM x",
            'SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a) AS _row_number FROM x) WHERE "_row_number" = 1',
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (a, b) a, b FROM x ORDER BY c DESC",
            'SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c DESC) AS _row_number FROM x) WHERE "_row_number" = 1',
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT a, b FROM x ORDER BY c DESC",
            "SELECT DISTINCT a, b FROM x ORDER BY c DESC",
        )
        self.validate(
            eliminate_distinct_on,
            "SELECT DISTINCT ON (_row_number) _row_number FROM x ORDER BY c DESC",
            'SELECT _row_number FROM (SELECT _row_number, ROW_NUMBER() OVER (PARTITION BY _row_number ORDER BY c DESC) AS _row_number_2 FROM x) WHERE "_row_number_2" = 1',
        )

    def test_remove_precision_parameterized_types(self):
        self.validate(
            remove_precision_parameterized_types,
            "SELECT CAST(1 AS DECIMAL(10, 2)), CAST('13' AS VARCHAR(10))",
            "SELECT CAST(1 AS DECIMAL), CAST('13' AS VARCHAR)",
        )
