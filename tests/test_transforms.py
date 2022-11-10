import unittest

from sqlglot import parse_one
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.transforms import add_or_dpipe, unalias_group


class TestTime(unittest.TestCase):
    def validate(self, transform, sql, target, schema=None):
        with self.subTest(sql):
            expr = (
                parse_one(sql)
                if not schema
                else annotate_types(expression=parse_one(sql), schema=schema)
            )
            self.assertEqual(expr.transform(transform).sql(), target)

    def test_unalias_group(self):
        self.validate(
            unalias_group,
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, b, x.c, 4",
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, b, x.c, 4",
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
            "SELECT the_date AS the_date, COUNT(*) AS the_count FROM x GROUP BY the_date",
        )

    def test_add_to_dpipe(self):
        schema = {"x": {"cola": "VARCHAR", "colb": "CHAR", "colc": "INT"}}

        self.validate(
            add_or_dpipe,
            "SELECT x.cola + TRIM(x.colb) FROM x",
            "SELECT x.cola || TRIM(x.colb) FROM x",
            schema,
        )
        self.validate(
            add_or_dpipe,
            "SELECT x.colc + x.colc FROM x",
            "SELECT x.colc + x.colc FROM x",
            schema,
        )
        self.validate(
            add_or_dpipe,
            "SELECT ('a') + TRIM('b') FROM x",
            "SELECT ('a') || TRIM('b') FROM x",
            schema,
        )
