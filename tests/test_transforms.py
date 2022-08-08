import unittest

from sqlglot import parse_one
from sqlglot.transforms import unalias_group


class TestTime(unittest.TestCase):
    def validate(self, transform, sql, target):
        self.assertEqual(parse_one(sql).transform(transform).sql(), target)

    def test_unalias_group(self):
        self.validate(
            unalias_group,
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, b, x.c, 4",
            "SELECT a, b AS b, c AS c, 4 FROM x GROUP BY a, 2, x.c, 4",
        )
