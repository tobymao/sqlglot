import unittest

from sqlglot import expressions as exp
from sqlglot.dataframe.sql.dataframe import DataFrame


class TestDataframeColumn(unittest.TestCase):
    def test_hash_select_expression(self):
        expression = exp.select("cola").from_("table")
        self.assertEqual("t17051", DataFrame._create_hash_from_expression(expression))
