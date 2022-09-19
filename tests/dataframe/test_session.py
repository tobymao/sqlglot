import unittest

from sqlglot.dataframe.session import SparkSession


class TestDataframeWindow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession()

    def test_cdf_one_row(self):
        df = self.spark.createDataFrame([[1, 2]], ["cola", "colb"])
        self.assertEqual("SELECT cola, colb FROM (VALUES (1, 2)) AS tab(cola, colb)", df.sql(pretty=False))

    def test_cdf_multiple_rows(self):
        df = self.spark.createDataFrame([[1, 2], [3, 4], [None, 6]], ["cola", "colb"])
        self.assertEqual("SELECT cola, colb FROM (VALUES (1, 2), (3, 4), (NULL, 6)) AS tab(cola, colb)", df.sql(pretty=False))

    def test_cdf_no_schema(self):
        df = self.spark.createDataFrame([[1, 2], [3, 4], [None, 6]])
        self.assertEqual("SELECT _1, _2 FROM (VALUES (1, 2), (3, 4), (NULL, 6)) AS tab(_1, _2)", df.sql(pretty=False))

    def test_cdf_row_mixed_primitives(self):
        df = self.spark.createDataFrame([[1, 10.1, 'test', False, None]])
        self.assertEqual("SELECT _1, _2, _3, _4, _5 FROM (VALUES (1, 10.1, 'test', false, NULL)) AS tab(_1, _2, _3, _4, _5)",
                         df.sql(pretty=False))

    def test_cdf_dict_rows(self):
        df = self.spark.createDataFrame([{"cola": 1, "colb": "test"}, {"cola": 2, "colb": "test2"}])
        self.assertEqual("SELECT cola, colb FROM (VALUES (1, 'test'), (2, 'test2')) AS tab(cola, colb)", df.sql(pretty=False))

    def test_cdf_str_schema(self):
        df = self.spark.createDataFrame([[1, 'test']], "cola: INT, colb: STRING")
        self.assertEqual("SELECT CAST(cola AS INT) AS `cola`, CAST(colb AS STRING) AS `colb` FROM (VALUES (1, 'test')) AS tab(cola, colb)",
                         df.sql(pretty=False))
