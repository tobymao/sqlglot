import unittest

import sqlglot
from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql import types
from sqlglot.dataframe.sql.session import SparkSession


class TestDataframeSession(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.spark = SparkSession()

    def test_cdf_one_row(self):
        df = self.spark.createDataFrame([[1, 2]], ["cola", "colb"])
        self.assertEqual(
            "SELECT `a1`.`cola` AS `cola`, `a1`.`colb` AS `colb` FROM (VALUES (1, 2)) AS `a1`(`cola`, `colb`)",
            df.sql(pretty=False),
        )

    def test_cdf_multiple_rows(self):
        df = self.spark.createDataFrame([[1, 2], [3, 4], [None, 6]], ["cola", "colb"])
        self.assertEqual(
            "SELECT `a1`.`cola` AS `cola`, `a1`.`colb` AS `colb` FROM (VALUES (1, 2), (3, 4), (NULL, 6)) AS `a1`(`cola`, `colb`)",
            df.sql(pretty=False),
        )

    def test_cdf_no_schema(self):
        df = self.spark.createDataFrame([[1, 2], [3, 4], [None, 6]])
        self.assertEqual(
            "SELECT `a1`.`_1` AS `_1`, `a1`.`_2` AS `_2` FROM (VALUES (1, 2), (3, 4), (NULL, 6)) AS `a1`(`_1`, `_2`)",
            df.sql(pretty=False),
        )

    def test_cdf_row_mixed_primitives(self):
        df = self.spark.createDataFrame([[1, 10.1, "test", False, None]])
        self.assertEqual(
            "SELECT `a1`.`_1` AS `_1`, `a1`.`_2` AS `_2`, `a1`.`_3` AS `_3`, `a1`.`_4` AS `_4`, `a1`.`_5` AS `_5` FROM (VALUES (1, 10.1, 'test', false, NULL)) AS `a1`(`_1`, `_2`, `_3`, `_4`, `_5`)",
            df.sql(pretty=False),
        )

    def test_cdf_dict_rows(self):
        df = self.spark.createDataFrame([{"cola": 1, "colb": "test"}, {"cola": 2, "colb": "test2"}])
        self.assertEqual(
            "SELECT `a1`.`cola` AS `cola`, `a1`.`colb` AS `colb` FROM (VALUES (1, 'test'), (2, 'test2')) AS `a1`(`cola`, `colb`)",
            df.sql(pretty=False),
        )

    def test_cdf_str_schema(self):
        df = self.spark.createDataFrame([[1, "test"]], "cola: INT, colb: STRING")
        self.assertEqual(
            "SELECT CAST(`a1`.`cola` AS INT) AS `cola`, CAST(`a1`.`colb` AS STRING) AS `colb` FROM (VALUES (1, 'test')) AS `a1`(`cola`, `colb`)",
            df.sql(pretty=False),
        )

    def test_typed_schema_basic(self):
        schema = types.StructType(
            [
                types.StructField("cola", types.IntegerType()),
                types.StructField("colb", types.StringType()),
            ]
        )
        df = self.spark.createDataFrame([[1, "test"]], schema)
        self.assertEqual(
            "SELECT CAST(`a1`.`cola` AS int) AS `cola`, CAST(`a1`.`colb` AS string) AS `colb` FROM (VALUES (1, 'test')) AS `a1`(`cola`, `colb`)",
            df.sql(pretty=False),
        )

    def test_typed_schema_nested(self):
        schema = types.StructType(
            [
                types.StructField(
                    "cola",
                    types.StructType(
                        [
                            types.StructField("sub_cola", types.IntegerType()),
                            types.StructField("sub_colb", types.StringType()),
                        ]
                    ),
                )
            ]
        )
        df = self.spark.createDataFrame([[{"sub_cola": 1, "sub_colb": "test"}]], schema)
        self.assertEqual(
            "SELECT CAST(`a1`.`cola` AS struct<sub_cola:int, sub_colb:string>) AS `cola` FROM (VALUES (STRUCT(1 AS `sub_cola`, 'test' AS `sub_colb`))) AS `a1`(`cola`)",
            df.sql(pretty=False),
        )

    def test_sql_select_only(self):
        # TODO: Do exact matches once CTE names are deterministic
        query = "SELECT cola, colb FROM table"
        df = self.spark.sql(query)
        sqlglot.schema.add_table("table", {"cola": "string", "colb": "string"})
        self.assertIn(
            "SELECT `table`.`cola` AS `cola`, `table`.`colb` AS `colb` FROM `table` AS `table`", df.sql(pretty=False)
        )

    def test_sql_with_aggs(self):
        # TODO: Do exact matches once CTE names are deterministic
        query = "SELECT cola, colb FROM table"
        sqlglot.schema.add_table("table", {"cola": "string", "colb": "string"})
        df = self.spark.sql(query).groupBy(F.col("cola")).agg(F.sum("colb"))
        result = df.sql(pretty=False, optimize=False)
        self.assertIn("SELECT cola, colb FROM table", result)
        self.assertIn("SUM(colb)", result)
        self.assertIn("GROUP BY cola", result)

    def test_sql_non_select(self):
        query = "CREATE TABLE new_table AS SELECT cola, colb FROM table"
        df = self.spark.sql(query)
        sqlglot.schema.add_table("table", {"cola": "string", "colb": "string"})
        self.assertEqual("CREATE TABLE new_table AS SELECT cola, colb FROM table", df.sql(pretty=False, optimize=False))

    def test_session_create_builder_patterns(self):
        spark = SparkSession()
        self.assertEqual(spark.builder.appName("abc").getOrCreate(), spark)
