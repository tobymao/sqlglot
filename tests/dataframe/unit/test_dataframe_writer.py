import unittest

from sqlglot.dataframe.session import SparkSession
from sqlglot.dataframe import types


class TestDataFrameWriter(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.df = SparkSession().createDataFrame(
            [[1, 2, 'test']],
            types.StructType([
                types.StructField("cola", types.IntegerType()),
                types.StructField("colb", types.IntegerType()),
                types.StructField("colc", types.StringType()),
            ])
        )

    def test_insertInto_full_path(self):
        self.assertEqual(
            "INSERT INTO catalog.db.table_name SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.insertInto("catalog.db.table_name").sql(pretty=False)
        )

    def test_insertInto_db_table(self):
        self.assertEqual(
            "INSERT INTO db.table_name SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.insertInto("db.table_name").sql(pretty=False)
        )

    def test_insertInto_table(self):
        self.assertEqual(
            "INSERT INTO table_name SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.insertInto("table_name").sql(pretty=False)
        )

    def test_insertInto_overwrite(self):
        self.assertEqual(
            "INSERT OVERWRITE TABLE table_name SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.insertInto("table_name", overwrite=True).sql(pretty=False)
        )

    def test_saveAsTable_format(self):
        with self.assertRaises(NotImplementedError):
            self.df.write.saveAsTable("table_name", format="parquet").sql(pretty=False)

    def test_saveAsTable_append(self):
        self.assertEqual(
            "INSERT INTO table_name SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.saveAsTable("table_name", mode="append").sql(pretty=False)
        )

    def test_saveAsTable_overwrite(self):
        self.assertEqual(
            "CREATE OR REPLACE TABLE table_name AS SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.saveAsTable("table_name", mode="overwrite").sql(pretty=False),
        )

    def test_saveAsTable_error(self):
        self.assertEqual(
            "CREATE TABLE table_name AS SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.saveAsTable("table_name", mode="error").sql(pretty=False),
        )

    def test_saveAsTable_error(self):
        self.assertEqual(
            "CREATE TABLE IF NOT EXISTS table_name AS SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.saveAsTable("table_name", mode="ignore").sql(pretty=False),
        )

    def test_mode_standalone(self):
        self.assertEqual(
            "CREATE TABLE IF NOT EXISTS table_name AS SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.mode("ignore").saveAsTable("table_name").sql(pretty=False),
        )

    def test_mode_override(self):
        self.assertEqual(
            "CREATE OR REPLACE TABLE table_name AS SELECT CAST(cola AS int) AS `cola`, CAST(colb AS int) AS `colb`, CAST(colc AS string) AS `colc` FROM (VALUES (1, 2, 'test')) AS tab(cola, colb, colc)",
            self.df.write.mode("ignore").saveAsTable("table_name", mode="overwrite").sql(pretty=False),
        )