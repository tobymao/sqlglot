import sqlglot
from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.errors import OptimizeError
from tests.dataframe.unit.dataframe_test_base import DataFrameTestBase


class TestSessionCaseSensitivity(DataFrameTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.spark = SparkSession.builder.config("sqlframe.dialect", "snowflake").getOrCreate()

    tests = [
        (
            "All lower no intention of CS",
            "test",
            "test",
            {"name": "VARCHAR"},
            "name",
            '''SELECT "TEST"."NAME" AS "NAME" FROM "TEST" AS "TEST"''',
        ),
        (
            "Table has CS while column does not",
            '"Test"',
            '"Test"',
            {"name": "VARCHAR"},
            "name",
            '''SELECT "Test"."NAME" AS "NAME" FROM "Test" AS "Test"''',
        ),
        (
            "Column has CS while table does not",
            "test",
            "test",
            {'"Name"': "VARCHAR"},
            '"Name"',
            '''SELECT "TEST"."Name" AS "Name" FROM "TEST" AS "TEST"''',
        ),
        (
            "Both Table and column have CS",
            '"Test"',
            '"Test"',
            {'"Name"': "VARCHAR"},
            '"Name"',
            '''SELECT "Test"."Name" AS "Name" FROM "Test" AS "Test"''',
        ),
        (
            "Lowercase CS table and column",
            '"test"',
            '"test"',
            {'"name"': "VARCHAR"},
            '"name"',
            '''SELECT "test"."name" AS "name" FROM "test" AS "test"''',
        ),
        (
            "CS table and column and query table but no CS in query column",
            '"test"',
            '"test"',
            {'"name"': "VARCHAR"},
            "name",
            OptimizeError(),
        ),
        (
            "CS table and column and query column but no CS in query table",
            '"test"',
            "test",
            {'"name"': "VARCHAR"},
            '"name"',
            OptimizeError(),
        ),
    ]

    def test_basic_case_sensitivity(self):
        for test_name, table_name, spark_table, schema, spark_column, expected in self.tests:
            with self.subTest(test_name):
                sqlglot.schema.add_table(table_name, schema, dialect=self.spark.dialect)
                df = self.spark.table(spark_table).select(F.col(spark_column))
                if isinstance(expected, OptimizeError):
                    with self.assertRaises(OptimizeError):
                        df.sql()
                else:
                    self.compare_sql(df, expected)

    def test_alias(self):
        col = F.col('"Name"')
        self.assertEqual(col.sql(dialect=self.spark.dialect), '"Name"')
        self.assertEqual(col.alias("nAME").sql(dialect=self.spark.dialect), '"Name" AS NAME')
        self.assertEqual(col.alias('"nAME"').sql(dialect=self.spark.dialect), '"Name" AS "nAME"')
