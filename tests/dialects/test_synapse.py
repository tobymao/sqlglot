from sqlglot import exp, parse_one
from tests.dialects.test_dialect import Validator


class TestSynapse(Validator):
    dialect = "synapse"

    def test_dialect_registration(self):
        self.validate_identity("SELECT 1")
        self.assertIsInstance(parse_one("SELECT 1", read="synapse"), exp.Select)

    def test_openrowset_options(self):
        expression = self.parse_one(
            "SELECT * FROM OPENROWSET("
            "BULK '/lake-raw/file.parquet', "
            "DATA_SOURCE = 'datasource_x', FORMAT = 'Parquet', "
            "PARSER_VERSION = '2.0', FIRSTROW = 2"
            ") AS source"
        )

        openrowset = expression.find(exp.OpenRowset)
        self.assertIsNotNone(openrowset)
        self.assertEqual(["Property"] * 4, [p.__class__.__name__ for p in openrowset.args["properties"]])
        self.assertEqual(
            "SELECT * FROM OPENROWSET(BULK '/lake-raw/file.parquet', "
            "DATA_SOURCE='datasource_x', FORMAT='Parquet', "
            "PARSER_VERSION='2.0', FIRSTROW=2) AS source",
            expression.sql(dialect=self.dialect),
        )

    def test_openrowset_bulk_list_and_schema(self):
        expression = self.parse_one(
            "SELECT source.[id] FROM OPENROWSET("
            "BULK ('/lake-raw/a.csv', '/lake-raw/b.csv'), "
            "DATA_SOURCE = 'datasource_x', FORMAT = 'CSV', "
            "FIELDTERMINATOR = ','"
            ") WITH ([id] INT, [name] VARCHAR(255)) AS source"
        )

        openrowset = expression.find(exp.OpenRowset)
        self.assertIsNotNone(openrowset)
        self.assertTrue(openrowset.args["bulk_parenthesized"])
        self.assertEqual(2, len(openrowset.args["bulk"]))
        self.assertEqual(2, len(openrowset.args["schema"].expressions))
        self.assertEqual(
            "SELECT source.[id] FROM OPENROWSET(BULK ('/lake-raw/a.csv', '/lake-raw/b.csv'), "
            "DATA_SOURCE='datasource_x', FORMAT='CSV', FIELDTERMINATOR=',') WITH "
            "([id] INTEGER, [name] VARCHAR(255)) AS source",
            expression.sql(dialect=self.dialect),
        )

    def test_openrowset_format_options(self):
        expression = self.parse_one(
            "SELECT * FROM OPENROWSET("
            "BULK 'file.csv', FORMAT = 'CSV', "
            "FORMAT_OPTIONS = (FIELDQUOTE = '\"', FIELDTERMINATOR = ','), "
            "PARSER_VERSION = '2.0'"
            ") AS source"
        )

        format_options = expression.find(exp.FormatOptionsProperty)
        self.assertIsNotNone(format_options)
        self.assertTrue(format_options.args["equals"])
        self.assertEqual(
            "SELECT * FROM OPENROWSET(BULK 'file.csv', FORMAT='CSV', "
            "FORMAT_OPTIONS = (FIELDQUOTE='\"', FIELDTERMINATOR=','), "
            "PARSER_VERSION='2.0') AS source",
            expression.sql(dialect=self.dialect),
        )

    def test_openrowset_output_alias(self):
        expression = self.parse_one(
            "SELECT output.filepath() FROM OPENROWSET("
            "BULK 'file.csv', FORMAT = 'CSV'"
            ") WITH (filepath VARCHAR(255)) AS output"
        )

        self.assertEqual(
            "SELECT output.filepath() FROM OPENROWSET(BULK 'file.csv', FORMAT='CSV') "
            "WITH (filepath VARCHAR(255)) AS output",
            expression.sql(dialect=self.dialect),
        )

    def test_try_parse(self):
        expression = self.parse_one(
            "SELECT TRY_PARSE(NULLIF(value, 'nan') AS DATETIME USING 'en-GB') AS parsed "
            "FROM source"
        )

        try_parse = expression.find(exp.TryParse)
        self.assertIsNotNone(try_parse)
        self.assertEqual("en-GB", try_parse.args["culture"].name)
        self.assertEqual(
            "SELECT TRY_PARSE(NULLIF(value, 'nan') AS DATETIME USING 'en-GB') AS parsed FROM source",
            expression.sql(dialect=self.dialect),
        )

    def test_external_file_format(self):
        expression = self.parse_one(
            "CREATE EXTERNAL FILE FORMAT textdelimitedpipe WITH ("
            "FORMAT_TYPE = DELIMITEDTEXT, "
            "FORMAT_OPTIONS (FIELD_TERMINATOR = '|')"
            ")"
        )

        self.assertIsInstance(expression, exp.Create)
        self.assertEqual("FILE FORMAT", expression.args["kind"])
        self.assertIsInstance(expression.this, exp.Table)
        self.assertIsNotNone(expression.find(exp.FormatOptionsProperty))
        self.assertEqual(
            "CREATE EXTERNAL FILE FORMAT textdelimitedpipe WITH "
            "(FORMAT_TYPE=DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR='|'))",
            expression.sql(dialect=self.dialect),
        )

    def test_external_data_source(self):
        expression = self.parse_one(
            "CREATE EXTERNAL DATA SOURCE [datasource_x] WITH ("
            "LOCATION = N'https://x.dfs.core.windows.net/'"
            ")"
        )

        self.assertIsInstance(expression, exp.Create)
        self.assertEqual("DATA SOURCE", expression.args["kind"])
        self.assertIsNotNone(expression.find(exp.LocationProperty))
        self.assertEqual(
            "CREATE EXTERNAL DATA SOURCE [datasource_x] WITH "
            "(LOCATION=N'https://x.dfs.core.windows.net/')",
            expression.sql(dialect=self.dialect),
        )

    def test_external_table_properties(self):
        expression = self.parse_one(
            "CREATE EXTERNAL TABLE [wqdataportal].[annual_conc_ds] ("
            "[Site Code] VARCHAR(100), [Sample Reference] INT"
            ") WITH (DATA_SOURCE = [datasource_x], "
            "LOCATION = N'lake-curated/wqdataportal/annual_conc_ds/', "
            "FILE_FORMAT = [parquet])"
        )

        self.assertIsInstance(expression, exp.Create)
        self.assertIsInstance(expression.this, exp.Schema)
        self.assertEqual(2, len(expression.this.expressions))
        self.assertEqual(
            "CREATE EXTERNAL TABLE [wqdataportal].[annual_conc_ds] "
            "([Site Code] VARCHAR(100), [Sample Reference] INTEGER) WITH "
            "(DATA_SOURCE=datasource_x, LOCATION=N'lake-curated/wqdataportal/annual_conc_ds/', "
            "FILE_FORMAT=parquet)",
            expression.sql(dialect=self.dialect),
        )

    def test_external_table_inside_conditional_block(self):
        expression = self.parse_one(
            "IF OBJECT_ID('wqdataportal.annual_conc_ds', 'ET') IS NULL "
            "BEGIN CREATE EXTERNAL TABLE [wqdataportal].[annual_conc_ds] "
            "([Site Code] VARCHAR(100)) WITH (DATA_SOURCE = [datasource_x]) END"
        )

        self.assertIsNotNone(expression.find(exp.Create))
        self.assertEqual(
            "CREATE EXTERNAL TABLE [wqdataportal].[annual_conc_ds] "
            "([Site Code] VARCHAR(100)) WITH (DATA_SOURCE=datasource_x)",
            expression.find(exp.Create).sql(dialect=self.dialect),
        )
