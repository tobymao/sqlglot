from tests.dialects.test_dialect import Validator


class TestAthena(Validator):
    dialect = "athena"
    maxDiff = None

    def test_athena(self):
        self.validate_identity(
            """USING EXTERNAL FUNCTION some_function(input VARBINARY)
            RETURNS VARCHAR
                LAMBDA 'some-name'
            SELECT
            some_function(1)""",
            check_command_warning=True,
        )

        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS t (name STRING) LOCATION 's3://bucket/tmp/mytable/' TBLPROPERTIES ('table_type'='iceberg', 'FORMAT'='parquet')"
        )
