from tests.dialects.test_dialect import Validator


class TestAthena(Validator):
    dialect = "athena"
    maxDiff = None

    def test_athena(self):
        self.validate_identity(
            "CREATE TABLE IF NOT EXISTS t (name STRING) LOCATION 's3://bucket/tmp/mytable/' TBLPROPERTIES ('table_type'='iceberg', 'FORMAT'='parquet')"
        )
        self.validate_identity(
            "UNLOAD (SELECT name1, address1, comment1, key1 FROM table1) "
            "TO 's3://amzn-s3-demo-bucket/ partitioned/' "
            "WITH (format = 'TEXTFILE', partitioned_by = ARRAY['key1'])",
            check_command_warning=True,
        )
        self.validate_identity(
            """USING EXTERNAL FUNCTION some_function(input VARBINARY)
            RETURNS VARCHAR
                LAMBDA 'some-name'
            SELECT
            some_function(1)""",
            check_command_warning=True,
        )
