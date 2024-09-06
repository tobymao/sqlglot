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

    def test_ddl_quoting(self):
        self.validate_identity("CREATE SCHEMA `foo`")
        self.validate_identity("CREATE SCHEMA foo")
        self.validate_identity("CREATE SCHEMA foo", write_sql="CREATE SCHEMA `foo`", identify=True)

        self.validate_identity("CREATE EXTERNAL TABLE `foo` (`id` INTEGER) LOCATION 's3://foo/'")
        self.validate_identity("CREATE EXTERNAL TABLE foo (id INTEGER) LOCATION 's3://foo/'")
        self.validate_identity(
            "CREATE EXTERNAL TABLE foo (id INTEGER) LOCATION 's3://foo/'",
            write_sql="CREATE EXTERNAL TABLE `foo` (`id` INTEGER) LOCATION 's3://foo/'",
            identify=True,
        )

        self.validate_identity("DROP TABLE `foo`")
        self.validate_identity("DROP TABLE foo")
        self.validate_identity("DROP TABLE foo", write_sql="DROP TABLE `foo`", identify=True)

        self.validate_identity('CREATE VIEW "foo" AS SELECT "id" FROM "tbl"')
        self.validate_identity("CREATE VIEW foo AS SELECT id FROM tbl")
        self.validate_identity(
            "CREATE VIEW foo AS SELECT id FROM tbl",
            write_sql='CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
            identify=True,
        )

        # As a side effect of being able to parse both quote types, we can also fix the quoting on incorrectly quoted source queries
        self.validate_identity('CREATE SCHEMA "foo"', write_sql="CREATE SCHEMA `foo`")
        self.validate_identity(
            'CREATE EXTERNAL TABLE "foo" ("id" INTEGER) LOCATION \'s3://foo/\'',
            write_sql="CREATE EXTERNAL TABLE `foo` (`id` INTEGER) LOCATION 's3://foo/'",
        )
        self.validate_identity('DROP TABLE "foo"', write_sql="DROP TABLE `foo`")
        self.validate_identity(
            'CREATE VIEW `foo` AS SELECT "id" FROM `tbl`',
            write_sql='CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
        )

    def test_dml_quoting(self):
        self.validate_identity("SELECT a AS foo FROM tbl")
        self.validate_identity('SELECT "a" AS "foo" FROM "tbl"')
        self.validate_identity(
            'SELECT `a` AS `foo` FROM "tbl"',
            write_sql='SELECT "a" AS "foo" FROM "tbl"',
            identify=True,
        )
