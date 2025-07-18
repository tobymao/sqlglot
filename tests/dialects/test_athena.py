from sqlglot import exp
from tests.dialects.test_dialect import Validator


class TestAthena(Validator):
    dialect = "athena"
    maxDiff = None

    def test_athena(self):
        self.validate_identity(r"SELECT '\d+'")
        self.validate_identity("SELECT 'foo''bar'")
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

        self.validate_identity(
            "/* leading comment */CREATE SCHEMA foo",
            "/* leading comment */ CREATE SCHEMA `foo`",
            identify=True,
        )
        self.validate_identity(
            "/* leading comment */SELECT * FROM foo",
            '/* leading comment */ SELECT * FROM "foo"',
            identify=True,
        )

    def test_ddl(self):
        # Hive-like, https://docs.aws.amazon.com/athena/latest/ug/create-table.html
        self.validate_identity("CREATE EXTERNAL TABLE foo (id INT) COMMENT 'test comment'")
        self.validate_identity(
            r"CREATE EXTERNAL TABLE george.t (id INT COMMENT 'foo \\ bar') LOCATION 's3://my-bucket/'"
        )
        self.validate_identity(
            r"CREATE EXTERNAL TABLE my_table (id BIGINT COMMENT 'this is the row\'s id') LOCATION 's3://my-s3-bucket'"
        )
        self.validate_identity(
            "CREATE EXTERNAL TABLE foo (id INT, val STRING) CLUSTERED BY (id, val) INTO 10 BUCKETS"
        )
        self.validate_identity(
            "CREATE EXTERNAL TABLE foo (id INT, val STRING) STORED AS PARQUET LOCATION 's3://foo' TBLPROPERTIES ('has_encryped_data'='true', 'classification'='test')"
        )
        self.validate_identity(
            "CREATE EXTERNAL TABLE IF NOT EXISTS foo (a INT, b STRING) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' WITH SERDEPROPERTIES ('case.insensitive'='FALSE') LOCATION 's3://table/path'"
        )
        self.validate_identity(
            """CREATE EXTERNAL TABLE x (y INT) ROW FORMAT SERDE 'serde' ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' WITH SERDEPROPERTIES ('input.regex'='')""",
        )
        self.validate_identity(
            """CREATE EXTERNAL TABLE `my_table` (`a7` ARRAY<DATE>) ROW FORMAT SERDE 'a' STORED AS INPUTFORMAT 'b' OUTPUTFORMAT 'c' LOCATION 'd' TBLPROPERTIES ('e'='f')"""
        )

        # Iceberg, https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html
        self.validate_identity(
            "CREATE TABLE iceberg_table (`id` BIGINT, `data` STRING, category STRING) PARTITIONED BY (category, BUCKET(16, id)) LOCATION 's3://amzn-s3-demo-bucket/your-folder/' TBLPROPERTIES ('table_type'='ICEBERG', 'write_compression'='snappy')"
        )
        self.validate_identity(
            "CREATE OR REPLACE TABLE iceberg_table (`id` BIGINT, `data` STRING, category STRING) PARTITIONED BY (category, BUCKET(16, id)) LOCATION 's3://amzn-s3-demo-bucket/your-folder/' TBLPROPERTIES ('table_type'='ICEBERG', 'write_compression'='snappy')"
        )

        # CTAS goes to the Trino engine, where the table properties cant be encased in single quotes like they can for Hive
        # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
        # They're also case sensitive and need to be lowercase, otherwise you get eg "Table properties [FORMAT] are not supported."
        self.validate_identity(
            "CREATE TABLE foo WITH (table_type='ICEBERG', location='s3://foo/', format='orc', partitioning=ARRAY['bucket(id, 5)']) AS SELECT * FROM a"
        )
        self.validate_identity(
            "CREATE TABLE foo WITH (table_type='HIVE', external_location='s3://foo/', format='parquet', partitioned_by=ARRAY['ds']) AS SELECT * FROM a"
        )
        self.validate_identity(
            "CREATE TABLE foo AS WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo"
        )

        # ALTER TABLE ADD COLUMN not supported, it needs to be generated as ALTER TABLE ADD COLUMNS
        self.validate_identity(
            "ALTER TABLE `foo`.`bar` ADD COLUMN `end_ts` BIGINT",
            "ALTER TABLE `foo`.`bar` ADD COLUMNS (`end_ts` BIGINT)",
        )
        self.validate_identity("ALTER TABLE `foo` DROP COLUMN `id`")

    def test_dml(self):
        self.validate_all(
            "SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES ('2022-01-01')) AS t(ds)",
            read={"": "SELECT CAST(ds AS STRING) AS ds FROM (VALUES ('2022-01-01')) AS t(ds)"},
            write={
                "hive": "SELECT CAST(ds AS STRING) AS ds FROM (VALUES ('2022-01-01')) AS t(ds)",
                "trino": "SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES ('2022-01-01')) AS t(ds)",
                "athena": "SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES ('2022-01-01')) AS t(ds)",
            },
        )

    def test_ddl_quoting(self):
        self.validate_identity("CREATE SCHEMA `foo`")
        self.validate_identity("CREATE SCHEMA foo")

        self.validate_identity("CREATE EXTERNAL TABLE `foo` (`id` INT) LOCATION 's3://foo/'")
        self.validate_identity("CREATE EXTERNAL TABLE foo (id INT) LOCATION 's3://foo/'")
        self.validate_identity(
            "CREATE EXTERNAL TABLE foo (id INT) LOCATION 's3://foo/'",
            "CREATE EXTERNAL TABLE `foo` (`id` INT) LOCATION 's3://foo/'",
            identify=True,
        )

        self.validate_identity("CREATE TABLE foo AS SELECT * FROM a")
        self.validate_identity('CREATE TABLE "foo" AS SELECT * FROM "a"')

        self.validate_identity('DROP VIEW IF EXISTS "foo"."bar"')
        self.validate_identity('CREATE VIEW "foo" AS SELECT "id" FROM "tbl"')
        self.validate_identity(
            "CREATE VIEW foo AS SELECT id FROM tbl",
            'CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
            identify=True,
        )

        self.validate_identity("DROP TABLE `foo`")
        self.validate_identity("DROP TABLE foo")
        self.validate_identity(
            "DROP TABLE foo",
            "DROP TABLE `foo`",
            identify=True,
        )

        self.validate_identity('CREATE VIEW "foo" AS SELECT "id" FROM "tbl"')
        self.validate_identity("CREATE VIEW foo AS SELECT id FROM tbl")
        self.validate_identity(
            "CREATE VIEW foo AS SELECT id FROM tbl",
            'CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
            identify=True,
        )

        # As a side effect of being able to parse both quote types, we can also fix the quoting on incorrectly quoted source queries
        self.validate_identity('CREATE SCHEMA "foo"', "CREATE SCHEMA `foo`")
        self.validate_identity('DROP TABLE "foo"', "DROP TABLE `foo`")
        self.validate_identity(
            "DESCRIBE foo.bar",
            "DESCRIBE `foo`.`bar`",
            identify=True,
        )
        self.validate_identity(
            'CREATE TABLE "foo" AS WITH "foo" AS (SELECT "a", "b" FROM "bar") SELECT * FROM "foo"'
        )

    def test_dml_quoting(self):
        self.validate_identity("SELECT a AS foo FROM tbl")
        self.validate_identity('SELECT "a" AS "foo" FROM "tbl"')

        self.validate_identity("INSERT INTO foo (id) VALUES (1)")
        self.validate_identity('INSERT INTO "foo" ("id") VALUES (1)')

        self.validate_identity("UPDATE foo SET id = 3 WHERE id = 7")
        self.validate_identity('UPDATE "foo" SET "id" = 3 WHERE "id" = 7')

        self.validate_identity("DELETE FROM foo WHERE id > 10")
        self.validate_identity('DELETE FROM "foo" WHERE "id" > 10')

        self.validate_identity("WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo")
        self.validate_identity(
            "WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo",
            'WITH "foo" AS (SELECT "a", "b" FROM "bar") SELECT * FROM "foo"',
            identify=True,
        )

    def test_create_table(self):
        # There are two CREATE TABLE syntaxes
        # Both hit Athena's Hive engine but creating an Iceberg table is different from creating a normal Hive table

        table_schema = exp.Schema(
            this=exp.to_table("foo.bar"),
            expressions=[
                exp.ColumnDef(this=exp.to_identifier("a"), kind=exp.DataType.build("int")),
                exp.ColumnDef(this=exp.to_identifier("b"), kind=exp.DataType.build("varchar")),
            ],
        )

        # Hive tables - CREATE EXTERNAL TABLE
        ct_hive = exp.Create(
            this=table_schema,
            kind="TABLE",
            properties=exp.Properties(
                expressions=[
                    exp.ExternalProperty(),
                    exp.FileFormatProperty(this=exp.Literal.string("parquet")),
                    exp.LocationProperty(this=exp.Literal.string("s3://foo")),
                    exp.PartitionedByProperty(
                        this=exp.Schema(expressions=[exp.to_column("partition_col")])
                    ),
                ]
            ),
        )
        self.assertEqual(
            ct_hive.sql(dialect=self.dialect, identify=True),
            "CREATE EXTERNAL TABLE `foo`.`bar` (`a` INT, `b` STRING) STORED AS PARQUET LOCATION 's3://foo' PARTITIONED BY (`partition_col`)",
        )

        # Iceberg tables - CREATE TABLE... TBLPROPERTIES ('table_type'='iceberg')
        # no EXTERNAL keyword and the 'table_type=iceberg' property must be set
        # ref: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
        ct_iceberg = exp.Create(
            this=table_schema,
            kind="TABLE",
            properties=exp.Properties(
                expressions=[
                    exp.FileFormatProperty(this=exp.Literal.string("parquet")),
                    exp.LocationProperty(this=exp.Literal.string("s3://foo")),
                    exp.PartitionedByProperty(
                        this=exp.Schema(
                            expressions=[
                                exp.to_column("partition_col"),
                                exp.PartitionedByBucket(
                                    this=exp.to_column("a"), expression=exp.Literal.number(4)
                                ),
                            ]
                        )
                    ),
                    exp.Property(this=exp.var("table_type"), value=exp.Literal.string("iceberg")),
                ]
            ),
        )
        self.assertEqual(
            ct_iceberg.sql(dialect=self.dialect, identify=True),
            "CREATE TABLE `foo`.`bar` (`a` INT, `b` STRING) STORED AS PARQUET LOCATION 's3://foo' PARTITIONED BY (`partition_col`, BUCKET(4, `a`)) TBLPROPERTIES ('table_type'='iceberg')",
        )

    def test_ctas(self):
        # Hive tables use 'external_location' to specify the table location, Iceberg tables use 'location' to specify the table location
        # In addition, Hive tables used 'partitioned_by' to specify the partition fields and Iceberg tables use 'partitioning' to specify the partition fields
        # The 'table_type' property is used to determine if it's a Hive or an Iceberg table. If it's omitted, it defaults to Hive
        # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
        ctas_hive = exp.Create(
            this=exp.to_table("foo.bar"),
            kind="TABLE",
            properties=exp.Properties(
                expressions=[
                    exp.FileFormatProperty(this=exp.Literal.string("parquet")),
                    exp.LocationProperty(this=exp.Literal.string("s3://foo")),
                    exp.PartitionedByProperty(
                        this=exp.Schema(expressions=[exp.to_column("partition_col", quoted=True)])
                    ),
                ]
            ),
            expression=exp.select("1"),
        )

        # Even if identify=True, the column names should not be quoted within the string literals in the partitioned_by ARRAY[]
        self.assertEqual(
            ctas_hive.sql(dialect=self.dialect, identify=True),
            "CREATE TABLE \"foo\".\"bar\" WITH (format='parquet', external_location='s3://foo', partitioned_by=ARRAY['partition_col']) AS SELECT 1",
        )
        self.assertEqual(
            ctas_hive.sql(dialect=self.dialect, identify=False),
            "CREATE TABLE foo.bar WITH (format='parquet', external_location='s3://foo', partitioned_by=ARRAY['partition_col']) AS SELECT 1",
        )

        ctas_iceberg = exp.Create(
            this=exp.to_table("foo.bar"),
            kind="TABLE",
            properties=exp.Properties(
                expressions=[
                    exp.Property(this=exp.var("table_type"), value=exp.Literal.string("iceberg")),
                    exp.LocationProperty(this=exp.Literal.string("s3://foo")),
                    exp.PartitionedByProperty(
                        this=exp.Schema(
                            expressions=[
                                exp.to_column("partition_col"),
                                exp.PartitionedByBucket(
                                    this=exp.to_column("a", quoted=True),
                                    expression=exp.Literal.number(4),
                                ),
                            ]
                        )
                    ),
                ]
            ),
            expression=exp.select("1"),
        )
        # Even if identify=True, the column names should not be quoted within the string literals in the partitioning ARRAY[]
        # Technically Trino's Iceberg connector does support quoted column names in the string literals but its undocumented
        # so we dont do it to keep consistency with the Hive connector
        self.assertEqual(
            ctas_iceberg.sql(dialect=self.dialect, identify=True),
            "CREATE TABLE \"foo\".\"bar\" WITH (table_type='iceberg', location='s3://foo', partitioning=ARRAY['partition_col', 'BUCKET(a, 4)']) AS SELECT 1",
        )
        self.assertEqual(
            ctas_iceberg.sql(dialect=self.dialect, identify=False),
            "CREATE TABLE foo.bar WITH (table_type='iceberg', location='s3://foo', partitioning=ARRAY['partition_col', 'BUCKET(a, 4)']) AS SELECT 1",
        )

    def test_parse_partitioned_by_returns_iceberg_transforms(self):
        # check that parse_into works for PartitionedByProperty and also that correct AST nodes are emitted for Iceberg transforms
        parsed = self.parse_one(
            "(a, bucket(4, b), truncate(3, c), month(d))", into=exp.PartitionedByProperty
        )

        assert isinstance(parsed, exp.PartitionedByProperty)
        assert isinstance(parsed.this, exp.Schema)
        assert next(n for n in parsed.this.expressions if isinstance(n, exp.PartitionedByBucket))
        assert next(n for n in parsed.this.expressions if isinstance(n, exp.PartitionByTruncate))
