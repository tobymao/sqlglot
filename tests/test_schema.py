import unittest

from sqlglot import exp, parse_one, to_table
from sqlglot.errors import SchemaError
from sqlglot.schema import MappingSchema, ensure_schema


class TestSchema(unittest.TestCase):
    def assert_column_names(self, schema, *table_results):
        for table, result in table_results:
            with self.subTest(f"{table} -> {result}"):
                self.assertEqual(schema.column_names(to_table(table)), result)

    def assert_column_names_raises(self, schema, *tables):
        for table in tables:
            with self.subTest(table):
                with self.assertRaises(SchemaError):
                    schema.column_names(to_table(table))

    def assert_column_names_empty(self, schema, *tables):
        for table in tables:
            with self.subTest(table):
                self.assertEqual(schema.column_names(to_table(table)), [])

    def test_schema(self):
        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                },
                "y": {
                    "b": "uint64",
                    "c": "uint64",
                },
            },
        )

        self.assert_column_names(
            schema,
            ("x", ["a"]),
            ("y", ["b", "c"]),
            ("z.x", ["a"]),
            ("z.x.y", ["b", "c"]),
        )

        self.assert_column_names_empty(
            schema,
            "z",
            "z.z",
            "z.z.z",
        )

    def test_schema_db(self):
        schema = ensure_schema(
            {
                "d1": {
                    "x": {
                        "a": "uint64",
                    },
                    "y": {
                        "b": "uint64",
                    },
                },
                "d2": {
                    "x": {
                        "c": "uint64",
                    },
                },
            },
        )

        self.assert_column_names(
            schema,
            ("d1.x", ["a"]),
            ("d2.x", ["c"]),
            ("y", ["b"]),
            ("d1.y", ["b"]),
            ("z.d1.y", ["b"]),
        )

        self.assert_column_names_raises(
            schema,
            "x",
        )

        self.assert_column_names_empty(
            schema,
            "z.x",
            "z.y",
        )

    def test_schema_catalog(self):
        schema = ensure_schema(
            {
                "c1": {
                    "d1": {
                        "x": {
                            "a": "uint64",
                        },
                        "y": {
                            "b": "uint64",
                        },
                        "z": {
                            "c": "uint64",
                        },
                    },
                },
                "c2": {
                    "d1": {
                        "y": {
                            "d": "uint64",
                        },
                        "z": {
                            "e": "uint64",
                        },
                    },
                    "d2": {
                        "z": {
                            "f": "uint64",
                        },
                    },
                },
            }
        )

        self.assert_column_names(
            schema,
            ("x", ["a"]),
            ("d1.x", ["a"]),
            ("c1.d1.x", ["a"]),
            ("c1.d1.y", ["b"]),
            ("c1.d1.z", ["c"]),
            ("c2.d1.y", ["d"]),
            ("c2.d1.z", ["e"]),
            ("d2.z", ["f"]),
            ("c2.d2.z", ["f"]),
        )

        self.assert_column_names_raises(
            schema,
            "y",
            "z",
            "d1.y",
            "d1.z",
        )

        self.assert_column_names_empty(
            schema,
            "q",
            "d2.x",
            "a.b.c",
        )

    def test_schema_add_table_with_and_without_mapping(self):
        schema = MappingSchema()
        schema.add_table("test")
        self.assertEqual(schema.column_names("test"), [])
        schema.add_table("test", {"x": "string"})
        self.assertEqual(schema.column_names("test"), ["x"])
        schema.add_table("test", {"x": "string", "y": "int"})
        self.assertEqual(schema.column_names("test"), ["x", "y"])
        schema.add_table("test")
        self.assertEqual(schema.column_names("test"), ["x", "y"])

    def test_schema_get_column_type(self):
        schema = MappingSchema({"A": {"b": "varchar"}})
        self.assertEqual(schema.get_column_type("a", "B").this, exp.DataType.Type.VARCHAR)
        self.assertEqual(
            schema.get_column_type(exp.Table(this="a"), exp.Column(this="b")).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type("a", exp.Column(this="b")).this, exp.DataType.Type.VARCHAR
        )
        self.assertEqual(
            schema.get_column_type(exp.Table(this="a"), "b").this, exp.DataType.Type.VARCHAR
        )
        schema = MappingSchema({"a": {"b": {"c": "varchar"}}})
        self.assertEqual(
            schema.get_column_type(exp.Table(this="b", db="a"), exp.Column(this="c")).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type(exp.Table(this="b", db="a"), "c").this, exp.DataType.Type.VARCHAR
        )
        schema = MappingSchema({"a": {"b": {"c": {"d": "varchar"}}}})
        self.assertEqual(
            schema.get_column_type(
                exp.Table(this="c", db="b", catalog="a"), exp.Column(this="d")
            ).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type(exp.Table(this="c", db="b", catalog="a"), "d").this,
            exp.DataType.Type.VARCHAR,
        )

        schema = MappingSchema({"foo": {"bar": parse_one("INT", into=exp.DataType)}})
        self.assertEqual(schema.get_column_type("foo", "bar").this, exp.DataType.Type.INT)

    def test_schema_normalization(self):
        schema = MappingSchema(
            schema={"x": {"`y`": {"Z": {"a": "INT", "`B`": "VARCHAR"}, "w": {"C": "INT"}}}},
            dialect="clickhouse",
        )

        table_z = exp.Table(this="z", db="y", catalog="x")
        table_w = exp.Table(this="w", db="y", catalog="x")

        self.assertEqual(schema.column_names(table_z), ["a", "B"])
        self.assertEqual(schema.column_names(table_w), ["c"])

        schema = MappingSchema(schema={"x": {"`y`": "INT"}}, dialect="clickhouse")
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["y"])

        # Check that add_table normalizes both the table and the column names to be added / updated
        schema = MappingSchema()
        schema.add_table("Foo", {"SomeColumn": "INT", '"SomeColumn"': "DOUBLE"})
        self.assertEqual(schema.column_names(exp.Table(this="fOO")), ["somecolumn", "SomeColumn"])

        # Check that names are normalized to uppercase for Snowflake
        schema = MappingSchema(schema={"x": {"foo": "int", '"bLa"': "int"}}, dialect="snowflake")
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["FOO", "bLa"])

        # Check that switching off the normalization logic works as expected
        schema = MappingSchema(schema={"x": {"foo": "int"}}, normalize=False, dialect="snowflake")
        self.assertEqual(schema.column_names(exp.Table(this="x")), ["foo"])

        # Check that the correct dialect is used when calling schema methods
        # Note: T-SQL is case-insensitive by default, so `fo` in clickhouse will match the normalized table name
        schema = MappingSchema(schema={"[Fo]": {"x": "int"}}, dialect="tsql")
        self.assertEqual(
            schema.column_names("[Fo]"), schema.column_names("`fo`", dialect="clickhouse")
        )

        # Check that all column identifiers are normalized to lowercase for BigQuery, even quoted
        # ones. Also, ensure that tables aren't normalized, since they're case-sensitive by default.
        schema = MappingSchema(schema={"Foo": {"`BaR`": "int"}}, dialect="bigquery")
        self.assertEqual(schema.column_names("Foo"), ["bar"])
        self.assertEqual(schema.column_names("foo"), [])

        # Check that the schema's normalization setting can be overridden
        schema = MappingSchema(schema={"X": {"y": "int"}}, normalize=False, dialect="snowflake")
        self.assertEqual(schema.column_names("x", normalize=True), ["y"])
