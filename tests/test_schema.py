import gc
import unittest
import weakref

from sqlglot import exp, parse_one, to_table
from sqlglot.errors import SchemaError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.schema import MappingSchema, Schema, ensure_schema


class TestSchema(unittest.TestCase):
    def test_ref_type(self) -> None:
        schema = MappingSchema({"orders": {"customer": "REF(public.customer)"}})
        datatype = exp.DataType.build("REF(public.customer)")
        self.assertEqual(schema.get_column_type(exp.to_table("orders"), "customer"), datatype)

        expression = annotate_types(
            qualify(
                parse_one(
                    "SELECT customer, ARRAY(customer), COALESCE(customer, NULL), "
                    "CAST(customer AS REF(public.customer)) FROM orders"
                ),
                schema=schema,
            ),
            schema=schema,
        )
        self.assertEqual(expression.selects[0].type, datatype)
        self.assertEqual(
            expression.selects[1].type, exp.DataType.build("ARRAY<REF(public.customer)>")
        )
        self.assertEqual(expression.selects[2].type, datatype)
        self.assertEqual(expression.selects[3].type, exp.DataType.build('REF("public"."customer")'))
        # The target inside REF is type metadata, not another source in the query.
        self.assertEqual(set(build_scope(expression).sources), {"orders"})

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

    def test_schema_add_table_invalidates_partial_lookups(self):
        schema = MappingSchema({"db": {"t1": {"a": "int"}}})
        self.assertEqual(schema.column_names(to_table("t1")), ["a"])

        schema.add_table("db.t1", {"a": "int", "b": "int"})
        self.assertEqual(schema.column_names(to_table("t1")), ["a", "b"])

        schema.add_table("db2.t1", {"c": "int"})
        with self.assertRaises(SchemaError):
            schema.column_names(to_table("t1"))

    def test_schema_get_column_type(self):
        schema = MappingSchema({"A": {"b": "varchar"}})
        self.assertEqual(schema.get_column_type("a", "B").this, exp.DataType.Type.VARCHAR)
        self.assertEqual(
            schema.get_column_type(exp.table_("a"), exp.column("b")).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type("a", exp.column("b")).this, exp.DataType.Type.VARCHAR
        )
        self.assertEqual(
            schema.get_column_type(exp.table_("a"), "b").this, exp.DataType.Type.VARCHAR
        )
        schema = MappingSchema({"a": {"b": {"c": "varchar"}}})
        self.assertEqual(
            schema.get_column_type(exp.table_("b", db="a"), exp.column("c")).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type(exp.table_("b", db="a"), "c").this, exp.DataType.Type.VARCHAR
        )
        schema = MappingSchema({"a": {"b": {"c": {"d": "varchar"}}}})
        self.assertEqual(
            schema.get_column_type(exp.table_("c", db="b", catalog="a"), exp.column("d")).this,
            exp.DataType.Type.VARCHAR,
        )
        self.assertEqual(
            schema.get_column_type(exp.table_("c", db="b", catalog="a"), "d").this,
            exp.DataType.Type.VARCHAR,
        )

        schema = MappingSchema({"foo": {"bar": parse_one("INT", into=exp.DataType)}})
        self.assertEqual(schema.get_column_type("foo", "bar").this, exp.DataType.Type.INT)

    def test_schema_normalization(self):
        schema = MappingSchema(
            schema={"x": {"`y`": {"Z": {"a": "INT", "`B`": "VARCHAR"}, "w": {"C": "INT"}}}},
            dialect="clickhouse",
        )

        table_z = exp.table_("Z", db="y", catalog="x")
        table_w = exp.table_("w", db="y", catalog="x")

        self.assertEqual(schema.column_names(table_z), ["a", "B"])
        self.assertEqual(schema.column_names(table_w), ["C"])

        schema = MappingSchema(schema={"x": {"`y`": "INT"}}, dialect="clickhouse")
        self.assertEqual(schema.column_names(exp.table_("x")), ["y"])

        # Check that add_table normalizes both the table and the column names to be added / updated
        schema = MappingSchema()
        schema.add_table("Foo", {"SomeColumn": "INT", '"SomeColumn"': "DOUBLE"})
        self.assertEqual(schema.column_names(exp.table_("fOO")), ["somecolumn", "SomeColumn"])

        # Check that names are normalized to uppercase for Snowflake
        schema = MappingSchema(schema={"x": {"foo": "int", '"bLa"': "int"}}, dialect="snowflake")
        self.assertEqual(schema.column_names(exp.table_("x")), ["FOO", "bLa"])

        # Check that switching off the normalization logic works as expected
        schema = MappingSchema(schema={"x": {"foo": "int"}}, normalize=False, dialect="snowflake")
        self.assertEqual(schema.column_names(exp.table_("x")), ["foo"])

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

        # Check that identifiers in nested data types are normalized
        # BigQuery: STRUCT field names should be lowercased
        schema = MappingSchema({"t": {"col": "STRUCT<FooBar INT>"}}, dialect="bigquery")
        col_type = schema.get_column_type("t", "col")
        self.assertEqual(col_type.expressions[0].name, "foobar")

        # Snowflake: STRUCT field names should be uppercased
        schema = MappingSchema({"t": {"col": "STRUCT<FooBar INT>"}}, dialect="snowflake")
        col_type = schema.get_column_type("T", "COL")
        self.assertEqual(col_type.expressions[0].name, "FOOBAR")

        # ClickHouse: STRUCT field names should preserve case (case-sensitive)
        schema = MappingSchema({"t": {"col": "STRUCT<FooBar INT>"}}, dialect="clickhouse")
        col_type = schema.get_column_type("t", "col")
        self.assertEqual(col_type.expressions[0].name, "FooBar")

        # Nested STRUCT field names should also be normalized
        schema = MappingSchema(
            {"t": {"col": "STRUCT<Outer STRUCT<Inner INT>>"}}, dialect="bigquery"
        )
        col_type = schema.get_column_type("t", "col")
        self.assertEqual(col_type.expressions[0].name, "outer")
        self.assertEqual(col_type.expressions[0].kind.expressions[0].name, "inner")

        # ARRAY of STRUCT field names should also be normalized
        schema = MappingSchema({"t": {"col": "ARRAY<STRUCT<FooBar INT>>"}}, dialect="bigquery")
        col_type = schema.get_column_type("t", "col")
        struct_type = col_type.expressions[0]
        self.assertEqual(struct_type.expressions[0].name, "foobar")

    def test_same_number_of_qualifiers(self):
        schema = MappingSchema({"x": {"y": {"c1": "int"}}})

        with self.assertRaises(SchemaError) as ctx:
            schema.add_table("z", {"c2": "int"})

        self.assertEqual(
            str(ctx.exception),
            "Table z must match the schema's nesting level: 2.",
        )

        schema = MappingSchema()
        schema.add_table("x.y", {"c1": "int"})

        with self.assertRaises(SchemaError) as ctx:
            schema.add_table("z", {"c2": "int"})

        self.assertEqual(
            str(ctx.exception),
            "Table z must match the schema's nesting level: 2.",
        )

        with self.assertRaises(SchemaError) as ctx:
            MappingSchema({"x": {"y": {"c1": "int"}}, "z": {"c2": "int"}})

        self.assertEqual(
            str(ctx.exception),
            "Table z must match the schema's nesting level: 2.",
        )

        with self.assertRaises(SchemaError) as ctx:
            MappingSchema(
                {
                    "catalog": {
                        "db": {"tbl": {"col": "a"}},
                    },
                    "tbl2": {"col": "b"},
                },
            )
        self.assertEqual(
            str(ctx.exception),
            "Table tbl2 must match the schema's nesting level: 3.",
        )

        with self.assertRaises(SchemaError) as ctx:
            MappingSchema(
                {
                    "tbl2": {"col": "b"},
                    "catalog": {
                        "db": {"tbl": {"col": "a"}},
                    },
                },
            )
        self.assertEqual(
            str(ctx.exception),
            "Table catalog.db.tbl must match the schema's nesting level: 1.",
        )

    def test_schema_add_table_error_formatting(self):
        for dialect, read, sql, expected in (
            ("postgres", None, '"foo.bar"', '"foo.bar"'),
            ("bigquery", None, "`foo bar`", "`foo bar`"),
            ("tsql", None, "[foo.bar]", "[foo.bar]"),
            ("snowflake", None, "foo", "FOO"),
            ("tsql", "postgres", '"foo.bar"', "[foo.bar]"),
        ):
            schema = MappingSchema({"db": {"items": {"id": "INT"}}}, dialect=dialect)
            expression = to_table(sql, dialect=read or dialect)
            for table in (sql, expression):
                with self.subTest(dialect=dialect, read=read, table=table):
                    with self.assertRaises(SchemaError) as ctx:
                        schema.add_table(table, {"id": "INT"}, dialect=read)

                    self.assertEqual(
                        str(ctx.exception),
                        f"Table {expected} must match the schema's nesting level: 2.",
                    )

            self.assertEqual(expression.sql(read or dialect), sql)

    def test_has_column(self):
        schema = MappingSchema({"x": {"c": "int"}})
        self.assertTrue(schema.has_column("x", exp.column("c")))
        self.assertFalse(schema.has_column("x", exp.column("k")))

    def test_find(self):
        schema = MappingSchema({"x": {"c": "int"}})
        found = schema.find(exp.to_table("x"))
        self.assertEqual(found, {"c": "int"})
        found = schema.find(exp.to_table("x"), ensure_data_types=True)
        self.assertEqual(found, {"c": exp.DataType.build("int")})

    def test_cache_does_not_retain_queries(self):
        class Marker:
            pass

        for normalize in (False, True):
            for method, args in (
                ("find", ()),
                ("column_names", ()),
                ("get_column_type", ("id",)),
                ("has_column", ("id",)),
                ("add_table", ({"id": "INT"},)),
            ):
                with self.subTest(normalize=normalize, method=method):
                    schema = MappingSchema(
                        {"items": {"id": "INT"}}, dialect="duckdb", normalize=normalize
                    )
                    query = parse_one("SELECT id FROM items", dialect="duckdb")
                    table = query.find(exp.Table)
                    marker = Marker()
                    # Compiled expressions cannot be weakly referenced directly.
                    query.meta["marker"] = table.meta["marker"] = marker
                    marker_ref = weakref.ref(marker)

                    getattr(schema, method)(table, *args)
                    del query, table, marker
                    gc.collect()

                    self.assertIsNone(marker_ref())
                    self.assertEqual(schema.column_names("items"), ["id"])

    def test_cache_ignores_aliases(self):
        schema = MappingSchema({"items": {"id": "INT"}})
        for alias in ("a", "b"):
            table = exp.table_("items", alias=alias)
            self.assertEqual(schema.column_names(table), ["id"])
            self.assertEqual(schema.find(table), {"id": "INT"})

        self.assertEqual(len(schema._find_cache), 1)
        self.assertEqual(len(schema._normalized_table_cache), 1)

    def test_cache_normalization(self):
        schema = MappingSchema(
            {
                "foo": {"lower": "INT"},
                "Foo": {"mixed": "INT"},
                "FOO": {"upper": "INT"},
            },
            dialect="postgres",
            normalize=False,
        )
        table = exp.table_("Foo")
        for dialect, normalize, expected in (
            ("postgres", True, "lower"),
            ("postgres", False, "mixed"),
            ("snowflake", True, "upper"),
            ("bigquery", True, "mixed"),
        ):
            with self.subTest(dialect=dialect, normalize=normalize):
                self.assertEqual(
                    schema.column_names(table, dialect=dialect, normalize=normalize), [expected]
                )
                self.assertEqual(
                    schema.column_names("Foo", dialect=dialect, normalize=normalize), [expected]
                )
        self.assertEqual(table.sql(), "Foo")

        table.this.set("quoted", True)
        self.assertEqual(schema.column_names(table, normalize=True), ["mixed"])
        table.this.set("quoted", False)
        self.assertEqual(schema.column_names(table, normalize=True), ["lower"])
        self.assertEqual(schema.column_names(to_table("Foo()"), normalize=True), ["mixed"])

        self.assertEqual(schema.find(exp.table_("Foo")), {"mixed": "INT"})
        self.assertEqual(schema.find(exp.table_("foo")), {"lower": "INT"})
        self.assertEqual(schema.find(exp.table_("FOO")), {"upper": "INT"})

    def test_cache_table_mutation(self):
        schema = MappingSchema(
            {
                "db": {"items": {"id": "INT"}, "other": {"value": "TEXT"}},
                "other_db": {"items": {"other_id": "INT"}},
            }
        )
        table = to_table("db.items")
        self.assertEqual(schema.column_names(table), ["id"])
        table.this.set("this", "other")
        self.assertEqual(schema.column_names(table), ["value"])
        table.this.set("this", "items")
        table.args["db"].set("this", "other_db")
        self.assertEqual(schema.column_names(table), ["other_id"])
        table.args["db"].set("this", "db")
        self.assertEqual(schema.column_names(table), ["id"])

    def test_custom_schema(self):
        class CustomSchema(Schema):
            def add_table(
                self, table, column_mapping=None, dialect=None, normalize=None, match_depth=True
            ):
                pass

            def column_names(self, table, only_visible=False, dialect=None, normalize=None):
                return ["a"]

            def get_column_type(self, table, column, dialect=None, normalize=None):
                return exp.DataType.build("int")

            @property
            def supported_table_args(self):
                return ("this",)

            @property
            def empty(self):
                return False

        schema = CustomSchema()
        expression = annotate_types(
            qualify(parse_one("SELECT * FROM t"), schema=schema), schema=schema
        )
        self.assertEqual(expression.sql(), 'SELECT "t"."a" AS "a" FROM "t" AS "t"')
        self.assertEqual(expression.selects[0].type.sql(), "INT")
