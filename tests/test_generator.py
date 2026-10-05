import unittest

from sqlglot import exp, parse_one
from sqlglot.expressions import Expression, Func
from sqlglot.parsers.snowflake import SnowflakeParser
from tests.helpers import is_compiled

import sqlglot.expressions.core as _core_module

_EXPRESSION_IS_COMPILED = is_compiled(_core_module)


class TestGenerator(unittest.TestCase):
    @unittest.skipIf(_EXPRESSION_IS_COMPILED, "mypyc compiled expressions cannot be subclassed")
    def test_fallback_function_sql(self):
        class SpecialUdf(Expression, Func):
            arg_types = {"a": True, "b": False}

        SnowflakeParser.FUNCTIONS["SPECIAL_UDF"] = SpecialUdf.from_arg_list
        try:
            sql = "SELECT SPECIAL_UDF(a) FROM x"
            expression = parse_one(sql, dialect="snowflake")
            self.assertEqual(expression.sql(), "SELECT SPECIAL_UDF(a) FROM x")
        finally:
            del SnowflakeParser.FUNCTIONS["SPECIAL_UDF"]

    @unittest.skipIf(_EXPRESSION_IS_COMPILED, "mypyc compiled expressions cannot be subclassed")
    def test_fallback_function_var_args_sql(self):
        class SpecialUdf(Expression, Func):
            arg_types = {"a": True, "expressions": False}
            is_var_len_args = True

        SnowflakeParser.FUNCTIONS["SPECIAL_UDF"] = SpecialUdf.from_arg_list
        try:
            sql = "SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x"
            expression = parse_one(sql, dialect="snowflake")
            self.assertEqual(expression.sql(), sql)
        finally:
            del SnowflakeParser.FUNCTIONS["SPECIAL_UDF"]

        self.assertEqual(
            exp.DateTrunc(this=exp.to_column("event_date"), unit=exp.var("MONTH")).sql(),
            "DATE_TRUNC('MONTH', event_date)",
        )

    def test_identify(self):
        self.assertEqual(parse_one("x").sql(identify=True), '"x"')
        self.assertEqual(parse_one("x").sql(identify=False), "x")
        self.assertEqual(parse_one("X").sql(identify=True), '"X"')
        self.assertEqual(parse_one('"x"').sql(identify=False), '"x"')
        self.assertEqual(parse_one("x").sql(identify="safe"), '"x"')
        self.assertEqual(parse_one("X").sql(identify="safe"), "X")
        self.assertEqual(parse_one("x as 1").sql(identify="safe"), '"x" AS "1"')
        self.assertEqual(parse_one("X as 1").sql(identify="safe"), 'X AS "1"')

    def test_generate_nested_binary(self):
        sql = "SELECT 'foo'" + (" || 'foo'" * 1000)
        self.assertEqual(parse_one(sql).sql(copy=False), sql)

    def test_overlap_operator(self):
        for op in ("&<", "&>"):
            with self.subTest(op=op):
                input_sql = f"SELECT '[1,10]'::int4range {op} '[5,15]'::int4range"
                expected_sql = (
                    f"SELECT CAST('[1,10]' AS INT4RANGE) {op} CAST('[5,15]' AS INT4RANGE)"
                )
                ast = parse_one(input_sql, read="postgres")
                self.assertEqual(ast.sql(), expected_sql)
                self.assertEqual(ast.sql("postgres"), expected_sql)

    def test_pretty_nested_types(self):
        def assert_pretty_nested(
            datatype: exp.DataType,
            single_line: str,
            pretty: str,
            max_text_width: int = 10,
            **kwargs,
        ) -> None:
            self.assertEqual(datatype.sql(), single_line)
            self.assertEqual(
                datatype.sql(pretty=True, max_text_width=max_text_width, **kwargs), pretty
            )

        # STRUCT
        type_str = "STRUCT<a INT, b TEXT>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<\n  a INT,\n  b TEXT\n>",
        )

        # STRUCT - type def shorter than max text width so stays one line
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<a INT, b TEXT>",
            max_text_width=50,
        )

        # STRUCT, leading_comma = True
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "STRUCT<\n  a INT\n  , b TEXT\n>",
            leading_comma=True,
        )

        # ARRAY
        type_str = "ARRAY<DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "ARRAY<\n  DECIMAL(38, 9)\n>",
        )

        # ARRAY nested STRUCT
        type_str = "ARRAY<STRUCT<a INT, b TEXT>>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "ARRAY<\n  STRUCT<\n    a INT,\n    b TEXT\n  >\n>",
        )

        # RANGE
        type_str = "RANGE<DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "RANGE<\n  DECIMAL(38, 9)\n>",
        )

        # LIST
        type_str = "LIST<INT, INT, TEXT>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "LIST<\n  INT,\n  INT,\n  TEXT\n>",
        )

        # MAP
        type_str = "MAP<INT, DECIMAL(38, 9)>"
        assert_pretty_nested(
            exp.DataType.build(type_str),
            type_str,
            "MAP<\n  INT,\n  DECIMAL(38, 9)\n>",
        )

    def test_setop_grouping_and_precedence(self):
        right = exp.union("SELECT 1", exp.intersect("SELECT 2", "SELECT 3"))
        left = exp.intersect(exp.union("SELECT 1", "SELECT 2"), "SELECT 3")

        for dialect, right_sql, left_sql in (
            (
                "postgres",
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
                "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3",
            ),
            (
                "spark",
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
                "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3",
            ),
            (
                "hive",
                "SELECT 1 UNION (SELECT 2 INTERSECT SELECT 3)",
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
            ),
            (
                "oracle",
                "SELECT 1 UNION (SELECT 2 INTERSECT SELECT 3)",
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
            ),
            (
                "sqlite",
                "SELECT 1 UNION SELECT * FROM (SELECT 2 INTERSECT SELECT 3)",
                "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
            ),
            (
                "bigquery",
                "SELECT 1 UNION DISTINCT (SELECT 2 INTERSECT DISTINCT SELECT 3)",
                "(SELECT 1 UNION DISTINCT SELECT 2) INTERSECT DISTINCT SELECT 3",
            ),
        ):
            with self.subTest(dialect=dialect):
                self.assertEqual(right.sql(dialect), right_sql)
                self.assertEqual(left.sql(dialect), left_sql)

        self.assertEqual(
            exp.union(
                "SELECT 1",
                exp.union(exp.intersect("SELECT 2", "SELECT 3"), "SELECT 4"),
            ).sql("postgres"),
            "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 UNION SELECT 4",
        )
        self.assertEqual(
            exp.union(exp.intersect("SELECT 1", "SELECT 2"), "SELECT 3").sql("postgres"),
            "SELECT 1 INTERSECT SELECT 2 UNION SELECT 3",
        )
        self.assertEqual(
            exp.except_("SELECT 1", exp.intersect("SELECT 2", "SELECT 3")).sql("postgres"),
            "SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3",
        )

    def test_setop_precedence_roundtrip_and_transpilation(self):
        sql = "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2"

        for read, write in (
            ("postgres", "postgres"),
            ("hive", "hive"),
            ("postgres", "spark"),
            ("hive", "oracle"),
        ):
            with self.subTest(read=read, write=write):
                self.assertEqual(parse_one(sql, read=read).sql(dialect=write), sql)

        hive_sql = parse_one(sql, read="postgres").sql(dialect="hive")
        self.assertEqual(hive_sql, "SELECT 1 UNION (SELECT 2 INTERSECT SELECT 2)")
        hive_tree = parse_one(hive_sql, read="hive").assert_is(exp.Union)
        self.assertIsInstance(hive_tree.expression, exp.Subquery)
        self.assertIsInstance(hive_tree.expression.this, exp.Intersect)

        postgres_sql = parse_one(sql, read="hive").sql(dialect="postgres")
        self.assertEqual(postgres_sql, "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 2")
        postgres_tree = parse_one(postgres_sql, read="postgres").assert_is(exp.Intersect)
        self.assertIsInstance(postgres_tree.this, exp.Subquery)
        self.assertIsInstance(postgres_tree.this.this, exp.Union)

    def test_setop_right_operand_associativity(self):
        right_except = exp.except_("SELECT 1", exp.except_("SELECT 2", "SELECT 1"))
        self.assertEqual(right_except.sql("postgres"), "SELECT 1 EXCEPT (SELECT 2 EXCEPT SELECT 1)")
        self.assertIsInstance(
            parse_one(right_except.sql("postgres"), read="postgres").expression,
            exp.Subquery,
        )

        right_mixed = exp.union("SELECT 1", exp.union("SELECT 2", "SELECT 3", distinct=False))
        self.assertEqual(
            right_mixed.sql("postgres"),
            "SELECT 1 UNION (SELECT 2 UNION ALL SELECT 3)",
        )
        self.assertEqual(
            right_mixed.sql("bigquery"),
            "SELECT 1 UNION DISTINCT (SELECT 2 UNION ALL SELECT 3)",
        )
        self.assertEqual(
            exp.union(exp.union("SELECT 1", "SELECT 2", distinct=False), "SELECT 3").sql(
                "postgres"
            ),
            "SELECT 1 UNION ALL SELECT 2 UNION SELECT 3",
        )
        self.assertEqual(
            exp.union(exp.union("SELECT 1", "SELECT 2", distinct=False), "SELECT 3").sql(
                "bigquery"
            ),
            "(SELECT 1 UNION ALL SELECT 2) UNION DISTINCT SELECT 3",
        )
        self.assertEqual(
            exp.union("SELECT 1", exp.union("SELECT 2", "SELECT 3")).sql("postgres"),
            "SELECT 1 UNION SELECT 2 UNION SELECT 3",
        )

        right_all = exp.union(
            "SELECT 2",
            exp.union(
                exp.union("SELECT 3", "SELECT 2", distinct=False),
                "SELECT 2",
                distinct=False,
            ),
            distinct=False,
        )
        self.assertEqual(
            right_all.sql("sqlite"),
            "SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 2 UNION ALL SELECT 2",
        )

    def test_setop_branch_modifiers_and_ctes(self):
        branch = exp.union("SELECT 2", "SELECT 3", distinct=False).limit(1)
        tree = exp.union("SELECT 2", branch, distinct=False).limit(2)
        self.assertEqual(
            tree.sql("tsql"),
            "SELECT TOP 2 * FROM (SELECT 2 AS [2] UNION ALL "
            "(SELECT TOP 1 * FROM (SELECT 2 AS [2] UNION ALL SELECT 3) AS _l_0)) AS _l_0",
        )

        tree = exp.union("SELECT x FROM c", "SELECT 2").with_("c", as_="SELECT 1 AS x")
        tree.set("limit", exp.Fetch(direction="FIRST", count=exp.Literal.number(1)))
        for pretty in (False, True):
            with self.subTest(pretty=pretty):
                sql = tree.sql("clickhouse", pretty=pretty)
                reparsed = parse_one(sql, read="clickhouse")
                self.assertIsInstance(reparsed, exp.Select)
                self.assertIsInstance(reparsed.args.get("limit"), exp.Limit)
                inner = reparsed.args["from_"].this.this
                self.assertIsInstance(inner, exp.Union)
                self.assertIsNotNone(inner.args.get("with_"))
                self.assertIsNone(inner.args.get("limit"))

        branch = exp.union("SELECT x FROM c", "SELECT 2").with_("c", as_="SELECT 1 AS x")
        sql = exp.union("SELECT 0", branch, distinct=False).sql("postgres")
        self.assertEqual(
            sql,
            "SELECT 0 UNION ALL (WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION SELECT 2)",
        )
        nested = parse_one(sql, read="postgres").expression
        self.assertIsInstance(nested, exp.Subquery)
        self.assertIsNotNone(nested.this.args.get("with_"))

        settings = parse_one("SELECT 3 SETTINGS max_threads = 1", read="clickhouse")
        branch = exp.union("SELECT 1", "SELECT 2", distinct=False)
        branch.set("settings", [setting.copy() for setting in settings.args["settings"]])
        self.assertEqual(
            exp.union("SELECT 0", branch, distinct=False).sql("clickhouse"),
            "SELECT 0 UNION ALL (SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1)",
        )

    def test_nested_setop_branch_modifiers(self):
        for key, clause in (
            ("sort", "SORT BY"),
            ("distribute", "DISTRIBUTE BY"),
            ("cluster", "CLUSTER BY"),
        ):
            branch_sql = f"SELECT 1 AS x UNION ALL SELECT 2 AS x {clause} x"
            branch = parse_one(branch_sql, read="spark")
            for left in (False, True):
                with self.subTest(clause=clause, left=left):
                    tree = exp.union(
                        branch.copy() if left else "SELECT 0 AS x",
                        "SELECT 0 AS x" if left else branch.copy(),
                        distinct=False,
                    )
                    expected = (
                        f"({branch_sql}) UNION ALL SELECT 0 AS x"
                        if left
                        else f"SELECT 0 AS x UNION ALL ({branch_sql})"
                    )
                    self.assertEqual(tree.sql("spark"), expected)
                    reparsed = parse_one(expected, read="spark")
                    operand = reparsed.this if left else reparsed.expression
                    self.assertIsInstance(operand, exp.Subquery)
                    self.assertIsNotNone(operand.this.args.get(key))
                    self.assertIsNone(reparsed.args.get(key))

        branch = parse_one("SELECT 1 UNION ALL SELECT 2 SORT BY 1", read="spark")
        self.assertEqual(
            exp.union("SELECT 0", branch, distinct=False).sql("postgres"),
            "SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2",
        )

    def test_deep_setop_generation_preserves_grouping(self):
        tree = exp.select("0")
        for index in range(1, 301):
            operator = exp.Union if index % 2 else exp.Intersect
            tree = operator(this=tree, expression=exp.select(str(index)))

        sql = tree.sql(copy=False)
        self.assertTrue(sql.startswith("("))
        self.assertTrue(sql.endswith("INTERSECT SELECT 300"))

        prefix = tree
        for _ in range(290):
            prefix = prefix.this
        reparsed = parse_one(prefix.sql(copy=False))
        while isinstance(prefix, exp.SetOperation):
            self.assertIs(type(reparsed), type(prefix))
            self.assertEqual(reparsed.expression.sql(), prefix.expression.sql())
            reparsed = reparsed.this.unnest()
            prefix = prefix.this

        for dialect, modifier in (("tsql", "TOP 1"), ("clickhouse", "LIMIT 1")):
            nested = exp.select("1")
            for _ in range(130):
                nested = exp.union("SELECT 2", nested, distinct=False).limit(1)
            sql = nested.sql(dialect, pretty=True, copy=False)
            self.assertEqual(sql.count("UNION ALL"), 130)
            self.assertEqual(sql.count(modifier), 130)
