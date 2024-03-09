import datetime
import math
import unittest

from sqlglot import ParseError, alias, exp, parse_one


class TestExpressions(unittest.TestCase):
    maxDiff = None

    def test_arg_key(self):
        self.assertEqual(parse_one("sum(1)").find(exp.Literal).arg_key, "this")

    def test_depth(self):
        self.assertEqual(parse_one("x(1)").find(exp.Literal).depth, 1)

    def test_iter(self):
        self.assertEqual([exp.Literal.number(1), exp.Literal.number(2)], list(parse_one("[1, 2]")))

        with self.assertRaises(TypeError):
            for x in parse_one("1"):
                pass

    def test_eq(self):
        query = parse_one("SELECT x FROM t")
        self.assertEqual(query, query.copy())

        self.assertNotEqual(exp.to_identifier("a"), exp.to_identifier("A"))

        self.assertEqual(
            exp.Column(table=exp.to_identifier("b"), this=exp.to_identifier("b")),
            exp.Column(this=exp.to_identifier("b"), table=exp.to_identifier("b")),
        )

        self.assertNotEqual(exp.to_identifier("a", quoted=True), exp.to_identifier("A"))
        self.assertNotEqual(exp.to_identifier("A", quoted=True), exp.to_identifier("A"))
        self.assertNotEqual(
            exp.to_identifier("A", quoted=True), exp.to_identifier("a", quoted=True)
        )
        self.assertNotEqual(parse_one("'x'"), parse_one("'X'"))
        self.assertNotEqual(parse_one("'1'"), parse_one("1"))
        self.assertEqual(parse_one("`a`", read="hive"), parse_one('"a"'))
        self.assertEqual(parse_one("`a`", read="hive"), parse_one('"a"  '))
        self.assertEqual(parse_one("`a`.`b`", read="hive"), parse_one('"a"."b"'))
        self.assertEqual(parse_one("select a, b+1"), parse_one("SELECT a, b + 1"))
        self.assertNotEqual(parse_one("`a`.`b`.`c`", read="hive"), parse_one("a.b.c"))
        self.assertNotEqual(parse_one("a.b.c.d", read="hive"), parse_one("a.b.c"))
        self.assertEqual(parse_one("a.b.c.d", read="hive"), parse_one("a.b.c.d"))
        self.assertEqual(parse_one("a + b * c - 1.0"), parse_one("a+b*c-1.0"))
        self.assertNotEqual(parse_one("a + b * c - 1.0"), parse_one("a + b * c + 1.0"))
        self.assertEqual(parse_one("a as b"), parse_one("a AS b"))
        self.assertNotEqual(parse_one("a as b"), parse_one("a"))
        self.assertEqual(
            parse_one("ROW() OVER(Partition by y)"),
            parse_one("ROW() OVER (partition BY y)"),
        )
        self.assertEqual(parse_one("TO_DATE(x)", read="hive"), parse_one("ts_or_ds_to_date(x)"))
        self.assertEqual(exp.Table(pivots=[]), exp.Table())
        self.assertNotEqual(exp.Table(pivots=[None]), exp.Table())
        self.assertEqual(
            exp.DataType.build("int"), exp.DataType(this=exp.DataType.Type.INT, nested=False)
        )

    def test_find(self):
        expression = parse_one("CREATE TABLE x STORED AS PARQUET AS SELECT * FROM y")
        self.assertTrue(expression.find(exp.Create))
        self.assertFalse(expression.find(exp.Group))
        self.assertEqual(
            [table.name for table in expression.find_all(exp.Table)],
            ["x", "y"],
        )

    def test_find_all(self):
        expression = parse_one(
            """
            SELECT *
            FROM (
                SELECT b.*
                FROM a.b b
            ) x
            JOIN (
              SELECT c.foo
              FROM a.c c
              WHERE foo = 1
            ) y
              ON x.c = y.foo
            CROSS JOIN (
              SELECT *
              FROM (
                SELECT d.bar
                FROM d
              ) nested
            ) z
              ON x.c = y.foo
            """
        )

        self.assertEqual(
            [table.name for table in expression.find_all(exp.Table)],
            ["b", "c", "d"],
        )

        expression = parse_one("select a + b + c + d")

        self.assertEqual(
            [column.name for column in expression.find_all(exp.Column)],
            ["d", "c", "a", "b"],
        )
        self.assertEqual(
            [column.name for column in expression.find_all(exp.Column, bfs=False)],
            ["a", "b", "c", "d"],
        )

    def test_find_ancestor(self):
        column = parse_one("select * from foo where (a + 1 > 2)").find(exp.Column)
        self.assertIsInstance(column, exp.Column)
        self.assertIsInstance(column.parent_select, exp.Select)
        self.assertIsNone(column.find_ancestor(exp.Join))

    def test_to_dot(self):
        orig = parse_one('a.b.c."d".e.f')
        self.assertEqual(".".join(str(p) for p in orig.parts), 'a.b.c."d".e.f')

        self.assertEqual(
            ".".join(
                str(p)
                for p in exp.Dot.build(
                    [
                        exp.to_table("a.b.c"),
                        exp.to_identifier("d"),
                        exp.to_identifier("e"),
                        exp.to_identifier("f"),
                    ]
                ).parts
            ),
            "a.b.c.d.e.f",
        )

        self.assertEqual(".".join(str(p) for p in orig.parts), 'a.b.c."d".e.f')
        column = orig.find(exp.Column)
        dot = column.to_dot()

        self.assertEqual(dot.sql(), 'a.b.c."d".e.f')

        self.assertEqual(
            dot,
            exp.Dot(
                this=exp.Dot(
                    this=exp.Dot(
                        this=exp.Dot(
                            this=exp.Dot(
                                this=exp.to_identifier("a"),
                                expression=exp.to_identifier("b"),
                            ),
                            expression=exp.to_identifier("c"),
                        ),
                        expression=exp.to_identifier("d", quoted=True),
                    ),
                    expression=exp.to_identifier("e"),
                ),
                expression=exp.to_identifier("f"),
            ),
        )

    def test_root(self):
        ast = parse_one("select * from (select a from x)")
        self.assertIs(ast, ast.root())
        self.assertIs(ast, ast.find(exp.Column).root())

    def test_alias_or_name(self):
        expression = parse_one(
            "SELECT a, b AS B, c + d AS e, *, 'zz', 'zz' AS z FROM foo as bar, baz"
        )
        self.assertEqual(
            [e.alias_or_name for e in expression.expressions],
            ["a", "B", "e", "*", "zz", "z"],
        )
        self.assertEqual(
            {e.alias_or_name for e in expression.find_all(exp.Table)},
            {"bar", "baz"},
        )

        expression = parse_one(
            """
            WITH first AS (SELECT * FROM foo),
                 second AS (SELECT * FROM bar)
            SELECT * FROM first, second, (SELECT * FROM baz) AS third
        """
        )

        self.assertEqual(
            [e.alias_or_name for e in expression.args["with"].expressions],
            ["first", "second"],
        )

        self.assertEqual("first", expression.args["from"].alias_or_name)
        self.assertEqual(
            [e.alias_or_name for e in expression.args["joins"]],
            ["second", "third"],
        )

        self.assertEqual(parse_one("x.*").name, "*")
        self.assertEqual(parse_one("NULL").name, "NULL")
        self.assertEqual(parse_one("a.b.c").name, "c")

    def test_table_name(self):
        bq_dashed_table = exp.to_table("a-1.b.c", dialect="bigquery")
        self.assertEqual(exp.table_name(bq_dashed_table), '"a-1".b.c')
        self.assertEqual(exp.table_name(bq_dashed_table, dialect="bigquery"), "`a-1`.b.c")
        self.assertEqual(exp.table_name("a-1.b.c", dialect="bigquery"), "`a-1`.b.c")
        self.assertEqual(exp.table_name(parse_one("a", into=exp.Table)), "a")
        self.assertEqual(exp.table_name(parse_one("a.b", into=exp.Table)), "a.b")
        self.assertEqual(exp.table_name(parse_one("a.b.c", into=exp.Table)), "a.b.c")
        self.assertEqual(exp.table_name("a.b.c"), "a.b.c")
        self.assertEqual(exp.table_name(exp.to_table("a.b.c.d.e", dialect="bigquery")), "a.b.c.d.e")
        self.assertEqual(exp.table_name(exp.to_table("'@foo'", dialect="snowflake")), "'@foo'")
        self.assertEqual(exp.table_name(exp.to_table("@foo", dialect="snowflake")), "@foo")
        self.assertEqual(
            exp.table_name(parse_one("foo.`{bar,er}`", read="databricks"), dialect="databricks"),
            "foo.`{bar,er}`",
        )

        self.assertEqual(exp.table_name(bq_dashed_table, identify=True), '"a-1"."b"."c"')

    def test_table(self):
        self.assertEqual(exp.table_("a", alias="b"), parse_one("select * from a b").find(exp.Table))
        self.assertEqual(exp.table_("a", "").sql(), "a")
        self.assertEqual(exp.Table(db=exp.to_identifier("a")).sql(), "a")

    def test_replace_tables(self):
        self.assertEqual(
            exp.replace_tables(
                parse_one(
                    'select * from a AS a, b, c.a, d.a cross join e.a cross join "f-F"."A" cross join G'
                ),
                {
                    "a": "a1",
                    "b": "b.a",
                    "c.a": "c.a2",
                    "d.a": "d2",
                    "`f-F`.`A`": '"F"',
                    "g": "g1.a",
                },
                dialect="bigquery",
            ).sql(),
            'SELECT * FROM a1 AS a /* a */, b.a /* b */, c.a2 /* c.a */, d2 /* d.a */ CROSS JOIN e.a CROSS JOIN "F" /* f-F.A */ CROSS JOIN g1.a /* g */',
        )

        self.assertEqual(
            exp.replace_tables(
                parse_one("select * from example.table", dialect="bigquery"),
                {"example.table": "`my-project.example.table`"},
                dialect="bigquery",
            ).sql(),
            'SELECT * FROM "my-project"."example"."table" /* example.table */',
        )

    def test_expand(self):
        self.assertEqual(
            exp.expand(
                parse_one('select * from "a-b"."C" AS a'),
                {
                    "`a-b`.`c`": parse_one("select 1"),
                },
                dialect="spark",
            ).sql(),
            "SELECT * FROM (SELECT 1) AS a /* source: a-b.c */",
        )

    def test_replace_placeholders(self):
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from :tbl1 JOIN :tbl2 ON :col1 = :str1 WHERE :col2 > :int1"),
                tbl1=exp.to_identifier("foo"),
                tbl2=exp.to_identifier("bar"),
                col1=exp.to_identifier("a"),
                col2=exp.to_identifier("c"),
                str1="b",
                int1=100,
            ).sql(),
            "SELECT * FROM foo JOIN bar ON a = 'b' WHERE c > 100",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from ? JOIN ? ON ? = ? WHERE ? = 'bla'"),
                exp.to_identifier("foo"),
                exp.to_identifier("bar"),
                exp.to_identifier("a"),
                "b",
                "bla",
            ).sql(),
            "SELECT * FROM foo JOIN bar ON a = 'b' WHERE 'bla' = 'bla'",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from ? WHERE ? > 100"),
                exp.to_identifier("foo"),
            ).sql(),
            "SELECT * FROM foo WHERE ? > 100",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from :name WHERE ? > 100"), another_name="bla"
            ).sql(),
            "SELECT * FROM :name WHERE ? > 100",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from (SELECT :col1 FROM ?) WHERE :col2 > ?"),
                exp.to_identifier("tbl1"),
                100,
                "tbl3",
                col1=exp.to_identifier("a"),
                col2=exp.to_identifier("b"),
                col3="c",
            ).sql(),
            "SELECT * FROM (SELECT a FROM tbl1) WHERE b > 100",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from foo WHERE x > ? AND y IS ?"), 0, False
            ).sql(),
            "SELECT * FROM foo WHERE x > 0 AND y IS FALSE",
        )
        self.assertEqual(
            exp.replace_placeholders(
                parse_one("select * from foo WHERE x > :int1 AND y IS :bool1"), int1=0, bool1=False
            ).sql(),
            "SELECT * FROM foo WHERE x > 0 AND y IS FALSE",
        )

    def test_function_building(self):
        self.assertEqual(exp.func("max", 1).sql(), "MAX(1)")
        self.assertEqual(exp.func("max", 1, 2).sql(), "MAX(1, 2)")
        self.assertEqual(exp.func("bla", 1, "foo").sql(), "BLA(1, foo)")
        self.assertEqual(exp.func("COUNT", exp.Star()).sql(), "COUNT(*)")
        self.assertEqual(exp.func("bloo").sql(), "BLOO()")
        self.assertEqual(exp.func("concat", exp.convert("a")).sql("duckdb"), "CONCAT('a')")
        self.assertEqual(
            exp.func("locate", "'x'", "'xo'", dialect="hive").sql("hive"), "LOCATE('x', 'xo')"
        )
        self.assertEqual(
            exp.func("log", exp.to_identifier("x"), 2, dialect="bigquery").sql("bigquery"),
            "LOG(x, 2)",
        )
        self.assertEqual(
            exp.func("log", dialect="bigquery", expression="x", this=2).sql("bigquery"),
            "LOG(x, 2)",
        )

        self.assertIsInstance(exp.func("instr", "x", "b", dialect="mysql"), exp.StrPosition)
        self.assertIsInstance(exp.func("bla", 1, "foo"), exp.Anonymous)
        self.assertIsInstance(
            exp.func("cast", this=exp.Literal.number(5), to=exp.DataType.build("DOUBLE")),
            exp.Cast,
        )

        with self.assertRaises(ValueError):
            exp.func("some_func", 1, arg2="foo")

        with self.assertRaises(ValueError):
            exp.func("abs")

        with self.assertRaises(ValueError) as cm:
            exp.func("to_hex", dialect="bigquery", this=5)

        self.assertEqual(
            str(cm.exception),
            "Unable to convert 'to_hex' into a Func. Either manually construct the Func "
            "expression of interest or parse the function call.",
        )

    def test_named_selects(self):
        expression = parse_one(
            "SELECT a, b AS B, c + d AS e, *, 'zz', 'zz' AS z FROM foo as bar, baz"
        )
        self.assertEqual(expression.named_selects, ["a", "B", "e", "*", "zz", "z"])

        expression = parse_one(
            """
            WITH first AS (SELECT * FROM foo)
            SELECT foo.bar, foo.baz as bazz, SUM(x) FROM first
        """
        )
        self.assertEqual(expression.named_selects, ["bar", "bazz"])

        expression = parse_one(
            """
            SELECT foo, bar FROM first
            UNION SELECT "ss" as foo, bar FROM second
            UNION ALL SELECT foo, bazz FROM third
        """
        )
        self.assertEqual(expression.named_selects, ["foo", "bar"])

    def test_selects(self):
        expression = parse_one("SELECT FROM x")
        self.assertEqual(expression.selects, [])

        expression = parse_one("SELECT a FROM x")
        self.assertEqual([s.sql() for s in expression.selects], ["a"])

        expression = parse_one("SELECT a, b FROM x")
        self.assertEqual([s.sql() for s in expression.selects], ["a", "b"])

        expression = parse_one("(SELECT a, b FROM x)")
        self.assertEqual([s.sql() for s in expression.selects], ["a", "b"])

    def test_alias_column_names(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x) AS y")
        subquery = expression.find(exp.Subquery)
        self.assertEqual(subquery.alias_column_names, [])

        expression = parse_one("SELECT * FROM (SELECT * FROM x) AS y(a)")
        subquery = expression.find(exp.Subquery)
        self.assertEqual(subquery.alias_column_names, ["a"])

        expression = parse_one("SELECT * FROM (SELECT * FROM x) AS y(a, b)")
        subquery = expression.find(exp.Subquery)
        self.assertEqual(subquery.alias_column_names, ["a", "b"])

        expression = parse_one("WITH y AS (SELECT * FROM x) SELECT * FROM y")
        cte = expression.find(exp.CTE)
        self.assertEqual(cte.alias_column_names, [])

        expression = parse_one("WITH y(a, b) AS (SELECT * FROM x) SELECT * FROM y")
        cte = expression.find(exp.CTE)
        self.assertEqual(cte.alias_column_names, ["a", "b"])

        expression = parse_one("SELECT * FROM tbl AS tbl(a, b)")
        table = expression.find(exp.Table)
        self.assertEqual(table.alias_column_names, ["a", "b"])

    def test_ctes(self):
        expression = parse_one("SELECT a FROM x")
        self.assertEqual(expression.ctes, [])

        expression = parse_one("WITH x AS (SELECT a FROM y) SELECT a FROM x")
        self.assertEqual([s.sql() for s in expression.ctes], ["x AS (SELECT a FROM y)"])

    def test_hash(self):
        self.assertEqual(
            {
                parse_one("select a.b"),
                parse_one("1+2"),
                parse_one('"a"."b"'),
                parse_one("a.b.c.d"),
            },
            {
                parse_one("select a.b"),
                parse_one("1+2"),
                parse_one('"a"."b"'),
                parse_one("a.b.c.d"),
            },
        )

    def test_sql(self):
        self.assertEqual(parse_one("x + y * 2").sql(), "x + y * 2")
        self.assertEqual(parse_one('select "x"').sql(dialect="hive", pretty=True), "SELECT\n  `x`")
        self.assertEqual(parse_one("X + y").sql(identify=True, normalize=True), '"x" + "y"')
        self.assertEqual(parse_one('"X" + Y').sql(identify=True, normalize=True), '"X" + "y"')
        self.assertEqual(parse_one("SUM(X)").sql(identify=True, normalize=True), 'SUM("x")')

    def test_transform_with_arguments(self):
        expression = parse_one("a")

        def fun(node, alias_=True):
            if alias_:
                return parse_one("a AS a")
            return node

        transformed_expression = expression.transform(fun)
        self.assertEqual(transformed_expression.sql(dialect="presto"), "a AS a")

        transformed_expression_2 = expression.transform(fun, alias_=False)
        self.assertEqual(transformed_expression_2.sql(dialect="presto"), "a")

    def test_transform_simple(self):
        expression = parse_one("IF(a > 0, a, b)")

        def fun(node):
            if isinstance(node, exp.Column) and node.name == "a":
                return parse_one("c - 2")
            return node

        actual_expression_1 = expression.transform(fun)
        self.assertEqual(actual_expression_1.sql(dialect="presto"), "IF(c - 2 > 0, c - 2, b)")
        self.assertIsNot(actual_expression_1, expression)

        actual_expression_2 = expression.transform(fun, copy=False)
        self.assertEqual(actual_expression_2.sql(dialect="presto"), "IF(c - 2 > 0, c - 2, b)")
        self.assertIs(actual_expression_2, expression)

    def test_transform_no_infinite_recursion(self):
        expression = parse_one("a")

        def fun(node):
            if isinstance(node, exp.Column) and node.name == "a":
                return parse_one("FUN(a)")
            return node

        self.assertEqual(expression.transform(fun).sql(), "FUN(a)")

    def test_transform_multiple_children(self):
        expression = parse_one("SELECT * FROM x")

        def fun(node):
            if isinstance(node, exp.Star):
                return [parse_one(c) for c in ["a", "b"]]
            return node

        self.assertEqual(expression.transform(fun).sql(), "SELECT a, b FROM x")

    def test_transform_node_removal(self):
        expression = parse_one("SELECT a, b FROM x")

        def remove_column_b(node):
            if isinstance(node, exp.Column) and node.name == "b":
                return None
            return node

        self.assertEqual(expression.transform(remove_column_b).sql(), "SELECT a FROM x")
        self.assertEqual(expression.transform(lambda _: None), None)

        expression = parse_one("CAST(x AS FLOAT)")

        def remove_non_list_arg(node):
            if isinstance(node, exp.DataType):
                return None
            return node

        self.assertEqual(expression.transform(remove_non_list_arg).sql(), "CAST(x AS)")

        expression = parse_one("SELECT a, b FROM x")

        def remove_all_columns(node):
            if isinstance(node, exp.Column):
                return None
            return node

        self.assertEqual(expression.transform(remove_all_columns).sql(), "SELECT FROM x")

    def test_replace(self):
        expression = parse_one("SELECT a, b FROM x")
        expression.find(exp.Column).replace(parse_one("c"))
        self.assertEqual(expression.sql(), "SELECT c, b FROM x")
        expression.find(exp.Table).replace(parse_one("y"))
        self.assertEqual(expression.sql(), "SELECT c, b FROM y")

    def test_arg_deletion(self):
        # Using the pop helper method
        expression = parse_one("SELECT a, b FROM x")
        expression.find(exp.Column).pop()
        self.assertEqual(expression.sql(), "SELECT b FROM x")

        expression.find(exp.Column).pop()
        self.assertEqual(expression.sql(), "SELECT FROM x")

        expression.pop()
        self.assertEqual(expression.sql(), "SELECT FROM x")

        expression = parse_one("WITH x AS (SELECT a FROM x) SELECT * FROM x")
        expression.find(exp.With).pop()
        self.assertEqual(expression.sql(), "SELECT * FROM x")

        # Manually deleting by setting to None
        expression = parse_one("SELECT * FROM foo JOIN bar")
        self.assertEqual(len(expression.args.get("joins", [])), 1)

        expression.set("joins", None)
        self.assertEqual(expression.sql(), "SELECT * FROM foo")
        self.assertEqual(expression.args.get("joins", []), [])
        self.assertIsNone(expression.args.get("joins"))

    def test_walk(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x)")
        self.assertEqual(len(list(expression.walk())), 9)
        self.assertEqual(len(list(expression.walk(bfs=False))), 9)
        self.assertTrue(all(isinstance(e, exp.Expression) for e, _, _ in expression.walk()))
        self.assertTrue(
            all(isinstance(e, exp.Expression) for e, _, _ in expression.walk(bfs=False))
        )

    def test_functions(self):
        self.assertIsInstance(parse_one("x LIKE ANY (y)"), exp.Like)
        self.assertIsInstance(parse_one("x ILIKE ANY (y)"), exp.ILike)
        self.assertIsInstance(parse_one("ABS(a)"), exp.Abs)
        self.assertIsInstance(parse_one("APPROX_DISTINCT(a)"), exp.ApproxDistinct)
        self.assertIsInstance(parse_one("ARRAY(a)"), exp.Array)
        self.assertIsInstance(parse_one("ARRAY_AGG(a)"), exp.ArrayAgg)
        self.assertIsInstance(parse_one("ARRAY_CONTAINS(a, 'a')"), exp.ArrayContains)
        self.assertIsInstance(parse_one("ARRAY_SIZE(a)"), exp.ArraySize)
        self.assertIsInstance(parse_one("AVG(a)"), exp.Avg)
        self.assertIsInstance(parse_one("BEGIN DEFERRED TRANSACTION"), exp.Transaction)
        self.assertIsInstance(parse_one("CEIL(a)"), exp.Ceil)
        self.assertIsInstance(parse_one("CEILING(a)"), exp.Ceil)
        self.assertIsInstance(parse_one("COALESCE(a, b)"), exp.Coalesce)
        self.assertIsInstance(parse_one("COMMIT"), exp.Commit)
        self.assertIsInstance(parse_one("COUNT(a)"), exp.Count)
        self.assertIsInstance(parse_one("COUNT_IF(a > 0)"), exp.CountIf)
        self.assertIsInstance(parse_one("DATE_ADD(a, 1)"), exp.DateAdd)
        self.assertIsInstance(parse_one("DATE_DIFF(a, 2)"), exp.DateDiff)
        self.assertIsInstance(parse_one("DATE_STR_TO_DATE(a)"), exp.DateStrToDate)
        self.assertIsInstance(parse_one("DAY(a)"), exp.Day)
        self.assertIsInstance(parse_one("EXP(a)"), exp.Exp)
        self.assertIsInstance(parse_one("FLOOR(a)"), exp.Floor)
        self.assertIsInstance(parse_one("GENERATE_SERIES(a, b, c)"), exp.GenerateSeries)
        self.assertIsInstance(parse_one("GLOB(x, y)"), exp.Glob)
        self.assertIsInstance(parse_one("GREATEST(a, b)"), exp.Greatest)
        self.assertIsInstance(parse_one("IF(a, b, c)"), exp.If)
        self.assertIsInstance(parse_one("INITCAP(a)"), exp.Initcap)
        self.assertIsInstance(parse_one("JSON_EXTRACT(a, '$.name')"), exp.JSONExtract)
        self.assertIsInstance(parse_one("JSON_EXTRACT_SCALAR(a, '$.name')"), exp.JSONExtractScalar)
        self.assertIsInstance(parse_one("LEAST(a, b)"), exp.Least)
        self.assertIsInstance(parse_one("LIKE(x, y)"), exp.Like)
        self.assertIsInstance(parse_one("LN(a)"), exp.Ln)
        self.assertIsInstance(parse_one("LOG10(a)"), exp.Log10)
        self.assertIsInstance(parse_one("MAX(a)"), exp.Max)
        self.assertIsInstance(parse_one("MIN(a)"), exp.Min)
        self.assertIsInstance(parse_one("MONTH(a)"), exp.Month)
        self.assertIsInstance(parse_one("POSITION(' ' IN a)"), exp.StrPosition)
        self.assertIsInstance(parse_one("POW(a, 2)"), exp.Pow)
        self.assertIsInstance(parse_one("POWER(a, 2)"), exp.Pow)
        self.assertIsInstance(parse_one("QUANTILE(a, 0.90)"), exp.Quantile)
        self.assertIsInstance(parse_one("REGEXP_LIKE(a, 'test')"), exp.RegexpLike)
        self.assertIsInstance(parse_one("REGEXP_SPLIT(a, 'test')"), exp.RegexpSplit)
        self.assertIsInstance(parse_one("ROLLBACK"), exp.Rollback)
        self.assertIsInstance(parse_one("ROUND(a)"), exp.Round)
        self.assertIsInstance(parse_one("ROUND(a, 2)"), exp.Round)
        self.assertIsInstance(parse_one("SPLIT(a, 'test')"), exp.Split)
        self.assertIsInstance(parse_one("STR_POSITION(a, 'test')"), exp.StrPosition)
        self.assertIsInstance(parse_one("STR_TO_UNIX(a, 'format')"), exp.StrToUnix)
        self.assertIsInstance(parse_one("STRUCT_EXTRACT(a, 'test')"), exp.StructExtract)
        self.assertIsInstance(parse_one("SUM(a)"), exp.Sum)
        self.assertIsInstance(parse_one("SQRT(a)"), exp.Sqrt)
        self.assertIsInstance(parse_one("STDDEV(a)"), exp.Stddev)
        self.assertIsInstance(parse_one("STDDEV_POP(a)"), exp.StddevPop)
        self.assertIsInstance(parse_one("STDDEV_SAMP(a)"), exp.StddevSamp)
        self.assertIsInstance(parse_one("TIME_TO_STR(a, 'format')"), exp.TimeToStr)
        self.assertIsInstance(parse_one("TIME_TO_TIME_STR(a)"), exp.Cast)
        self.assertIsInstance(parse_one("TIME_TO_UNIX(a)"), exp.TimeToUnix)
        self.assertIsInstance(parse_one("TIME_STR_TO_DATE(a)"), exp.TimeStrToDate)
        self.assertIsInstance(parse_one("TIME_STR_TO_TIME(a)"), exp.TimeStrToTime)
        self.assertIsInstance(parse_one("TIME_STR_TO_UNIX(a)"), exp.TimeStrToUnix)
        self.assertIsInstance(parse_one("TRIM(LEADING 'b' FROM 'bla')"), exp.Trim)
        self.assertIsInstance(parse_one("TS_OR_DS_ADD(a, 1, 'day')"), exp.TsOrDsAdd)
        self.assertIsInstance(parse_one("TS_OR_DS_TO_DATE(a)"), exp.TsOrDsToDate)
        self.assertIsInstance(parse_one("TS_OR_DS_TO_DATE_STR(a)"), exp.Substring)
        self.assertIsInstance(parse_one("UNIX_TO_STR(a, 'format')"), exp.UnixToStr)
        self.assertIsInstance(parse_one("UNIX_TO_TIME(a)"), exp.UnixToTime)
        self.assertIsInstance(parse_one("UNIX_TO_TIME_STR(a)"), exp.UnixToTimeStr)
        self.assertIsInstance(parse_one("VARIANCE(a)"), exp.Variance)
        self.assertIsInstance(parse_one("VARIANCE_POP(a)"), exp.VariancePop)
        self.assertIsInstance(parse_one("YEAR(a)"), exp.Year)
        self.assertIsInstance(parse_one("HLL(a)"), exp.Hll)
        self.assertIsInstance(parse_one("ARRAY(time, foo)"), exp.Array)
        self.assertIsInstance(parse_one("STANDARD_HASH('hello', 'sha256')"), exp.StandardHash)
        self.assertIsInstance(parse_one("DATE(foo)"), exp.Date)
        self.assertIsInstance(parse_one("HEX(foo)"), exp.Hex)
        self.assertIsInstance(parse_one("TO_HEX(foo)", read="bigquery"), exp.Hex)
        self.assertIsInstance(parse_one("TO_HEX(MD5(foo))", read="bigquery"), exp.MD5)
        self.assertIsInstance(parse_one("TRANSFORM(a, b)", read="spark"), exp.Transform)
        self.assertIsInstance(parse_one("ADD_MONTHS(a, b)"), exp.AddMonths)

    def test_column(self):
        column = parse_one("a.b.c.d")
        self.assertEqual(column.catalog, "a")
        self.assertEqual(column.db, "b")
        self.assertEqual(column.table, "c")
        self.assertEqual(column.name, "d")

        column = parse_one("a")
        self.assertEqual(column.name, "a")
        self.assertEqual(column.table, "")

        fields = parse_one("a.b.c.d.e")
        self.assertIsInstance(fields, exp.Dot)
        self.assertEqual(fields.text("expression"), "e")
        column = fields.find(exp.Column)
        self.assertEqual(column.name, "d")
        self.assertEqual(column.table, "c")
        self.assertEqual(column.db, "b")
        self.assertEqual(column.catalog, "a")

        column = parse_one("a[0].b")
        self.assertIsInstance(column, exp.Dot)
        self.assertIsInstance(column.this, exp.Bracket)
        self.assertIsInstance(column.this.this, exp.Column)

        column = parse_one("a.*")
        self.assertIsInstance(column, exp.Column)
        self.assertIsInstance(column.this, exp.Star)
        self.assertIsInstance(column.args["table"], exp.Identifier)
        self.assertEqual(column.table, "a")

        self.assertIsInstance(parse_one("*"), exp.Star)
        self.assertEqual(exp.column("a", table="b", db="c", catalog="d"), exp.to_column("d.c.b.a"))

        dot = exp.column("d", "c", "b", "a", fields=["e", "f"])
        self.assertIsInstance(dot, exp.Dot)
        self.assertEqual(dot.sql(), "a.b.c.d.e.f")

    def test_text(self):
        column = parse_one("a.b.c.d.e")
        self.assertEqual(column.text("expression"), "e")
        self.assertEqual(column.text("y"), "")
        self.assertEqual(parse_one("select * from x.y").find(exp.Table).text("db"), "x")
        self.assertEqual(parse_one("select *").name, "")
        self.assertEqual(parse_one("1 + 1").name, "1")
        self.assertEqual(parse_one("'a'").name, "a")

    def test_alias(self):
        self.assertEqual(alias("foo", "bar").sql(), "foo AS bar")
        self.assertEqual(alias("foo", "bar-1").sql(), 'foo AS "bar-1"')
        self.assertEqual(alias("foo", "bar_1").sql(), "foo AS bar_1")
        self.assertEqual(alias("foo * 2", "2bar").sql(), 'foo * 2 AS "2bar"')
        self.assertEqual(alias('"foo"', "_bar").sql(), '"foo" AS _bar')
        self.assertEqual(alias("foo", "bar", quoted=True).sql(), 'foo AS "bar"')

    def test_unit(self):
        unit = parse_one("timestamp_trunc(current_timestamp, week(thursday))")
        self.assertIsNotNone(unit.find(exp.CurrentTimestamp))
        week = unit.find(exp.Week)
        self.assertEqual(week.this, exp.var("thursday"))

        for abbreviated_unit, unnabreviated_unit in exp.TimeUnit.UNABBREVIATED_UNIT_NAME.items():
            interval = parse_one(f"interval '500 {abbreviated_unit}'")
            self.assertIsInstance(interval.unit, exp.Var)
            self.assertEqual(interval.unit.name, unnabreviated_unit)

    def test_identifier(self):
        self.assertTrue(exp.to_identifier('"x"').quoted)
        self.assertFalse(exp.to_identifier("x").quoted)
        self.assertTrue(exp.to_identifier("foo ").quoted)
        self.assertFalse(exp.to_identifier("_x").quoted)

    def test_function_normalizer(self):
        self.assertEqual(parse_one("HELLO()").sql(normalize_functions="lower"), "hello()")
        self.assertEqual(parse_one("hello()").sql(normalize_functions="upper"), "HELLO()")
        self.assertEqual(parse_one("heLLO()").sql(normalize_functions=False), "heLLO()")
        self.assertEqual(parse_one("SUM(x)").sql(normalize_functions="lower"), "sum(x)")
        self.assertEqual(parse_one("sum(x)").sql(normalize_functions="upper"), "SUM(x)")

    def test_properties_from_dict(self):
        self.assertEqual(
            exp.Properties.from_dict(
                {
                    "FORMAT": "parquet",
                    "PARTITIONED_BY": (exp.to_identifier("a"), exp.to_identifier("b")),
                    "custom": 1,
                    "ENGINE": None,
                    "COLLATE": True,
                }
            ),
            exp.Properties(
                expressions=[
                    exp.FileFormatProperty(this=exp.Literal.string("parquet")),
                    exp.PartitionedByProperty(
                        this=exp.Tuple(expressions=[exp.to_identifier("a"), exp.to_identifier("b")])
                    ),
                    exp.Property(this=exp.Literal.string("custom"), value=exp.Literal.number(1)),
                    exp.EngineProperty(this=exp.null()),
                    exp.CollateProperty(this=exp.true()),
                ]
            ),
        )

        self.assertRaises(ValueError, exp.Properties.from_dict, {"FORMAT": object})

    def test_convert(self):
        for value, expected in [
            (1, "1"),
            ("1", "'1'"),
            (None, "NULL"),
            (True, "TRUE"),
            ((1, "2", None), "(1, '2', NULL)"),
            ([1, "2", None], "ARRAY(1, '2', NULL)"),
            ({"x": None}, "MAP(ARRAY('x'), ARRAY(NULL))"),
            (
                datetime.datetime(2022, 10, 1, 1, 1, 1, 1),
                "TIME_STR_TO_TIME('2022-10-01T01:01:01.000001+00:00')",
            ),
            (
                datetime.datetime(2022, 10, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
                "TIME_STR_TO_TIME('2022-10-01T01:01:01+00:00')",
            ),
            (datetime.date(2022, 10, 1), "DATE_STR_TO_DATE('2022-10-01')"),
            (math.nan, "NULL"),
        ]:
            with self.subTest(value):
                self.assertEqual(exp.convert(value).sql(), expected)

        self.assertEqual(
            exp.convert({"test": "value"}).sql(dialect="spark"),
            "MAP_FROM_ARRAYS(ARRAY('test'), ARRAY('value'))",
        )

    def test_comment_alias(self):
        sql = """
        SELECT
            a,
            b AS B,
            c, /*comment*/
            d AS D, -- another comment
            CAST(x AS INT) -- final comment
        FROM foo
        """
        expression = parse_one(sql)
        self.assertEqual(
            [e.alias_or_name for e in expression.expressions],
            ["a", "B", "c", "D", "x"],
        )
        self.assertEqual(
            expression.sql(),
            "SELECT a, b AS B, c /* comment */, d AS D /* another comment */, CAST(x AS INT) /* final comment */ FROM foo",
        )
        self.assertEqual(
            expression.sql(comments=False),
            "SELECT a, b AS B, c, d AS D, CAST(x AS INT) FROM foo",
        )
        self.assertEqual(
            expression.sql(pretty=True, comments=False),
            """SELECT
  a,
  b AS B,
  c,
  d AS D,
  CAST(x AS INT)
FROM foo""",
        )
        self.assertEqual(
            expression.sql(pretty=True),
            """SELECT
  a,
  b AS B,
  c, /* comment */
  d AS D, /* another comment */
  CAST(x AS INT) /* final comment */
FROM foo""",
        )

    def test_to_interval(self):
        self.assertEqual(exp.to_interval("1day").sql(), "INTERVAL '1' DAY")
        self.assertEqual(exp.to_interval("  5     months").sql(), "INTERVAL '5' MONTHS")
        with self.assertRaises(ValueError):
            exp.to_interval("bla")

        self.assertEqual(exp.to_interval(exp.Literal.string("1day")).sql(), "INTERVAL '1' DAY")
        self.assertEqual(
            exp.to_interval(exp.Literal.string("  5   months")).sql(), "INTERVAL '5' MONTHS"
        )
        with self.assertRaises(ValueError):
            exp.to_interval(exp.Literal.string("bla"))

    def test_to_table(self):
        table_only = exp.to_table("table_name")
        self.assertEqual(table_only.name, "table_name")
        self.assertIsNone(table_only.args.get("db"))
        self.assertIsNone(table_only.args.get("catalog"))
        db_and_table = exp.to_table("db.table_name")
        self.assertEqual(db_and_table.name, "table_name")
        self.assertEqual(db_and_table.args.get("db"), exp.to_identifier("db"))
        self.assertIsNone(db_and_table.args.get("catalog"))
        catalog_db_and_table = exp.to_table("catalog.db.table_name")
        self.assertEqual(catalog_db_and_table.name, "table_name")
        self.assertEqual(catalog_db_and_table.args.get("db"), exp.to_identifier("db"))
        self.assertEqual(catalog_db_and_table.args.get("catalog"), exp.to_identifier("catalog"))
        with self.assertRaises(ValueError):
            exp.to_table(1)

    def test_to_column(self):
        column_only = exp.to_column("column_name")
        self.assertEqual(column_only.name, "column_name")
        self.assertIsNone(column_only.args.get("table"))
        table_and_column = exp.to_column("table_name.column_name")
        self.assertEqual(table_and_column.name, "column_name")
        self.assertEqual(table_and_column.args.get("table"), exp.to_identifier("table_name"))
        with self.assertRaises(ValueError):
            exp.to_column(1)

    def test_union(self):
        expression = parse_one("SELECT cola, colb UNION SELECT colx, coly")
        self.assertIsInstance(expression, exp.Union)
        self.assertEqual(expression.named_selects, ["cola", "colb"])
        self.assertEqual(
            expression.selects,
            [
                exp.Column(this=exp.to_identifier("cola")),
                exp.Column(this=exp.to_identifier("colb")),
            ],
        )

    def test_values(self):
        self.assertEqual(
            exp.values([(1, 2), (3, 4)], "t", ["a", "b"]).sql(),
            "(VALUES (1, 2), (3, 4)) AS t(a, b)",
        )
        self.assertEqual(
            exp.values(
                [(1, 2), (3, 4)],
                "t",
                {"a": exp.DataType.build("TEXT"), "b": exp.DataType.build("TEXT")},
            ).sql(),
            "(VALUES (1, 2), (3, 4)) AS t(a, b)",
        )
        with self.assertRaises(ValueError):
            exp.values([(1, 2), (3, 4)], columns=["a"])

    def test_data_type_builder(self):
        self.assertEqual(exp.DataType.build("TEXT").sql(), "TEXT")
        self.assertEqual(exp.DataType.build("DECIMAL(10, 2)").sql(), "DECIMAL(10, 2)")
        self.assertEqual(exp.DataType.build("VARCHAR(255)").sql(), "VARCHAR(255)")
        self.assertEqual(exp.DataType.build("ARRAY<INT>").sql(), "ARRAY<INT>")
        self.assertEqual(exp.DataType.build("CHAR").sql(), "CHAR")
        self.assertEqual(exp.DataType.build("NCHAR").sql(), "CHAR")
        self.assertEqual(exp.DataType.build("VARCHAR").sql(), "VARCHAR")
        self.assertEqual(exp.DataType.build("NVARCHAR").sql(), "VARCHAR")
        self.assertEqual(exp.DataType.build("TEXT").sql(), "TEXT")
        self.assertEqual(exp.DataType.build("BINARY").sql(), "BINARY")
        self.assertEqual(exp.DataType.build("VARBINARY").sql(), "VARBINARY")
        self.assertEqual(exp.DataType.build("INT").sql(), "INT")
        self.assertEqual(exp.DataType.build("TINYINT").sql(), "TINYINT")
        self.assertEqual(exp.DataType.build("SMALLINT").sql(), "SMALLINT")
        self.assertEqual(exp.DataType.build("BIGINT").sql(), "BIGINT")
        self.assertEqual(exp.DataType.build("FLOAT").sql(), "FLOAT")
        self.assertEqual(exp.DataType.build("DOUBLE").sql(), "DOUBLE")
        self.assertEqual(exp.DataType.build("DECIMAL").sql(), "DECIMAL")
        self.assertEqual(exp.DataType.build("BOOLEAN").sql(), "BOOLEAN")
        self.assertEqual(exp.DataType.build("JSON").sql(), "JSON")
        self.assertEqual(exp.DataType.build("JSONB", dialect="postgres").sql(), "JSONB")
        self.assertEqual(exp.DataType.build("INTERVAL").sql(), "INTERVAL")
        self.assertEqual(exp.DataType.build("TIME").sql(), "TIME")
        self.assertEqual(exp.DataType.build("TIMESTAMP").sql(), "TIMESTAMP")
        self.assertEqual(exp.DataType.build("TIMESTAMPTZ").sql(), "TIMESTAMPTZ")
        self.assertEqual(exp.DataType.build("TIMESTAMPLTZ").sql(), "TIMESTAMPLTZ")
        self.assertEqual(exp.DataType.build("DATE").sql(), "DATE")
        self.assertEqual(exp.DataType.build("DATETIME").sql(), "DATETIME")
        self.assertEqual(exp.DataType.build("ARRAY").sql(), "ARRAY")
        self.assertEqual(exp.DataType.build("MAP").sql(), "MAP")
        self.assertEqual(exp.DataType.build("UUID").sql(), "UUID")
        self.assertEqual(exp.DataType.build("GEOGRAPHY").sql(), "GEOGRAPHY")
        self.assertEqual(exp.DataType.build("GEOMETRY").sql(), "GEOMETRY")
        self.assertEqual(exp.DataType.build("STRUCT").sql(), "STRUCT")
        self.assertEqual(exp.DataType.build("NULLABLE").sql(), "NULLABLE")
        self.assertEqual(exp.DataType.build("HLLSKETCH", dialect="redshift").sql(), "HLLSKETCH")
        self.assertEqual(exp.DataType.build("HSTORE", dialect="postgres").sql(), "HSTORE")
        self.assertEqual(exp.DataType.build("NULL").sql(), "NULL")
        self.assertEqual(exp.DataType.build("NULL", dialect="bigquery").sql(), "NULL")
        self.assertEqual(exp.DataType.build("UNKNOWN").sql(), "UNKNOWN")
        self.assertEqual(exp.DataType.build("UNKNOWN", dialect="bigquery").sql(), "UNKNOWN")
        self.assertEqual(exp.DataType.build("UNKNOWN", dialect="snowflake").sql(), "UNKNOWN")
        self.assertEqual(exp.DataType.build("TIMESTAMP", dialect="bigquery").sql(), "TIMESTAMPTZ")
        self.assertEqual(
            exp.DataType.build("struct<x int>", dialect="spark").sql(), "STRUCT<x INT>"
        )
        self.assertEqual(exp.DataType.build("USER-DEFINED").sql(), "USER-DEFINED")

        self.assertEqual(exp.DataType.build("ARRAY<UNKNOWN>").sql(), "ARRAY<UNKNOWN>")
        self.assertEqual(exp.DataType.build("ARRAY<NULL>").sql(), "ARRAY<NULL>")
        self.assertEqual(exp.DataType.build("varchar(100) collate 'en-ci'").sql(), "VARCHAR(100)")

        with self.assertRaises(ParseError):
            exp.DataType.build("varchar(")

    def test_rename_table(self):
        self.assertEqual(
            exp.rename_table("t1", "t2").sql(),
            "ALTER TABLE t1 RENAME TO t2",
        )

    def test_is_star(self):
        assert parse_one("*").is_star
        assert parse_one("foo.*").is_star
        assert parse_one("SELECT * FROM foo").is_star
        assert parse_one("(SELECT * FROM foo)").is_star
        assert parse_one("SELECT *, 1 FROM foo").is_star
        assert parse_one("SELECT foo.* FROM foo").is_star
        assert parse_one("SELECT * EXCEPT (a, b) FROM foo").is_star
        assert parse_one("SELECT foo.* EXCEPT (foo.a, foo.b) FROM foo").is_star
        assert parse_one("SELECT * REPLACE (a AS b, b AS C)").is_star
        assert parse_one("SELECT * EXCEPT (a, b) REPLACE (a AS b, b AS C)").is_star
        assert parse_one("SELECT * INTO newevent FROM event").is_star
        assert parse_one("SELECT * FROM foo UNION SELECT * FROM bar").is_star
        assert parse_one("SELECT * FROM bla UNION SELECT 1 AS x").is_star
        assert parse_one("SELECT 1 AS x UNION SELECT * FROM bla").is_star
        assert parse_one("SELECT 1 AS x UNION SELECT 1 AS x UNION SELECT * FROM foo").is_star

    def test_set_metadata(self):
        ast = parse_one("SELECT foo.col FROM foo")

        self.assertIsNone(ast._meta)

        # calling ast.meta would lazily instantiate self._meta
        self.assertEqual(ast.meta, {})
        self.assertEqual(ast._meta, {})

        ast.meta["some_meta_key"] = "some_meta_value"
        self.assertEqual(ast.meta.get("some_meta_key"), "some_meta_value")
        self.assertEqual(ast.meta.get("some_other_meta_key"), None)

        ast.meta["some_other_meta_key"] = "some_other_meta_value"
        self.assertEqual(ast.meta.get("some_other_meta_key"), "some_other_meta_value")

    def test_unnest(self):
        ast = parse_one("SELECT (((1)))")
        self.assertIs(ast.selects[0].unnest(), ast.find(exp.Literal))

        ast = parse_one("SELECT * FROM (((SELECT * FROM t)))")
        self.assertIs(ast.args["from"].this.unnest(), list(ast.find_all(exp.Select))[1])

        ast = parse_one("SELECT * FROM ((((SELECT * FROM t))) AS foo)")
        second_subquery = ast.args["from"].this.this
        innermost_subquery = list(ast.find_all(exp.Select))[1].parent
        self.assertIs(second_subquery, innermost_subquery.unwrap())

    def test_is_type(self):
        ast = parse_one("CAST(x AS VARCHAR)")
        assert ast.is_type("VARCHAR")
        assert not ast.is_type("VARCHAR(5)")
        assert not ast.is_type("FLOAT")

        ast = parse_one("CAST(x AS VARCHAR(5))")
        assert ast.is_type("VARCHAR")
        assert ast.is_type("VARCHAR(5)")
        assert not ast.is_type("VARCHAR(4)")
        assert not ast.is_type("FLOAT")

        ast = parse_one("CAST(x AS ARRAY<INT>)")
        assert ast.is_type("ARRAY")
        assert ast.is_type("ARRAY<INT>")
        assert not ast.is_type("ARRAY<FLOAT>")
        assert not ast.is_type("INT")

        ast = parse_one("CAST(x AS ARRAY)")
        assert ast.is_type("ARRAY")
        assert not ast.is_type("ARRAY<INT>")
        assert not ast.is_type("ARRAY<FLOAT>")
        assert not ast.is_type("INT")

        ast = parse_one("CAST(x AS STRUCT<a INT, b FLOAT>)")
        assert ast.is_type("STRUCT")
        assert ast.is_type("STRUCT<a INT, b FLOAT>")
        assert not ast.is_type("STRUCT<a VARCHAR, b INT>")

        dtype = exp.DataType.build("foo", udt=True)
        assert dtype.is_type("foo")
        assert not dtype.is_type("bar")

        dtype = exp.DataType.build("a.b.c", udt=True)
        assert dtype.is_type("a.b.c")

        with self.assertRaises(ParseError):
            exp.DataType.build("foo")

    def test_set_meta(self):
        query = parse_one("SELECT * FROM foo /* sqlglot.meta x = 1, y = a, z */")
        self.assertEqual(query.find(exp.Table).meta, {"x": "1", "y": "a", "z": True})
        self.assertEqual(query.sql(), "SELECT * FROM foo /* sqlglot.meta x = 1, y = a, z */")

    def test_assert_is(self):
        parse_one("x").assert_is(exp.Column)

        with self.assertRaisesRegex(
            AssertionError, "x is not <class 'sqlglot.expressions.Identifier'>\."
        ):
            parse_one("x").assert_is(exp.Identifier)
