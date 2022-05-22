import unittest

import sqlglot.expressions as exp
from sqlglot import parse_one


class TestExpressions(unittest.TestCase):
    def test_depth(self):
        self.assertEqual(parse_one("x(1)").find(exp.Literal).depth, 1)

    def test_eq(self):
        self.assertEqual(parse_one("`a`", read="hive"), parse_one('"a"'))
        self.assertEqual(parse_one("`a`", read="hive"), parse_one('"a"  '))
        self.assertEqual(parse_one("`a`.b", read="hive"), parse_one('"a"."b"'))
        self.assertEqual(parse_one("select a, b+1"), parse_one("SELECT a, b + 1"))
        self.assertEqual(parse_one("`a`.`b`.`c`", read="hive"), parse_one("a.b.c"))
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
        self.assertEqual(
            parse_one("TO_DATE(x)", read="hive"), parse_one("ts_or_ds_to_date_str(x)")
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
        self.assertIsInstance(column.find_ancestor(exp.Select), exp.Select)
        self.assertIsNone(column.find_ancestor(exp.Join))

    def test_alias_or_name(self):
        expression = parse_one(
            "SELECT a, b AS B, c + d AS e, *, 'zz', 'zz' AS z FROM foo as bar, baz"
        )
        self.assertEqual(
            [e.alias_or_name for e in expression.args["expressions"]],
            ["a", "B", "e", "*", "zz", "z"],
        )
        self.assertEqual(
            [e.alias_or_name for e in expression.args["from"].args["expressions"]],
            ["bar", "baz"],
        )

        expression = parse_one(
            """
            WITH first AS (SELECT * FROM foo),
                 second AS (SELECT * FROM bar)
            SELECT * FROM first, second, (SELECT * FROM baz) AS third
        """
        )

        self.assertEqual(
            [e.alias_or_name for e in expression.args["with"].args["expressions"]],
            ["first", "second"],
        )

        self.assertEqual(
            [e.alias_or_name for e in expression.args["from"].args["expressions"]],
            ["first", "second", "third"],
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
        self.assertEqual(expression.named_selects, ["foo", "bar", "bazz"])

    def test_hash(self):
        self.assertEqual(
            {
                parse_one("select a.b"),
                parse_one("1+2"),
                parse_one('"a".b'),
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
        assert parse_one("x + y * 2").sql() == "x + y * 2"
        assert (
            parse_one('select "x"').sql(dialect="hive", pretty=True) == "SELECT\n  `x`"
        )

    def test_transform_with_arguments(self):
        expression = parse_one("a")

        def fun(node, alias=True):
            if alias:
                return parse_one("a AS a")
            return node

        transformed_expression = expression.transform(fun)
        self.assertEqual(transformed_expression.sql(dialect="presto"), "a AS a")

        transformed_expression_2 = expression.transform(fun, alias=False)
        self.assertEqual(transformed_expression_2.sql(dialect="presto"), "a")

    def test_transform_simple(self):
        expression = parse_one("IF(a > 0, a, b)")

        def fun(node):
            if isinstance(node, exp.Column) and node.name == "a":
                return parse_one("c - 2")
            return node

        actual_expression_1 = expression.transform(fun)
        self.assertEqual(
            actual_expression_1.sql(dialect="presto"), "IF(c - 2 > 0, c - 2, b)"
        )
        self.assertIsNot(actual_expression_1, expression)

        actual_expression_2 = expression.transform(fun, copy=False)
        self.assertEqual(
            actual_expression_2.sql(dialect="presto"), "IF(c - 2 > 0, c - 2, b)"
        )
        self.assertIs(actual_expression_2, expression)

        with self.assertRaises(ValueError):
            parse_one("a").transform(lambda n: None)

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

    def test_replace(self):
        expression = parse_one("SELECT a, b FROM x")
        expression.find(exp.Column).replace(parse_one("c"))
        self.assertEqual(expression.sql(), "SELECT c, b FROM x")
        expression.find(exp.Table).replace(parse_one("y"))
        self.assertEqual(expression.sql(), "SELECT c, b FROM y")
        expression.find(exp.Column).replace(parse_one("d"), parse_one("e"))
        self.assertEqual(expression.sql(), "SELECT d, e, b FROM y")

    def test_walk(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x)")
        self.assertEqual(len(list(expression.walk())), 11)
        self.assertEqual(len(list(expression.walk(stop_types=exp.Subquery))), 4)

    def test_functions(self):
        # pylint: disable=too-many-statements
        self.assertIsInstance(parse_one("ABS(a)"), exp.Abs)
        self.assertIsInstance(parse_one("APPROX_DISTINCT(a)"), exp.ApproxDistinct)
        self.assertIsInstance(parse_one("ARRAY(a)"), exp.Array)
        self.assertIsInstance(parse_one("ARRAY_AGG(a)"), exp.ArrayAgg)
        self.assertIsInstance(parse_one("ARRAY_CONTAINS(a, 'a')"), exp.ArrayContains)
        self.assertIsInstance(parse_one("ARRAY_SIZE(a)"), exp.ArraySize)
        self.assertIsInstance(parse_one("AVG(a)"), exp.Avg)
        self.assertIsInstance(parse_one("CEIL(a)"), exp.Ceil)
        self.assertIsInstance(parse_one("CEILING(a)"), exp.Ceil)
        self.assertIsInstance(parse_one("COALESCE(a, b)"), exp.Coalesce)
        self.assertIsInstance(parse_one("COUNT(a)"), exp.Count)
        self.assertIsInstance(parse_one("DATE_ADD(a, 1)"), exp.DateAdd)
        self.assertIsInstance(parse_one("DATE_DIFF(a, 2)"), exp.DateDiff)
        self.assertIsInstance(parse_one("DATE_STR_TO_DATE(a)"), exp.DateStrToDate)
        self.assertIsInstance(parse_one("DAY(a)"), exp.Day)
        self.assertIsInstance(parse_one("EXP(a)"), exp.Exp)
        self.assertIsInstance(parse_one("FLOOR(a)"), exp.Floor)
        self.assertIsInstance(parse_one("GREATEST(a, b)"), exp.Greatest)
        self.assertIsInstance(parse_one("IF(a, b, c)"), exp.If)
        self.assertIsInstance(parse_one("INITCAP(a)"), exp.Initcap)
        self.assertIsInstance(parse_one("JSON_EXTRACT(a, '$.name')"), exp.JSONExtract)
        self.assertIsInstance(
            parse_one("JSON_EXTRACT_SCALAR(a, '$.name')"), exp.JSONExtractScalar
        )
        self.assertIsInstance(parse_one("LEAST(a, b)"), exp.Least)
        self.assertIsInstance(parse_one("LN(a)"), exp.Ln)
        self.assertIsInstance(parse_one("LOG10(a)"), exp.Log10)
        self.assertIsInstance(parse_one("MAX(a)"), exp.Max)
        self.assertIsInstance(parse_one("MIN(a)"), exp.Min)
        self.assertIsInstance(parse_one("MONTH(a)"), exp.Month)
        self.assertIsInstance(parse_one("POW(a, 2)"), exp.Pow)
        self.assertIsInstance(parse_one("POWER(a, 2)"), exp.Pow)
        self.assertIsInstance(parse_one("QUANTILE(a, 0.90)"), exp.Quantile)
        self.assertIsInstance(parse_one("REGEXP_LIKE(a, 'test')"), exp.RegexpLike)
        self.assertIsInstance(parse_one("REGEXP_SPLIT(a, 'test')"), exp.RegexpSplit)
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
        self.assertIsInstance(parse_one("TIME_TO_TIME_STR(a)"), exp.TimeToTimeStr)
        self.assertIsInstance(parse_one("TIME_TO_UNIX(a)"), exp.TimeToUnix)
        self.assertIsInstance(parse_one("TIME_STR_TO_DATE(a)"), exp.TimeStrToDate)
        self.assertIsInstance(parse_one("TIME_STR_TO_TIME(a)"), exp.TimeStrToTime)
        self.assertIsInstance(parse_one("TIME_STR_TO_UNIX(a)"), exp.TimeStrToUnix)
        self.assertIsInstance(parse_one("TS_OR_DS_ADD(a, 1, 'day')"), exp.TsOrDsAdd)
        self.assertIsInstance(parse_one("TS_OR_DS_TO_DATE(a)"), exp.TsOrDsToDate)
        self.assertIsInstance(parse_one("TS_OR_DS_TO_DATE_STR(a)"), exp.TsOrDsToDateStr)
        self.assertIsInstance(parse_one("UNIX_TO_STR(a, 'format')"), exp.UnixToStr)
        self.assertIsInstance(parse_one("UNIX_TO_TIME(a)"), exp.UnixToTime)
        self.assertIsInstance(parse_one("UNIX_TO_TIME_STR(a)"), exp.UnixToTimeStr)
        self.assertIsInstance(parse_one("VARIANCE(a)"), exp.Variance)
        self.assertIsInstance(parse_one("VARIANCE_POP(a)"), exp.VariancePop)
        self.assertIsInstance(parse_one("VARIANCE_SAMP(a)"), exp.VarianceSamp)
        self.assertIsInstance(parse_one("YEAR(a)"), exp.Year)

    def test_column(self):
        dot = parse_one("a.b.c")
        column = dot.this
        self.assertEqual(column.text("table"), "a")
        self.assertEqual(column.text("this"), "b")
        self.assertEqual(dot.text("expression"), "c")

        column = parse_one("a")
        self.assertEqual(column.text("this"), "a")
        self.assertIsNone(column.table)

        fields = parse_one("a.b.c.d")
        self.assertIsInstance(fields, exp.Dot)
        self.assertEqual(fields.text("expression"), "d")
        self.assertEqual(fields.this.text("expression"), "c")
        column = fields.find(exp.Column)
        self.assertEqual(column.text("this"), "b")
        self.assertEqual(column.text("table"), "a")

        column = parse_one("a[0].b")
        self.assertIsInstance(column, exp.Dot)
        self.assertIsInstance(column.this, exp.Bracket)
        self.assertIsInstance(column.this.this, exp.Column)

        column = parse_one("a.*")
        self.assertIsInstance(column, exp.Column)
        self.assertIsInstance(column.this, exp.Star)
        self.assertIsInstance(column.table, exp.Identifier)
        self.assertEqual(column.text("table"), "a")

        self.assertIsInstance(parse_one("*"), exp.Star)

    def test_text(self):
        column = parse_one("a.b.c")
        self.assertEqual(column.text("expression"), "c")
        self.assertEqual(column.text("y"), "")
        self.assertEqual(parse_one("select * from x.y").find(exp.Table).text("db"), "x")
        self.assertEqual(parse_one("select *").text("this"), "")
        self.assertEqual(parse_one("1 + 1").text("this"), "1")
        self.assertEqual(parse_one("'a'").text("this"), "a")

    def test_alias(self):
        self.assertEqual(exp.alias_("foo", "bar").sql(), "foo AS bar")
        self.assertEqual(exp.alias_("foo", "bar-1").sql(), 'foo AS "bar-1"')
        self.assertEqual(exp.alias_("foo", "bar_1").sql(), "foo AS bar_1")
        self.assertEqual(exp.alias_("foo * 2", "2bar").sql(), 'foo * 2 AS "2bar"')
        self.assertEqual(exp.alias_('"foo"', "_bar").sql(), '"foo" AS "_bar"')
        self.assertEqual(exp.alias_("foo", "bar", quoted=True).sql(), 'foo AS "bar"')
