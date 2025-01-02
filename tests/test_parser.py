import time
import unittest
from unittest.mock import patch

from sqlglot import Parser, exp, parse, parse_one
from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.parser import logger as parser_logger
from tests.helpers import assert_logger_contains


class TestParser(unittest.TestCase):
    def test_parse_empty(self):
        with self.assertRaises(ParseError):
            parse_one("")

    def test_parse_into(self):
        self.assertIsInstance(parse_one("(1)", into=exp.Tuple), exp.Tuple)
        self.assertIsInstance(parse_one("(1,)", into=exp.Tuple), exp.Tuple)
        self.assertIsInstance(parse_one("(x=1)", into=exp.Tuple), exp.Tuple)

        self.assertIsInstance(parse_one("select * from t", into=exp.Select), exp.Select)
        self.assertIsInstance(parse_one("select * from t limit 5", into=exp.Select), exp.Select)
        self.assertIsInstance(parse_one("left join foo", into=exp.Join), exp.Join)
        self.assertIsInstance(parse_one("int", into=exp.DataType), exp.DataType)
        self.assertIsInstance(parse_one("array<int>", into=exp.DataType), exp.DataType)
        self.assertIsInstance(parse_one("foo", into=exp.Table), exp.Table)
        self.assertIsInstance(
            parse_one(
                "WHEN MATCHED THEN UPDATE SET target.salary = COALESCE(source.salary, target.salary)",
                into=exp.Whens,
            ),
            exp.Whens,
        )

        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT * FROM tbl", into=exp.Table)

        self.assertEqual(
            str(ctx.exception),
            "Failed to parse 'SELECT * FROM tbl' into <class 'sqlglot.expressions.Table'>",
        )

    def test_parse_into_error(self):
        expected_message = "Failed to parse 'SELECT 1;' into [<class 'sqlglot.expressions.From'>]"
        expected_errors = [
            {
                "description": "Invalid expression / Unexpected token",
                "line": 1,
                "col": 6,
                "start_context": "",
                "highlight": "SELECT",
                "end_context": " 1;",
                "into_expression": exp.From,
            }
        ]
        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT 1;", read="sqlite", into=[exp.From])

        self.assertEqual(str(ctx.exception), expected_message)
        self.assertEqual(ctx.exception.errors, expected_errors)

    def test_parse_into_errors(self):
        expected_message = "Failed to parse 'SELECT 1;' into [<class 'sqlglot.expressions.From'>, <class 'sqlglot.expressions.Join'>]"
        expected_errors = [
            {
                "description": "Invalid expression / Unexpected token",
                "line": 1,
                "col": 6,
                "start_context": "",
                "highlight": "SELECT",
                "end_context": " 1;",
                "into_expression": exp.From,
            },
            {
                "description": "Invalid expression / Unexpected token",
                "line": 1,
                "col": 6,
                "start_context": "",
                "highlight": "SELECT",
                "end_context": " 1;",
                "into_expression": exp.Join,
            },
        ]
        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT 1;", "sqlite", into=[exp.From, exp.Join])

        self.assertEqual(str(ctx.exception), expected_message)
        self.assertEqual(ctx.exception.errors, expected_errors)

    def test_column(self):
        columns = parse_one("select a, ARRAY[1] b, case when 1 then 1 end").find_all(exp.Column)
        assert len(list(columns)) == 1

        self.assertIsNotNone(parse_one("date").find(exp.Column))

    def test_tuple(self):
        parse_one("(a,)").assert_is(exp.Tuple)

    def test_structs(self):
        cast = parse_one("cast(x as struct<int>)")
        self.assertIsInstance(cast.to.expressions[0], exp.DataType)
        self.assertEqual(cast.sql(), "CAST(x AS STRUCT<INT>)")

        cast = parse_one("cast(x as struct<varchar(10)>)")
        self.assertIsInstance(cast.to.expressions[0], exp.DataType)
        self.assertEqual(cast.sql(), "CAST(x AS STRUCT<VARCHAR(10)>)")

    def test_float(self):
        self.assertEqual(parse_one(".2"), parse_one("0.2"))

    def test_unnest(self):
        unnest_sql = "UNNEST(foo)"
        expr = parse_one(unnest_sql)
        self.assertIsInstance(expr, exp.Unnest)
        self.assertIsInstance(expr.expressions, list)
        self.assertEqual(expr.sql(), unnest_sql)

    def test_unnest_projection(self):
        expr = parse_one("SELECT foo IN UNNEST(bla) AS bar")
        self.assertIsInstance(expr.selects[0], exp.Alias)
        self.assertEqual(expr.selects[0].output_name, "bar")
        self.assertIsNotNone(parse_one("select unnest(x)").find(exp.Unnest))

    def test_unary_plus(self):
        self.assertEqual(parse_one("+15"), exp.Literal.number(15))

    def test_table(self):
        tables = [t.sql() for t in parse_one("select * from a, b.c, .d").find_all(exp.Table)]
        self.assertEqual(set(tables), {"a", "b.c", "d"})

    def test_union(self):
        self.assertIsInstance(parse_one("SELECT * FROM (SELECT 1) UNION SELECT 2"), exp.Union)
        self.assertIsInstance(
            parse_one("SELECT x FROM y HAVING x > (SELECT 1) UNION SELECT 2"), exp.Union
        )

        # Check that modifiers are attached to the topmost union node and not the rightmost query
        single_union = "SELECT x FROM t1 UNION ALL SELECT x FROM t2 LIMIT 1"
        expr = parse_one(single_union)
        limit = expr.assert_is(exp.Union).args.get("limit")
        self.assertIsInstance(limit, exp.Limit)
        self.assertEqual(expr.sql(), single_union)

        two_unions = (
            "SELECT x FROM t1 UNION ALL SELECT x FROM t2 UNION ALL SELECT x FROM t3 LIMIT 1"
        )
        expr = parse_one(two_unions)
        limit = expr.assert_is(exp.Union).args.get("limit")
        self.assertIsInstance(limit, exp.Limit)
        self.assertEqual(expr.sql(), two_unions)

        expr = parse_one(single_union, read="clickhouse")
        self.assertIsNone(expr.args.get("limit"))
        self.assertEqual(expr.sql(dialect="clickhouse"), single_union)

    def test_select(self):
        self.assertIsNotNone(parse_one("select 1 natural"))
        self.assertIsNotNone(parse_one("select * from (select 1) x order by x.y").args["order"])
        self.assertIsNotNone(
            parse_one("select * from x where a = (select 1) order by x.y").args["order"]
        )
        self.assertEqual(len(parse_one("select * from (select 1) x cross join y").args["joins"]), 1)
        self.assertEqual(
            parse_one("""SELECT * FROM x CROSS JOIN y, z LATERAL VIEW EXPLODE(y)""").sql(),
            """SELECT * FROM x CROSS JOIN y, z LATERAL VIEW EXPLODE(y)""",
        )
        self.assertIsNone(
            parse_one("create table a as (select b from c) index").find(exp.TableAlias)
        )

    def test_command(self):
        with self.assertLogs(parser_logger) as cm:
            expressions = parse("SET x = 1; ADD JAR s3://a; SELECT 1", read="hive")
            self.assertEqual(len(expressions), 3)
            self.assertEqual(expressions[0].sql(), "SET x = 1")
            self.assertEqual(expressions[1].sql(), "ADD JAR s3://a")
            self.assertEqual(expressions[2].sql(), "SELECT 1")

        assert "'ADD JAR s3://a'" in cm.output[0]

    def test_lambda_struct(self):
        expression = parse_one("FILTER(a.b, x -> x.id = id)")
        lambda_expr = expression.expression

        self.assertIsInstance(lambda_expr.this.this, exp.Dot)
        self.assertEqual(lambda_expr.sql(), "x -> x.id = id")

        self.assertIsNone(parse_one("FILTER([], x -> x)").find(exp.Column))

    def test_transactions(self):
        expression = parse_one("BEGIN TRANSACTION")
        self.assertIsNone(expression.this)
        self.assertEqual(expression.args["modes"], [])
        self.assertEqual(expression.sql(), "BEGIN")

        expression = parse_one("START TRANSACTION", read="mysql")
        self.assertIsNone(expression.this)
        self.assertEqual(expression.args["modes"], [])
        self.assertEqual(expression.sql(), "BEGIN")

        expression = parse_one("BEGIN DEFERRED TRANSACTION")
        self.assertEqual(expression.this, "DEFERRED")
        self.assertEqual(expression.args["modes"], [])
        self.assertEqual(expression.sql(), "BEGIN")

        expression = parse_one(
            "START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE", read="presto"
        )
        self.assertIsNone(expression.this)
        self.assertEqual(expression.args["modes"][0], "READ WRITE")
        self.assertEqual(expression.args["modes"][1], "ISOLATION LEVEL SERIALIZABLE")
        self.assertEqual(expression.sql(), "BEGIN")

        expression = parse_one("BEGIN", read="bigquery")
        self.assertNotIsInstance(expression, exp.Transaction)
        self.assertIsNone(expression.expression)
        self.assertEqual(expression.sql(), "BEGIN")

    def test_identify(self):
        expression = parse_one(
            """
            SELECT a, "b", c AS c, d AS "D", e AS "y|z'"
            FROM y."z"
        """
        )

        assert expression.expressions[0].name == "a"
        assert expression.expressions[1].name == "b"
        assert expression.expressions[2].alias == "c"
        assert expression.expressions[3].alias == "D"
        assert expression.expressions[4].alias == "y|z'"
        table = expression.args["from"].this
        assert table.name == "z"
        assert table.args["db"].name == "y"

    def test_multi(self):
        expressions = parse(
            """
            SELECT * FROM a; SELECT * FROM b;
        """
        )

        assert len(expressions) == 2
        assert expressions[0].args["from"].name == "a"
        assert expressions[1].args["from"].name == "b"

        expressions = parse("SELECT 1; ; SELECT 2")

        assert len(expressions) == 3
        assert expressions[1] is None

    def test_expression(self):
        ignore = Parser(error_level=ErrorLevel.IGNORE)
        self.assertIsInstance(ignore.expression(exp.Hint, expressions=[""]), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint, y=""), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint), exp.Hint)

        default = Parser(error_level=ErrorLevel.RAISE)
        self.assertIsInstance(default.expression(exp.Hint, expressions=[""]), exp.Hint)
        default.expression(exp.Hint, y="")
        default.expression(exp.Hint)
        self.assertEqual(len(default.errors), 3)

        warn = Parser(error_level=ErrorLevel.WARN)
        warn.expression(exp.Hint, y="")
        self.assertEqual(len(warn.errors), 2)

    def test_parse_errors(self):
        with self.assertRaises(ParseError):
            parse_one("IF(a > 0, a, b, c)")

        with self.assertRaises(ParseError):
            parse_one("IF(a > 0)")

        with self.assertRaises(ParseError):
            parse_one("SELECT CASE FROM x")

        with self.assertRaises(ParseError):
            parse_one("WITH cte AS (SELECT * FROM x)")

        with self.assertRaises(ParseError):
            parse_one("SELECT foo( FROM bar")

        self.assertEqual(
            parse_one(
                "CREATE TABLE t (i UInt8) ENGINE = AggregatingMergeTree() ORDER BY tuple()",
                read="clickhouse",
                error_level=ErrorLevel.RAISE,
            ).sql(dialect="clickhouse"),
            "CREATE TABLE t (i UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()",
        )

        with self.assertRaises(ParseError):
            parse_one("SELECT A[:")

        self.assertEqual(parse_one("as as", error_level=ErrorLevel.IGNORE).sql(), "AS as")

    def test_space(self):
        self.assertEqual(
            parse_one("SELECT ROW() OVER(PARTITION  BY x) FROM x GROUP  BY y").sql(),
            "SELECT ROW() OVER (PARTITION BY x) FROM x GROUP BY y",
        )

        self.assertEqual(
            parse_one(
                """SELECT   * FROM x GROUP
                BY y"""
            ).sql(),
            "SELECT * FROM x GROUP BY y",
        )

    def test_missing_by(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT FROM x ORDER BY")

    def test_parameter(self):
        self.assertEqual(parse_one("SELECT @x, @@x, @1").sql(), "SELECT @x, @@x, @1")

    def test_var(self):
        self.assertIsInstance(parse_one("INTERVAL '1' DAY").args["unit"], exp.Var)
        self.assertEqual(parse_one("SELECT @JOIN, @'foo'").sql(), "SELECT @JOIN, @'foo'")

    def test_comments_select(self):
        expression = parse_one(
            """
            --comment1.1
            --comment1.2
            SELECT /*comment1.3*/
                a, --comment2
                b as B, --comment3:testing
                "test--annotation",
                c, --comment4 --foo
                e, --
                f -- space
            FROM foo
            """
        )

        self.assertEqual(expression.comments, ["comment1.1", "comment1.2", "comment1.3"])
        self.assertEqual(expression.expressions[0].comments, ["comment2"])
        self.assertEqual(expression.expressions[1].comments, ["comment3:testing"])
        self.assertEqual(expression.expressions[2].comments, None)
        self.assertEqual(expression.expressions[3].comments, ["comment4 --foo"])
        self.assertEqual(expression.expressions[4].comments, [""])
        self.assertEqual(expression.expressions[5].comments, [" space"])

        expression = parse_one(
            """
            SELECT a.column_name --# Comment 1
                   ,b.column_name2, --# Comment 2
                   b.column_name3 AS NAME3 --# Comment 3
            FROM table_name a
            JOIN table_name2 b ON a.column_name = b.column_name
            """
        )

        self.assertEqual(expression.expressions[0].comments, ["# Comment 1"])
        self.assertEqual(expression.expressions[1].comments, ["# Comment 2"])
        self.assertEqual(expression.expressions[2].comments, ["# Comment 3"])

    def test_comments_select_cte(self):
        expression = parse_one(
            """
            /*comment1.1*/
            /*comment1.2*/
            WITH a AS (SELECT 1)
            SELECT /*comment2*/
                a.*
            FROM /*comment3*/
                a
            """
        )

        self.assertEqual(expression.comments, ["comment2"])
        self.assertEqual(expression.args.get("from").comments, ["comment3"])
        self.assertEqual(expression.args.get("with").comments, ["comment1.1", "comment1.2"])

    def test_comments_insert(self):
        expression = parse_one(
            """
            --comment1.1
            --comment1.2
            INSERT INTO /*comment1.3*/
                x       /*comment2*/
            VALUES      /*comment3*/
                (1, 'a', 2.0)
        """
        )

        self.assertEqual(expression.comments, ["comment1.1", "comment1.2", "comment1.3"])
        self.assertEqual(expression.this.comments, ["comment2"])

    def test_comments_insert_cte(self):
        expression = parse_one(
            """
            /*comment1.1*/
            /*comment1.2*/
            WITH a AS (SELECT 1)
            INSERT INTO /*comment2*/
                b /*comment3*/
            SELECT * FROM a
        """
        )

        self.assertEqual(expression.comments, ["comment2"])
        self.assertEqual(expression.this.comments, ["comment3"])
        self.assertEqual(expression.args.get("with").comments, ["comment1.1", "comment1.2"])

    def test_comments_update(self):
        expression = parse_one(
            """
            --comment1.1
            --comment1.2
            UPDATE  /*comment1.3*/
                tbl /*comment2*/
            SET     /*comment3*/
                x = 2
            WHERE /*comment4*/
                x <> 2
        """
        )

        self.assertEqual(expression.comments, ["comment1.1", "comment1.2", "comment1.3"])
        self.assertEqual(expression.this.comments, ["comment2"])
        self.assertEqual(expression.args.get("where").comments, ["comment4"])

    def test_comments_update_cte(self):
        expression = parse_one(
            """
            /*comment1.1*/
            /*comment1.2*/
            WITH a AS (SELECT * FROM b)
            UPDATE /*comment2*/
                a  /*comment3*/
            SET col = 1
        """
        )

        self.assertEqual(expression.comments, ["comment2"])
        self.assertEqual(expression.this.comments, ["comment3"])
        self.assertEqual(expression.args.get("with").comments, ["comment1.1", "comment1.2"])

    def test_comments_delete(self):
        expression = parse_one(
            """
            --comment1.1
            --comment1.2
            DELETE /*comment1.3*/
            FROM   /*comment2*/
                x  /*comment3*/
            WHERE  /*comment4*/
                y > 1
        """
        )

        self.assertEqual(expression.comments, ["comment1.1", "comment1.2", "comment1.3"])
        self.assertEqual(expression.this.comments, ["comment3"])
        self.assertEqual(expression.args.get("where").comments, ["comment4"])

    def test_comments_delete_cte(self):
        expression = parse_one(
            """
            /*comment1.1*/
            /*comment1.2*/
            WITH a AS (SELECT * FROM b)
            --comment2
            DELETE FROM a /*comment3*/
        """
        )

        self.assertEqual(expression.comments, ["comment2"])
        self.assertEqual(expression.this.comments, ["comment3"])
        self.assertEqual(expression.args["with"].comments, ["comment1.1", "comment1.2"])

    def test_type_literals(self):
        self.assertEqual(parse_one("int 1"), parse_one("CAST(1 AS INT)"))
        self.assertEqual(parse_one("int.5"), parse_one("CAST(0.5 AS INT)"))
        self.assertEqual(
            parse_one("TIMESTAMP '2022-01-01'").sql(), "CAST('2022-01-01' AS TIMESTAMP)"
        )
        self.assertEqual(
            parse_one("TIMESTAMP(1) '2022-01-01'").sql(), "CAST('2022-01-01' AS TIMESTAMP(1))"
        )
        self.assertEqual(
            parse_one("TIMESTAMP WITH TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMPTZ)",
        )
        self.assertEqual(
            parse_one("TIMESTAMP WITH LOCAL TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMPLTZ)",
        )
        self.assertEqual(
            parse_one("TIMESTAMP WITHOUT TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMP)",
        )
        self.assertEqual(
            parse_one("TIMESTAMP(1) WITH TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMPTZ(1))",
        )
        self.assertEqual(
            parse_one("TIMESTAMP(1) WITH LOCAL TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMPLTZ(1))",
        )
        self.assertEqual(
            parse_one("TIMESTAMP(1) WITHOUT TIME ZONE '2022-01-01'").sql(),
            "CAST('2022-01-01' AS TIMESTAMP(1))",
        )
        self.assertEqual(parse_one("TIMESTAMP(1) WITH TIME ZONE").sql(), "TIMESTAMPTZ(1)")
        self.assertEqual(parse_one("TIMESTAMP(1) WITH LOCAL TIME ZONE").sql(), "TIMESTAMPLTZ(1)")
        self.assertEqual(parse_one("TIMESTAMP(1) WITHOUT TIME ZONE").sql(), "TIMESTAMP(1)")
        self.assertEqual(parse_one("""JSON '{"x":"y"}'""").sql(), """PARSE_JSON('{"x":"y"}')""")
        self.assertIsInstance(parse_one("TIMESTAMP(1)"), exp.Func)
        self.assertIsInstance(parse_one("TIMESTAMP('2022-01-01')"), exp.Func)
        self.assertIsInstance(parse_one("TIMESTAMP()"), exp.Func)
        self.assertIsInstance(parse_one("map.x"), exp.Column)
        self.assertIsInstance(parse_one("CAST(x AS CHAR(5))").to.expressions[0], exp.DataTypeParam)
        self.assertEqual(parse_one("1::int64", dialect="bigquery"), parse_one("CAST(1 AS BIGINT)"))

    def test_set_expression(self):
        set_ = parse_one("SET")

        self.assertEqual(set_.sql(), "SET")
        self.assertIsInstance(set_, exp.Set)

        set_session = parse_one("SET SESSION x = 1")

        self.assertEqual(set_session.sql(), "SET SESSION x = 1")
        self.assertIsInstance(set_session, exp.Set)

        set_item = set_session.expressions[0]

        self.assertIsInstance(set_item, exp.SetItem)
        self.assertIsInstance(set_item.this, exp.EQ)
        self.assertIsInstance(set_item.this.this, exp.Column)
        self.assertIsInstance(set_item.this.expression, exp.Literal)

        self.assertEqual(set_item.args.get("kind"), "SESSION")

        set_to = parse_one("SET x TO 1")

        self.assertEqual(set_to.sql(), "SET x = 1")
        self.assertIsInstance(set_to, exp.Set)

        with self.assertLogs(parser_logger) as cm:
            set_as_command = parse_one("SET DEFAULT ROLE ALL TO USER")
            assert "'SET DEFAULT ROLE ALL TO USER'" in cm.output[0]

        self.assertEqual(set_as_command.sql(), "SET DEFAULT ROLE ALL TO USER")

        self.assertIsInstance(set_as_command, exp.Command)
        self.assertEqual(set_as_command.this, "SET")
        self.assertEqual(set_as_command.expression, " DEFAULT ROLE ALL TO USER")

    def test_pretty_config_override(self):
        self.assertEqual(parse_one("SELECT col FROM x").sql(), "SELECT col FROM x")
        with patch("sqlglot.pretty", True):
            self.assertEqual(parse_one("SELECT col FROM x").sql(), "SELECT\n  col\nFROM x")

        self.assertEqual(parse_one("SELECT col FROM x").sql(pretty=True), "SELECT\n  col\nFROM x")

    @patch("sqlglot.parser.logger")
    def test_comment_error_n(self, logger):
        parse_one(
            """SUM
(
-- test
)""",
            error_level=ErrorLevel.WARN,
        )

        assert_logger_contains(
            "Required keyword: 'this' missing for <class 'sqlglot.expressions.Sum'>. Line 4, Col: 1.",
            logger,
        )

    @patch("sqlglot.parser.logger")
    def test_comment_error_r(self, logger):
        parse_one(
            """SUM(-- test\r)""",
            error_level=ErrorLevel.WARN,
        )

        assert_logger_contains(
            "Required keyword: 'this' missing for <class 'sqlglot.expressions.Sum'>. Line 2, Col: 1.",
            logger,
        )

    @patch("sqlglot.parser.logger")
    def test_create_table_error(self, logger):
        parse_one(
            """CREATE TABLE SELECT""",
            error_level=ErrorLevel.WARN,
        )

        assert_logger_contains(
            "Expected table name",
            logger,
        )

    def test_pivot_columns(self):
        nothing_aliased = """
            SELECT * FROM (
                SELECT partname, price FROM part
            ) PIVOT (AVG(price) FOR partname IN ('prop', 'rudder'))
        """

        everything_aliased = """
            SELECT * FROM (
                SELECT partname, price FROM part
            ) PIVOT (AVG(price) AS avg_price FOR partname IN ('prop' AS prop1, 'rudder' AS rudder1))
        """

        only_pivot_columns_aliased = """
            SELECT * FROM (
                SELECT partname, price FROM part
            ) PIVOT (AVG(price) FOR partname IN ('prop' AS prop1, 'rudder' AS rudder1))
        """

        columns_partially_aliased = """
            SELECT * FROM (
                SELECT partname, price FROM part
            ) PIVOT (AVG(price) FOR partname IN ('prop' AS prop1, 'rudder'))
        """

        multiple_aggregates_aliased = """
            SELECT * FROM (
                SELECT partname, price, quality FROM part
            ) PIVOT (AVG(price) AS p, MAX(quality) AS q FOR partname IN ('prop' AS prop1, 'rudder'))
        """

        multiple_aggregates_not_aliased = """
            SELECT * FROM (
                SELECT partname, price, quality FROM part
            ) PIVOT (AVG(price), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
        """

        multiple_aggregates_not_aliased_with_quoted_identifier_spark = """
            SELECT * FROM (
                SELECT partname, price, quality FROM part
            ) PIVOT (AVG(`PrIcE`), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
        """

        multiple_aggregates_not_aliased_with_quoted_identifier_duckdb = """
            SELECT * FROM (
                SELECT partname, price, quality FROM part
            ) PIVOT (AVG("PrIcE"), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
        """

        query_to_column_names = {
            nothing_aliased: {
                "bigquery": ["prop", "rudder"],
                "duckdb": ["prop", "rudder"],
                "redshift": ["prop", "rudder"],
                "snowflake": ['''"'prop'"''', '''"'rudder'"'''],
                "spark": ["prop", "rudder"],
            },
            everything_aliased: {
                "bigquery": ["avg_price_prop1", "avg_price_rudder1"],
                "duckdb": ["prop1_avg_price", "rudder1_avg_price"],
                "redshift": ["prop1_avg_price", "rudder1_avg_price"],
                "spark": ["prop1", "rudder1"],
            },
            only_pivot_columns_aliased: {
                "bigquery": ["prop1", "rudder1"],
                "duckdb": ["prop1", "rudder1"],
                "redshift": ["prop1", "rudder1"],
                "spark": ["prop1", "rudder1"],
            },
            columns_partially_aliased: {
                "bigquery": ["prop1", "rudder"],
                "duckdb": ["prop1", "rudder"],
                "redshift": ["prop1", "rudder"],
                "spark": ["prop1", "rudder"],
            },
            multiple_aggregates_aliased: {
                "bigquery": ["p_prop1", "q_prop1", "p_rudder", "q_rudder"],
                "duckdb": ["prop1_p", "prop1_q", "rudder_p", "rudder_q"],
                "spark": ["prop1_p", "prop1_q", "rudder_p", "rudder_q"],
            },
            multiple_aggregates_not_aliased: {
                "duckdb": [
                    '"prop1_avg(price)"',
                    '"prop1_max(quality)"',
                    '"rudder_avg(price)"',
                    '"rudder_max(quality)"',
                ],
                "spark": [
                    "`prop1_avg(price)`",
                    "`prop1_max(quality)`",
                    "`rudder_avg(price)`",
                    "`rudder_max(quality)`",
                ],
            },
            multiple_aggregates_not_aliased_with_quoted_identifier_spark: {
                "spark": [
                    "`prop1_avg(PrIcE)`",
                    "`prop1_max(quality)`",
                    "`rudder_avg(PrIcE)`",
                    "`rudder_max(quality)`",
                ],
            },
            multiple_aggregates_not_aliased_with_quoted_identifier_duckdb: {
                "duckdb": [
                    '"prop1_avg(PrIcE)"',
                    '"prop1_max(quality)"',
                    '"rudder_avg(PrIcE)"',
                    '"rudder_max(quality)"',
                ],
            },
        }

        for query, dialect_columns in query_to_column_names.items():
            for dialect, expected_columns in dialect_columns.items():
                expr = parse_one(query, read=dialect)
                columns = expr.args["from"].this.args["pivots"][0].args["columns"]
                self.assertEqual(expected_columns, [col.sql(dialect=dialect) for col in columns])

    def test_parse_nested(self):
        def warn_over_threshold(query: str, max_threshold: float = 0.2):
            now = time.time()
            ast = parse_one(query)
            end = time.time() - now

            self.assertIsNotNone(ast)
            if end >= max_threshold:
                parser_logger.warning(
                    f"Query {query[:100]}... surpassed the time threshold of {max_threshold} seconds"
                )

        warn_over_threshold("SELECT * FROM a " + ("LEFT JOIN b ON a.id = b.id " * 38))
        warn_over_threshold("SELECT * FROM a " + ("LEFT JOIN UNNEST(ARRAY[]) " * 15))
        warn_over_threshold("SELECT * FROM a " + ("OUTER APPLY (SELECT * FROM b) " * 30))

    def test_parse_properties(self):
        self.assertEqual(
            parse_one("create materialized table x").sql(), "CREATE MATERIALIZED TABLE x"
        )

    def test_parse_floats(self):
        self.assertTrue(parse_one("1. ").is_number)

    def test_parse_terse_coalesce(self):
        self.assertIsNotNone(parse_one("SELECT x ?? y FROM z").find(exp.Coalesce))
        self.assertEqual(
            parse_one("SELECT a, b ?? 'No Data' FROM z").sql(),
            "SELECT a, COALESCE(b, 'No Data') FROM z",
        )
        self.assertEqual(
            parse_one("SELECT a, b ?? c ?? 'No Data' FROM z").sql(),
            "SELECT a, COALESCE(COALESCE(b, c), 'No Data') FROM z",
        )

    def test_parse_intervals(self):
        ast = parse_one(
            "SELECT a FROM tbl WHERE a <= DATE '1998-12-01' - INTERVAL '71 days' GROUP BY b"
        )

        self.assertEqual(ast.find(exp.Interval).this.sql(), "'71'")
        self.assertEqual(ast.find(exp.Interval).unit.assert_is(exp.Var).sql(), "DAYS")

    def test_parse_concat_ws(self):
        ast = parse_one("CONCAT_WS(' ', 'John', 'Doe')")

        self.assertEqual(ast.sql(), "CONCAT_WS(' ', 'John', 'Doe')")
        self.assertEqual(ast.expressions[0].sql(), "' '")
        self.assertEqual(ast.expressions[1].sql(), "'John'")
        self.assertEqual(ast.expressions[2].sql(), "'Doe'")

        # Ensure we can parse without argument when error level is ignore
        ast = parse(
            "CONCAT_WS()",
            error_level=ErrorLevel.IGNORE,
        )
        self.assertEqual(ast[0].sql(), "CONCAT_WS()")

    def test_parse_drop_schema(self):
        for dialect in [None, "bigquery", "snowflake"]:
            with self.subTest(dialect):
                ast = parse_one("DROP SCHEMA catalog.schema", dialect=dialect)
                self.assertEqual(
                    ast,
                    exp.Drop(
                        this=exp.Table(
                            this=None,
                            db=exp.Identifier(this="schema", quoted=False),
                            catalog=exp.Identifier(this="catalog", quoted=False),
                        ),
                        kind="SCHEMA",
                    ),
                )
                self.assertEqual(ast.sql(dialect=dialect), "DROP SCHEMA catalog.schema")

    def test_parse_create_schema(self):
        for dialect in [None, "bigquery", "snowflake"]:
            with self.subTest(dialect):
                ast = parse_one("CREATE SCHEMA catalog.schema", dialect=dialect)
                self.assertEqual(
                    ast,
                    exp.Create(
                        this=exp.Table(
                            this=None,
                            db=exp.Identifier(this="schema", quoted=False),
                            catalog=exp.Identifier(this="catalog", quoted=False),
                        ),
                        kind="SCHEMA",
                    ),
                )
                self.assertEqual(ast.sql(dialect=dialect), "CREATE SCHEMA catalog.schema")

    def test_values_as_identifier(self):
        sql = "SELECT values FROM t WHERE values + 1 > x"
        for dialect in (
            "bigquery",
            "clickhouse",
            "duckdb",
            "postgres",
            "redshift",
            "snowflake",
        ):
            with self.subTest(dialect):
                self.assertEqual(parse_one(sql, dialect=dialect).sql(dialect=dialect), sql)

    def test_alter_set(self):
        sqls = [
            "ALTER TABLE tbl SET TBLPROPERTIES ('x'='1', 'Z'='2')",
            "ALTER TABLE tbl SET SERDE 'test' WITH SERDEPROPERTIES ('k'='v', 'kay'='vee')",
            "ALTER TABLE tbl SET SERDEPROPERTIES ('k'='v', 'kay'='vee')",
            "ALTER TABLE tbl SET LOCATION 'new_location'",
            "ALTER TABLE tbl SET FILEFORMAT file_format",
            "ALTER TABLE tbl SET TAGS ('tag1' = 't1', 'tag2' = 't2')",
        ]

        for dialect in (
            "hive",
            "spark2",
            "spark",
            "databricks",
        ):
            for sql in sqls:
                with self.subTest(f"Testing query '{sql}' for dialect {dialect}"):
                    self.assertEqual(parse_one(sql, dialect=dialect).sql(dialect=dialect), sql)

    def test_distinct_from(self):
        self.assertIsInstance(parse_one("a IS DISTINCT FROM b OR c IS DISTINCT FROM d"), exp.Or)

    def test_trailing_comments(self):
        expressions = parse(
            """
        select * from x;
        -- my comment
            """
        )

        self.assertEqual(
            ";\n".join(e.sql() for e in expressions), "SELECT * FROM x;\n/* my comment */"
        )

    def test_parse_prop_eq(self):
        self.assertIsInstance(parse_one("x(a := b and c)").expressions[0], exp.PropertyEQ)

    def test_collate(self):
        collates = [
            ('pg_catalog."default"', exp.Column),
            ('"en_DE"', exp.Identifier),
            ("LATIN1_GENERAL_BIN", exp.Var),
            ("'en'", exp.Literal),
        ]

        for collate_pair in collates:
            collate_node = parse_one(
                f"""SELECT * FROM t WHERE foo LIKE '%bar%' COLLATE {collate_pair[0]}"""
            ).find(exp.Collate)
            self.assertIsInstance(collate_node, exp.Collate)
            self.assertIsInstance(collate_node.expression, collate_pair[1])

    def test_odbc_date_literals(self):
        for value, cls in [
            ("{d'2024-01-01'}", exp.Date),
            ("{t'12:00:00'}", exp.Time),
            ("{ts'2024-01-01 12:00:00'}", exp.Timestamp),
        ]:
            sql = f"INSERT INTO tab(ds) VALUES ({value})"
            expr = parse_one(sql)
            self.assertIsInstance(expr, exp.Insert)
            self.assertIsInstance(expr.expression.expressions[0].expressions[0], cls)

    def test_drop_column(self):
        ast = parse_one("ALTER TABLE tbl DROP COLUMN col")
        self.assertEqual(len(list(ast.find_all(exp.Table))), 1)
        self.assertEqual(len(list(ast.find_all(exp.Column))), 1)

    def test_udf_meta(self):
        ast = parse_one("YEAR(a) /* sqlglot.anonymous */")
        self.assertIsInstance(ast, exp.Anonymous)

        # Meta flag is case sensitive
        ast = parse_one("YEAR(a) /* sqlglot.anONymous */")
        self.assertIsInstance(ast, exp.Year)

        # Incomplete or incorrect anonymous meta comments are not registered
        ast = parse_one("YEAR(a) /* sqlglot.anon */")
        self.assertIsInstance(ast, exp.Year)
