import unittest
import doctest

from sqlglot import parse_one, select, from_, and_, or_, condition, not_
from sqlglot import expressions as exp


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests in the expressions module
    """
    tests.addTests(doctest.DocTestSuite(exp))
    return tests


class TestBuild(unittest.TestCase):
    def test_build(self):
        for expression, sql, *dialect in [
            (lambda: select("x"), "SELECT x"),
            (lambda: select("x", "y"), "SELECT x, y"),
            (lambda: select("x").from_("tbl"), "SELECT x FROM tbl"),
            (lambda: select("x", "y").from_("tbl"), "SELECT x, y FROM tbl"),
            (lambda: select("x").select("y").from_("tbl"), "SELECT x, y FROM tbl"),
            (
                lambda: select("x").select("y", append=False).from_("tbl"),
                "SELECT y FROM tbl",
            ),
            (lambda: select("x").from_("tbl").from_("tbl2"), "SELECT x FROM tbl, tbl2"),
            (
                lambda: select("x").from_("tbl, tbl2", "tbl3").from_("tbl4"),
                "SELECT x FROM tbl, tbl2, tbl3, tbl4",
            ),
            (
                lambda: select("x").from_("tbl").from_("tbl2", append=False),
                "SELECT x FROM tbl2",
            ),
            (lambda: select("SUM(x) AS y"), "SELECT SUM(x) AS y"),
            (
                lambda: select("x").from_("tbl").where("x > 0"),
                "SELECT x FROM tbl WHERE x > 0",
            ),
            (
                lambda: select("x").from_("tbl").where("x < 4 OR x > 5"),
                "SELECT x FROM tbl WHERE x < 4 OR x > 5",
            ),
            (
                lambda: select("x").from_("tbl").where("x > 0").where("x < 9"),
                "SELECT x FROM tbl WHERE x > 0 AND x < 9",
            ),
            (
                lambda: select("x").from_("tbl").where("x > 0", "x < 9"),
                "SELECT x FROM tbl WHERE x > 0 AND x < 9",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .where("x > 0")
                .where("x < 9", append=False),
                "SELECT x FROM tbl WHERE x < 9",
            ),
            (
                lambda: select("x", "y").from_("tbl").group_by("x"),
                "SELECT x, y FROM tbl GROUP BY x",
            ),
            (
                lambda: select("x", "y").from_("tbl").group_by("x, y"),
                "SELECT x, y FROM tbl GROUP BY x, y",
            ),
            (
                lambda: select("x", "y", "z", "a")
                .from_("tbl")
                .group_by("x, y", "z")
                .group_by("a"),
                "SELECT x, y, z, a FROM tbl GROUP BY x, y, z, a",
            ),
            (
                lambda: select("x").distinct(True).from_("tbl"),
                "SELECT DISTINCT x FROM tbl",
            ),
            (lambda: select("x").distinct(False).from_("tbl"), "SELECT x FROM tbl"),
            (
                lambda: select("x").lateral("OUTER explode(y) tbl2 AS z").from_("tbl"),
                "SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z",
            ),
            (
                lambda: select("x").from_("tbl").join("tbl2 ON tbl.y = tbl2.y"),
                "SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y",
            ),
            (
                lambda: select("x").from_("tbl").join("tbl2", on="tbl.y = tbl2.y"),
                "SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y",
            ),
            (
                lambda: select("x").from_("tbl").join("tbl2", join_type="left outer"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2",
            ),
            (
                lambda: select("x", "COUNT(y)")
                .from_("tbl")
                .group_by("x")
                .having("COUNT(y) > 0"),
                "SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 0",
            ),
            (
                lambda: select("x").from_("tbl").order_by("y"),
                "SELECT x FROM tbl ORDER BY y",
            ),
            (
                lambda: select("x").from_("tbl").order_by("x, y DESC"),
                "SELECT x FROM tbl ORDER BY x, y DESC",
            ),
            (
                lambda: select("x", "y", "z", "a")
                .from_("tbl")
                .order_by("x, y", "z")
                .order_by("a"),
                "SELECT x, y, z, a FROM tbl ORDER BY x, y, z, a",
            ),
            (lambda: select("x").from_("tbl").limit(10), "SELECT x FROM tbl LIMIT 10"),
            (
                lambda: select("x").from_("tbl").offset(10),
                "SELECT x FROM tbl OFFSET 10",
            ),
            (
                lambda: select("x").from_("tbl").with_("tbl", as_="SELECT x FROM tbl2"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_="SELECT x FROM tbl2", recursive=True),
                "WITH RECURSIVE tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_=select("x").from_("tbl2")),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl (x, y)", as_=select("x", "y").from_("tbl2")),
                "WITH tbl(x, y) AS (SELECT x, y FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .with_("tbl2", as_=select("x").from_("tbl3")),
                "WITH tbl AS (SELECT x FROM tbl2), tbl2 AS (SELECT x FROM tbl3) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_=select("x", "y").from_("tbl2"))
                .select("y"),
                "WITH tbl AS (SELECT x, y FROM tbl2) SELECT x, y FROM tbl",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .group_by("x"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl GROUP BY x",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .order_by("x"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl ORDER BY x",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .limit(10),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl LIMIT 10",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .offset(10),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl OFFSET 10",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .join("tbl3"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl JOIN tbl3",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .distinct(),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT DISTINCT x FROM tbl",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .where("x > 10"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl WHERE x > 10",
            ),
            (
                lambda: select("x")
                .with_("tbl", as_=select("x").from_("tbl2"))
                .from_("tbl")
                .having("x > 20"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl HAVING x > 20",
            ),
            (lambda: select("x").from_("tbl").subquery(), "(SELECT x FROM tbl)"),
            (
                lambda: select("x").from_("tbl").subquery("y"),
                "(SELECT x FROM tbl) AS y",
            ),
            (
                lambda: select("x").from_(select("x").from_("tbl").subquery()),
                "SELECT x FROM (SELECT x FROM tbl)",
            ),
            (lambda: from_("tbl").select("x"), "SELECT x FROM tbl"),
            (
                lambda: parse_one("SELECT a FROM tbl")
                .assert_is(exp.Select)
                .select("b"),
                "SELECT a, b FROM tbl",
            ),
            (
                lambda: parse_one("SELECT * FROM y").assert_is(exp.Select).ctas("x"),
                "CREATE TABLE x AS SELECT * FROM y",
            ),
            (
                lambda: parse_one("SELECT * FROM y")
                .assert_is(exp.Select)
                .ctas("foo.x", properties={"format": "parquet", "y": "2"}),
                "CREATE TABLE foo.x STORED AS PARQUET TBLPROPERTIES ('y' = '2') AS SELECT * FROM y",
                "hive",
            ),
            (lambda: and_("x=1", "y=1"), "x = 1 AND y = 1"),
            (lambda: condition("x=1").and_("y=1"), "x = 1 AND y = 1"),
            (lambda: and_("x=1", "y=1", "z=1"), "x = 1 AND y = 1 AND z = 1"),
            (lambda: condition("x=1").and_("y=1", "z=1"), "x = 1 AND y = 1 AND z = 1"),
            (lambda: and_("x=1", and_("y=1", "z=1")), "x = 1 AND (y = 1 AND z = 1)"),
            (
                lambda: condition("x=1").and_("y=1").and_("z=1"),
                "(x = 1 AND y = 1) AND z = 1",
            ),
            (lambda: or_(and_("x=1", "y=1"), "z=1"), "(x = 1 AND y = 1) OR z = 1"),
            (
                lambda: condition("x=1").and_("y=1").or_("z=1"),
                "(x = 1 AND y = 1) OR z = 1",
            ),
            (lambda: or_("z=1", and_("x=1", "y=1")), "z = 1 OR (x = 1 AND y = 1)"),
            (
                lambda: or_("z=1 OR a=1", and_("x=1", "y=1")),
                "(z = 1 OR a = 1) OR (x = 1 AND y = 1)",
            ),
            (lambda: not_("x=1"), "NOT x = 1"),
            (lambda: condition("x=1").not_(), "NOT x = 1"),
            (lambda: condition("x=1").and_("y=1").not_(), "NOT (x = 1 AND y = 1)"),
            (
                lambda: select("*").from_("x").where(condition("y=1").and_("z=1")),
                "SELECT * FROM x WHERE y = 1 AND z = 1",
            ),
        ]:
            with self.subTest(sql):
                self.assertEqual(expression().sql(dialect[0] if dialect else None), sql)
