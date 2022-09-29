import unittest

from sqlglot import and_, condition, exp, from_, not_, or_, parse_one, select


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
                lambda: select("x").from_("tbl").where(None).where(False, ""),
                "SELECT x FROM tbl WHERE FALSE",
            ),
            (
                lambda: select("x").from_("tbl").where("x > 0").where("x < 9", append=False),
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
                lambda: select("x", "y", "z", "a").from_("tbl").group_by("x, y", "z").group_by("a"),
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
                lambda: select("x").from_("tbl").join("tbl2", on=["tbl.y = tbl2.y", "a = b"]),
                "SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y AND a = b",
            ),
            (
                lambda: select("x").from_("tbl").join("tbl2", join_type="left outer"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2",
            ),
            (
                lambda: select("x").from_("tbl").join(exp.Table(this="tbl2"), join_type="left outer"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2",
            ),
            (
                lambda: select("x").from_("tbl").join(exp.Table(this="tbl2"), join_type="left outer", join_alias="foo"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2 AS foo",
            ),
            (
                lambda: select("x").from_("tbl").join(select("y").from_("tbl2"), join_type="left outer"),
                "SELECT x FROM tbl LEFT OUTER JOIN (SELECT y FROM tbl2)",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join(
                    select("y").from_("tbl2").subquery("aliased"),
                    join_type="left outer",
                ),
                "SELECT x FROM tbl LEFT OUTER JOIN (SELECT y FROM tbl2) AS aliased",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join(
                    select("y").from_("tbl2"),
                    join_type="left outer",
                    join_alias="aliased",
                ),
                "SELECT x FROM tbl LEFT OUTER JOIN (SELECT y FROM tbl2) AS aliased",
            ),
            (
                lambda: select("x").from_("tbl").join(parse_one("left join x", into=exp.Join), on="a=b"),
                "SELECT x FROM tbl LEFT JOIN x ON a = b",
            ),
            (
                lambda: select("x").from_("tbl").join("left join x", on="a=b"),
                "SELECT x FROM tbl LEFT JOIN x ON a = b",
            ),
            (
                lambda: select("x").from_("tbl").join("select b from tbl2", on="a=b", join_type="left"),
                "SELECT x FROM tbl LEFT JOIN (SELECT b FROM tbl2) ON a = b",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join(
                    "select b from tbl2",
                    on="a=b",
                    join_type="left",
                    join_alias="aliased",
                ),
                "SELECT x FROM tbl LEFT JOIN (SELECT b FROM tbl2) AS aliased ON a = b",
            ),
            (
                lambda: select("x", "COUNT(y)").from_("tbl").group_by("x").having("COUNT(y) > 0"),
                "SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 0",
            ),
            (
                lambda: select("x").from_("tbl").order_by("y"),
                "SELECT x FROM tbl ORDER BY y",
            ),
            (
                lambda: select("x").from_("tbl").cluster_by("y"),
                "SELECT x FROM tbl CLUSTER BY y",
            ),
            (
                lambda: select("x").from_("tbl").sort_by("y"),
                "SELECT x FROM tbl SORT BY y",
            ),
            (
                lambda: select("x").from_("tbl").order_by("x, y DESC"),
                "SELECT x FROM tbl ORDER BY x, y DESC",
            ),
            (
                lambda: select("x").from_("tbl").cluster_by("x, y DESC"),
                "SELECT x FROM tbl CLUSTER BY x, y DESC",
            ),
            (
                lambda: select("x").from_("tbl").sort_by("x, y DESC"),
                "SELECT x FROM tbl SORT BY x, y DESC",
            ),
            (
                lambda: select("x", "y", "z", "a").from_("tbl").order_by("x, y", "z").order_by("a"),
                "SELECT x, y, z, a FROM tbl ORDER BY x, y, z, a",
            ),
            (
                lambda: select("x", "y", "z", "a").from_("tbl").cluster_by("x, y", "z").cluster_by("a"),
                "SELECT x, y, z, a FROM tbl CLUSTER BY x, y, z, a",
            ),
            (
                lambda: select("x", "y", "z", "a").from_("tbl").sort_by("x, y", "z").sort_by("a"),
                "SELECT x, y, z, a FROM tbl SORT BY x, y, z, a",
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
                lambda: select("x").from_("tbl").with_("tbl", as_="SELECT x FROM tbl2", recursive=True),
                "WITH RECURSIVE tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x").from_("tbl").with_("tbl", as_=select("x").from_("tbl2")),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x").from_("tbl").with_("tbl (x, y)", as_=select("x", "y").from_("tbl2")),
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
                lambda: select("x").from_("tbl").with_("tbl", as_=select("x", "y").from_("tbl2")).select("y"),
                "WITH tbl AS (SELECT x, y FROM tbl2) SELECT x, y FROM tbl",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").group_by("x"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl GROUP BY x",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").order_by("x"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl ORDER BY x",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").limit(10),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl LIMIT 10",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").offset(10),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl OFFSET 10",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").join("tbl3"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl JOIN tbl3",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").distinct(),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT DISTINCT x FROM tbl",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").where("x > 10"),
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl WHERE x > 10",
            ),
            (
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl").having("x > 20"),
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
                lambda: parse_one("SELECT a FROM tbl").assert_is(exp.Select).select("b"),
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
                "CREATE TABLE foo.x STORED AS PARQUET TBLPROPERTIES ('y'='2') AS SELECT * FROM y",
                "hive",
            ),
            (lambda: and_("x=1", "y=1"), "x = 1 AND y = 1"),
            (lambda: condition("x").and_("y['a']").and_("1"), "(x AND y['a']) AND 1"),
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
            (
                lambda: exp.subquery("select x from tbl", "foo").select("x").where("x > 0"),
                "SELECT x FROM (SELECT x FROM tbl) AS foo WHERE x > 0",
            ),
            (
                lambda: exp.subquery("select x from tbl UNION select x from bar", "unioned").select("x"),
                "SELECT x FROM (SELECT x FROM tbl UNION SELECT x FROM bar) AS unioned",
            ),
            (
                lambda: exp.update("tbl", {"x": None, "y": {"x": 1}}),
                "UPDATE tbl SET x = NULL, y = MAP('x', 1)",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, where="y > 0"),
                "UPDATE tbl SET x = 1 WHERE y > 0",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, from_="tbl2"),
                "UPDATE tbl SET x = 1 FROM tbl2",
            ),
        ]:
            with self.subTest(sql):
                self.assertEqual(expression().sql(dialect[0] if dialect else None), sql)
