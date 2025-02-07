import unittest

from sqlglot import (
    alias,
    and_,
    case,
    condition,
    except_,
    exp,
    from_,
    intersect,
    not_,
    or_,
    parse_one,
    select,
    union,
)


class TestBuild(unittest.TestCase):
    def test_build(self):
        x = condition("x")
        x_plus_one = x + 1

        # Make sure we're not mutating x by changing its parent to be x_plus_one
        self.assertIsNone(x.parent)
        self.assertNotEqual(id(x_plus_one.this), id(x))

        for expression, sql, *dialect in [
            (lambda: x + 1, "x + 1"),
            (lambda: 1 + x, "1 + x"),
            (lambda: x - 1, "x - 1"),
            (lambda: 1 - x, "1 - x"),
            (lambda: x * 1, "x * 1"),
            (lambda: 1 * x, "1 * x"),
            (lambda: x / 1, "x / 1"),
            (lambda: 1 / x, "1 / x"),
            (lambda: x // 1, "CAST(x / 1 AS INT)"),
            (lambda: 1 // x, "CAST(1 / x AS INT)"),
            (lambda: x % 1, "x % 1"),
            (lambda: 1 % x, "1 % x"),
            (lambda: x**1, "POWER(x, 1)"),
            (lambda: 1**x, "POWER(1, x)"),
            (lambda: x & 1, "x AND 1"),
            (lambda: 1 & x, "1 AND x"),
            (lambda: x | 1, "x OR 1"),
            (lambda: 1 | x, "1 OR x"),
            (lambda: x < 1, "x < 1"),
            (lambda: 1 < x, "x > 1"),
            (lambda: x <= 1, "x <= 1"),
            (lambda: 1 <= x, "x >= 1"),
            (lambda: x > 1, "x > 1"),
            (lambda: 1 > x, "x < 1"),
            (lambda: x >= 1, "x >= 1"),
            (lambda: 1 >= x, "x <= 1"),
            (lambda: x.eq(1), "x = 1"),
            (lambda: x.neq(1), "x <> 1"),
            (lambda: x.is_(exp.Null()), "x IS NULL"),
            (lambda: x.as_("y"), "x AS y"),
            (lambda: x.isin(1, "2"), "x IN (1, '2')"),
            (lambda: x.isin(query="select 1"), "x IN (SELECT 1)"),
            (lambda: x.isin(unnest="x"), "x IN (SELECT UNNEST(x))"),
            (lambda: x.isin(unnest="x"), "x IN UNNEST(x)", "bigquery"),
            (lambda: x.isin(unnest=["x", "y"]), "x IN (SELECT UNNEST(x, y))"),
            (lambda: x.between(1, 2), "x BETWEEN 1 AND 2"),
            (lambda: 1 + x + 2 + 3, "1 + x + 2 + 3"),
            (lambda: 1 + x * 2 + 3, "1 + (x * 2) + 3"),
            (lambda: x * 1 * 2 + 3, "(x * 1 * 2) + 3"),
            (lambda: 1 + (x * 2) / 3, "1 + ((x * 2) / 3)"),
            (lambda: x & "y", "x AND 'y'"),
            (lambda: x | "y", "x OR 'y'"),
            (lambda: -x, "-x"),
            (lambda: ~x, "NOT x"),
            (lambda: x[1], "x[1]"),
            (lambda: x[1, 2], "x[1, 2]"),
            (lambda: x["y"] + 1, "x['y'] + 1"),
            (lambda: x.like("y"), "x LIKE 'y'"),
            (lambda: x.ilike("y"), "x ILIKE 'y'"),
            (lambda: x.rlike("y"), "REGEXP_LIKE(x, 'y')"),
            (
                lambda: case().when("x = 1", "x").else_("bar"),
                "CASE WHEN x = 1 THEN x ELSE bar END",
            ),
            (
                lambda: case("x").when("1", "x").else_("bar"),
                "CASE x WHEN 1 THEN x ELSE bar END",
            ),
            (lambda: exp.func("COALESCE", "x", 1), "COALESCE(x, 1)"),
            (lambda: exp.column("x").desc(), "x DESC"),
            (lambda: exp.column("x").desc(nulls_first=True), "x DESC NULLS FIRST"),
            (lambda: select("x"), "SELECT x"),
            (lambda: select("x"), "SELECT x"),
            (lambda: select("x", "y"), "SELECT x, y"),
            (lambda: select("x").from_("tbl"), "SELECT x FROM tbl"),
            (lambda: select("x", "y").from_("tbl"), "SELECT x, y FROM tbl"),
            (lambda: select("x").select("y").from_("tbl"), "SELECT x, y FROM tbl"),
            (lambda: select("comment", "begin"), "SELECT comment, begin"),
            (
                lambda: select("x").select("y", append=False).from_("tbl"),
                "SELECT y FROM tbl",
            ),
            (
                lambda: select("x").from_("tbl").from_("tbl2"),
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
                lambda: select("x").from_("tbl").where("x > 0").lock(),
                "SELECT x FROM tbl WHERE x > 0 FOR UPDATE",
                "mysql",
            ),
            (
                lambda: select("x").from_("tbl").where("x > 0").lock(update=False),
                "SELECT x FROM tbl WHERE x > 0 FOR SHARE",
                "postgres",
            ),
            (
                lambda: select("x").from_("tbl").hint("repartition(100)"),
                "SELECT /*+ REPARTITION(100) */ x FROM tbl",
                "spark",
            ),
            (
                lambda: select("x").from_("tbl").hint("coalesce(3)", "broadcast(x)"),
                "SELECT /*+ COALESCE(3), BROADCAST(x) */ x FROM tbl",
                "spark",
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
                lambda: select(1).from_("tbl").group_by("x with cube"),
                "SELECT 1 FROM tbl GROUP BY x WITH CUBE",
            ),
            (
                lambda: select("x").distinct("a", "b").from_("tbl"),
                "SELECT DISTINCT ON (a, b) x FROM tbl",
            ),
            (
                lambda: select("x").distinct(distinct=True).from_("tbl"),
                "SELECT DISTINCT x FROM tbl",
            ),
            (lambda: select("x").distinct(distinct=False).from_("tbl"), "SELECT x FROM tbl"),
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
                lambda: select("x")
                .from_("tbl")
                .join(exp.Table(this="tbl2"), join_type="left outer"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join(exp.Table(this="tbl2"), join_type="left outer", join_alias="foo"),
                "SELECT x FROM tbl LEFT OUTER JOIN tbl2 AS foo",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join(select("y").from_("tbl2"), join_type="left outer"),
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
                lambda: select("x")
                .from_("tbl")
                .join(parse_one("left join x", into=exp.Join), on="a=b"),
                "SELECT x FROM tbl LEFT JOIN x ON a = b",
            ),
            (
                lambda: select("x").from_("tbl").join("left join x", on="a=b"),
                "SELECT x FROM tbl LEFT JOIN x ON a = b",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .join("select b from tbl2", on="a=b", join_type="left"),
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
                lambda: select("x", "y", "z")
                .from_("merged_df")
                .join("vte_diagnosis_df", using=["patient_id", "encounter_id"]),
                "SELECT x, y, z FROM merged_df JOIN vte_diagnosis_df USING (patient_id, encounter_id)",
            ),
            (
                lambda: select("x", "y", "z")
                .from_("merged_df")
                .join(
                    "vte_diagnosis_df",
                    using=[exp.to_identifier("patient_id"), exp.to_identifier("encounter_id")],
                ),
                "SELECT x, y, z FROM merged_df JOIN vte_diagnosis_df USING (patient_id, encounter_id)",
            ),
            (
                lambda: parse_one("JOIN x", into=exp.Join).on("y = 1", "z = 1"),
                "JOIN x ON y = 1 AND z = 1",
            ),
            (
                lambda: parse_one("JOIN x", into=exp.Join).on("y = 1"),
                "JOIN x ON y = 1",
            ),
            (
                lambda: parse_one("JOIN x", into=exp.Join).using("bar", "bob"),
                "JOIN x USING (bar, bob)",
            ),
            (
                lambda: parse_one("JOIN x", into=exp.Join).using("bar"),
                "JOIN x USING (bar)",
            ),
            (
                lambda: select("x").from_("foo").join("bla", using="bob"),
                "SELECT x FROM foo JOIN bla USING (bob)",
            ),
            (
                lambda: select("x").from_("foo").join("bla", using="bob"),
                "SELECT x FROM foo JOIN bla USING (bob)",
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
                lambda: parse_one("select * from x union select * from y").order_by("y"),
                "SELECT * FROM x UNION SELECT * FROM y ORDER BY y",
            ),
            (
                lambda: select("x").from_("tbl").cluster_by("y"),
                "SELECT x FROM tbl CLUSTER BY y",
                "hive",
            ),
            (
                lambda: select("x").from_("tbl").sort_by("y"),
                "SELECT x FROM tbl SORT BY y",
                "hive",
            ),
            (
                lambda: select("x").from_("tbl").order_by("x, y DESC"),
                "SELECT x FROM tbl ORDER BY x, y DESC",
            ),
            (
                lambda: select("x").from_("tbl").cluster_by("x, y DESC"),
                "SELECT x FROM tbl CLUSTER BY x, y DESC",
                "hive",
            ),
            (
                lambda: select("x").from_("tbl").sort_by("x, y DESC"),
                "SELECT x FROM tbl SORT BY x, y DESC",
                "hive",
            ),
            (
                lambda: select("x", "y", "z", "a").from_("tbl").order_by("x, y", "z").order_by("a"),
                "SELECT x, y, z, a FROM tbl ORDER BY x, y, z, a",
            ),
            (
                lambda: select("x", "y", "z", "a")
                .from_("tbl")
                .cluster_by("x, y", "z")
                .cluster_by("a"),
                "SELECT x, y, z, a FROM tbl CLUSTER BY x, y, z, a",
                "hive",
            ),
            (
                lambda: select("x", "y", "z", "a").from_("tbl").sort_by("x, y", "z").sort_by("a"),
                "SELECT x, y, z, a FROM tbl SORT BY x, y, z, a",
                "hive",
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
                .with_("tbl", as_="SELECT x FROM tbl2", materialized=True),
                "WITH tbl AS MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_="SELECT x FROM tbl2", materialized=False),
                "WITH tbl AS NOT MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl",
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
                .with_("tbl", as_=select("x").from_("tbl2"), recursive=True, materialized=True),
                "WITH RECURSIVE tbl AS MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x")
                .from_("tbl")
                .with_("tbl", as_=select("x").from_("tbl2"), recursive=True, materialized=False),
                "WITH RECURSIVE tbl AS NOT MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl",
            ),
            (
                lambda: select("x").from_("tbl").with_("tbl", as_=select("x").from_("tbl2")),
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
                lambda: select("x").with_("tbl", as_=select("x").from_("tbl2")).from_("tbl"),
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
                "WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl, tbl3",
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
                lambda: exp.subquery("select x from tbl UNION select x from bar", "unioned").select(
                    "x"
                ),
                "SELECT x FROM (SELECT x FROM tbl UNION SELECT x FROM bar) AS unioned",
            ),
            (lambda: parse_one("(SELECT 1)").select("2"), "(SELECT 1, 2)"),
            (
                lambda: parse_one("(SELECT 1)").limit(1),
                "(SELECT 1) LIMIT 1",
            ),
            (
                lambda: parse_one("WITH t AS (SELECT 1) (SELECT 1)").limit(1),
                "WITH t AS (SELECT 1) (SELECT 1) LIMIT 1",
            ),
            (
                lambda: parse_one("(SELECT 1 LIMIT 2)").limit(1),
                "(SELECT 1 LIMIT 2) LIMIT 1",
            ),
            (
                lambda: parse_one("SELECT 1 UNION SELECT 2").limit(5).offset(2),
                "SELECT 1 UNION SELECT 2 LIMIT 5 OFFSET 2",
            ),
            (lambda: parse_one("(SELECT 1)").subquery(), "((SELECT 1))"),
            (lambda: parse_one("(SELECT 1)").subquery("alias"), "((SELECT 1)) AS alias"),
            (
                lambda: parse_one("(select * from foo)").with_("foo", "select 1 as c"),
                "WITH foo AS (SELECT 1 AS c) (SELECT * FROM foo)",
            ),
            (
                lambda: exp.update("tbl", {"x": None, "y": {"x": 1}}),
                "UPDATE tbl SET x = NULL, y = MAP(ARRAY('x'), ARRAY(1))",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, where="y > 0"),
                "UPDATE tbl SET x = 1 WHERE y > 0",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, where=exp.condition("y > 0")),
                "UPDATE tbl SET x = 1 WHERE y > 0",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, from_="tbl2"),
                "UPDATE tbl SET x = 1 FROM tbl2",
            ),
            (
                lambda: exp.update("tbl", {"x": 1}, from_="tbl2 cross join tbl3"),
                "UPDATE tbl SET x = 1 FROM tbl2 CROSS JOIN tbl3",
            ),
            (
                lambda: exp.update(
                    "my_table",
                    {"x": 1},
                    from_="baz",
                    where="my_table.id = baz.id",
                    with_={"baz": "SELECT id FROM foo UNION SELECT id FROM bar"},
                ),
                "WITH baz AS (SELECT id FROM foo UNION SELECT id FROM bar) UPDATE my_table SET x = 1 FROM baz WHERE my_table.id = baz.id",
            ),
            (
                lambda: exp.update("my_table").set_("x = 1"),
                "UPDATE my_table SET x = 1",
            ),
            (
                lambda: exp.update("my_table").set_("x = 1").where("y = 2"),
                "UPDATE my_table SET x = 1 WHERE y = 2",
            ),
            (
                lambda: exp.update("my_table").set_("a = 1").set_("b = 2"),
                "UPDATE my_table SET a = 1, b = 2",
            ),
            (
                lambda: exp.update("my_table")
                .set_("x = 1")
                .where("my_table.id = baz.id")
                .from_("baz")
                .with_("baz", "SELECT id FROM foo"),
                "WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz WHERE my_table.id = baz.id",
            ),
            (
                lambda: union("SELECT * FROM foo", "SELECT * FROM bla"),
                "SELECT * FROM foo UNION SELECT * FROM bla",
            ),
            (
                lambda: parse_one("SELECT * FROM foo").union("SELECT * FROM bla"),
                "SELECT * FROM foo UNION SELECT * FROM bla",
            ),
            (
                lambda: intersect("SELECT * FROM foo", "SELECT * FROM bla"),
                "SELECT * FROM foo INTERSECT SELECT * FROM bla",
            ),
            (
                lambda: parse_one("SELECT * FROM foo").intersect("SELECT * FROM bla"),
                "SELECT * FROM foo INTERSECT SELECT * FROM bla",
            ),
            (
                lambda: except_("SELECT * FROM foo", "SELECT * FROM bla"),
                "SELECT * FROM foo EXCEPT SELECT * FROM bla",
            ),
            (
                lambda: parse_one("SELECT * FROM foo").except_("SELECT * FROM bla"),
                "SELECT * FROM foo EXCEPT SELECT * FROM bla",
            ),
            (
                lambda: parse_one("(SELECT * FROM foo)").union("SELECT * FROM bla"),
                "(SELECT * FROM foo) UNION SELECT * FROM bla",
            ),
            (
                lambda: parse_one("(SELECT * FROM foo)").union("SELECT * FROM bla", distinct=False),
                "(SELECT * FROM foo) UNION ALL SELECT * FROM bla",
            ),
            (
                lambda: alias(parse_one("LAG(x) OVER (PARTITION BY y)"), "a"),
                "LAG(x) OVER (PARTITION BY y) AS a",
            ),
            (
                lambda: alias(parse_one("LAG(x) OVER (ORDER BY z)"), "a"),
                "LAG(x) OVER (ORDER BY z) AS a",
            ),
            (
                lambda: alias(parse_one("LAG(x) OVER (PARTITION BY y ORDER BY z)"), "a"),
                "LAG(x) OVER (PARTITION BY y ORDER BY z) AS a",
            ),
            (
                lambda: alias(parse_one("LAG(x) OVER ()"), "a"),
                "LAG(x) OVER () AS a",
            ),
            (lambda: exp.values([("1", 2)]), "VALUES ('1', 2)"),
            (lambda: exp.values([("1", 2)], "alias"), "(VALUES ('1', 2)) AS alias"),
            (lambda: exp.values([("1", 2), ("2", 3)]), "VALUES ('1', 2), ('2', 3)"),
            (
                lambda: exp.values(
                    [("1", 2, None), ("2", 3, None)], "alias", ["col1", "col2", "col3"]
                ),
                "(VALUES ('1', 2, NULL), ('2', 3, NULL)) AS alias(col1, col2, col3)",
            ),
            (lambda: exp.delete("y", where="x > 1"), "DELETE FROM y WHERE x > 1"),
            (lambda: exp.delete("y", where=exp.and_("x > 1")), "DELETE FROM y WHERE x > 1"),
            (
                lambda: select("AVG(a) OVER b")
                .from_("table")
                .window("b AS (PARTITION BY c ORDER BY d)"),
                "SELECT AVG(a) OVER b FROM table WINDOW b AS (PARTITION BY c ORDER BY d)",
            ),
            (
                lambda: select("AVG(a) OVER b", "MIN(c) OVER d")
                .from_("table")
                .window("b AS (PARTITION BY e ORDER BY f)")
                .window("d AS (PARTITION BY g ORDER BY h)"),
                "SELECT AVG(a) OVER b, MIN(c) OVER d FROM table WINDOW b AS (PARTITION BY e ORDER BY f), d AS (PARTITION BY g ORDER BY h)",
            ),
            (
                lambda: select("*")
                .from_("table")
                .qualify("row_number() OVER (PARTITION BY a ORDER BY b) = 1"),
                "SELECT * FROM table QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1",
            ),
            (lambda: exp.delete("tbl1", "x = 1").delete("tbl2"), "DELETE FROM tbl2 WHERE x = 1"),
            (lambda: exp.delete("tbl").where("x = 1"), "DELETE FROM tbl WHERE x = 1"),
            (lambda: exp.delete(exp.table_("tbl")), "DELETE FROM tbl"),
            (
                lambda: exp.delete("tbl", "x = 1").where("y = 2"),
                "DELETE FROM tbl WHERE x = 1 AND y = 2",
            ),
            (
                lambda: exp.delete("tbl", "x = 1").where(exp.condition("y = 2").or_("z = 3")),
                "DELETE FROM tbl WHERE x = 1 AND (y = 2 OR z = 3)",
            ),
            (
                lambda: exp.delete("tbl").where("x = 1").returning("*", dialect="postgres"),
                "DELETE FROM tbl WHERE x = 1 RETURNING *",
                "postgres",
            ),
            (
                lambda: exp.delete("tbl", where="x = 1", returning="*", dialect="postgres"),
                "DELETE FROM tbl WHERE x = 1 RETURNING *",
                "postgres",
            ),
            (
                lambda: exp.insert("SELECT * FROM tbl2", "tbl"),
                "INSERT INTO tbl SELECT * FROM tbl2",
            ),
            (
                lambda: exp.insert("SELECT * FROM tbl2", "tbl", returning="*"),
                "INSERT INTO tbl SELECT * FROM tbl2 RETURNING *",
            ),
            (
                lambda: exp.insert("SELECT * FROM tbl2", "tbl", overwrite=True),
                "INSERT OVERWRITE TABLE tbl SELECT * FROM tbl2",
            ),
            (
                lambda: exp.insert("VALUES (1, 2), (3, 4)", "tbl", columns=["cola", "colb"]),
                "INSERT INTO tbl (cola, colb) VALUES (1, 2), (3, 4)",
            ),
            (
                lambda: exp.insert("VALUES (1), (2)", "tbl", columns=["col a"]),
                'INSERT INTO tbl ("col a") VALUES (1), (2)',
            ),
            (
                lambda: exp.insert("SELECT * FROM cte", "t").with_("cte", as_="SELECT x FROM tbl"),
                "WITH cte AS (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte",
            ),
            (
                lambda: exp.insert("SELECT * FROM cte", "t").with_(
                    "cte", as_="SELECT x FROM tbl", materialized=True
                ),
                "WITH cte AS MATERIALIZED (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte",
            ),
            (
                lambda: exp.insert("SELECT * FROM cte", "t").with_(
                    "cte", as_="SELECT x FROM tbl", materialized=False
                ),
                "WITH cte AS NOT MATERIALIZED (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte",
            ),
            (
                lambda: exp.convert((exp.column("x"), exp.column("y"))).isin((1, 2), (3, 4)),
                "(x, y) IN ((1, 2), (3, 4))",
                "postgres",
            ),
            (lambda: exp.cast("CAST(x AS INT)", "int"), "CAST(x AS INT)"),
            (lambda: exp.cast("CAST(x AS TEXT)", "int"), "CAST(CAST(x AS TEXT) AS INT)"),
            (
                lambda: exp.rename_column("table1", "c1", "c2", True),
                "ALTER TABLE table1 RENAME COLUMN IF EXISTS c1 TO c2",
            ),
            (
                lambda: exp.rename_column("table1", "c1", "c2", False),
                "ALTER TABLE table1 RENAME COLUMN c1 TO c2",
            ),
            (
                lambda: exp.rename_column("table1", "c1", "c2"),
                "ALTER TABLE table1 RENAME COLUMN c1 TO c2",
            ),
            (
                lambda: exp.merge(
                    "WHEN MATCHED THEN UPDATE SET col1 = source.col1",
                    "WHEN NOT MATCHED THEN INSERT (col1) VALUES (source.col1)",
                    into="target_table",
                    using="source_table",
                    on="target_table.id = source_table.id",
                ),
                "MERGE INTO target_table USING source_table ON target_table.id = source_table.id WHEN MATCHED THEN UPDATE SET col1 = source.col1 WHEN NOT MATCHED THEN INSERT (col1) VALUES (source.col1)",
            ),
            (
                lambda: exp.merge(
                    "WHEN MATCHED AND source.is_deleted = 1 THEN DELETE",
                    "WHEN MATCHED THEN UPDATE SET val = source.val",
                    "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
                    into="target_table",
                    using="source_table",
                    on="target_table.id = source_table.id",
                ),
                "MERGE INTO target_table USING source_table ON target_table.id = source_table.id WHEN MATCHED AND source.is_deleted = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET val = source.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
            ),
            (
                lambda: exp.merge(
                    "WHEN MATCHED THEN UPDATE SET target.name = source.name",
                    into=exp.table_("target_table").as_("target"),
                    using=exp.table_("source_table").as_("source"),
                    on="target.id = source.id",
                ),
                "MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name",
            ),
            (
                lambda: exp.merge(
                    "WHEN MATCHED THEN UPDATE SET target.name = source.name",
                    into=exp.table_("target_table").as_("target"),
                    using=exp.table_("source_table").as_("source"),
                    on="target.id = source.id",
                    returning="target.*",
                ),
                "MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name RETURNING target.*",
            ),
            (
                lambda: exp.merge(
                    exp.When(
                        matched=True,
                        then=exp.Update(
                            expressions=[
                                exp.column("name", "target").eq(exp.column("name", "source"))
                            ]
                        ),
                    ),
                    into=exp.table_("target_table").as_("target"),
                    using=exp.table_("source_table").as_("source"),
                    on="target.id = source.id",
                    returning="target.*",
                ),
                "MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name RETURNING target.*",
            ),
            (
                lambda: exp.union("SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4"),
                "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4",
            ),
            (
                lambda: select("x")
                .with_("var1", as_=select("x").from_("tbl2").subquery(), scalar=True)
                .from_("tbl")
                .where("x > var1"),
                "WITH (SELECT x FROM tbl2) AS var1 SELECT x FROM tbl WHERE x > var1",
                "clickhouse",
            ),
            (
                lambda: select("x")
                .with_("var1", as_=select("x").from_("tbl2"), scalar=True)
                .from_("tbl")
                .where("x > var1"),
                "WITH (SELECT x FROM tbl2) AS var1 SELECT x FROM tbl WHERE x > var1",
                "clickhouse",
            ),
        ]:
            with self.subTest(sql):
                self.assertEqual(expression().sql(dialect[0] if dialect else None), sql)
