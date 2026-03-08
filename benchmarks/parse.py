import collections
import collections.abc
import pyperf

# Patch for Python 3.10+ compatibility with legacy parsers (moz_sql_parser)
collections.Iterable = collections.abc.Iterable

try:
    import sqlfluff
except ImportError:
    sqlfluff = None

try:
    import moz_sql_parser
except ImportError:
    moz_sql_parser = None

try:
    import sqloxide
except ImportError:
    sqloxide = None

try:
    import sqlparse
except ImportError:
    sqlparse = None

try:
    import sqltree
except ImportError:
    sqltree = None

try:
    import polyglot_sql
except ImportError:
    polyglot_sql = None

import sqlglot  # noqa: E402

large_in = (
    "SELECT * FROM t WHERE x IN (" + ", ".join(f"'s{i}'" for i in range(20000)) + ")"
    " OR y IN (" + ", ".join(str(i) for i in range(20000)) + ")"
)

values = "INSERT INTO t VALUES " + ", ".join(
    "(" + ", ".join(
        f"'s{i}_{j}'" if j % 2 else str(i * 20 + j) for j in range(20)
    ) + ")"
    for i in range(2000)
)

many_joins = "SELECT * FROM t0" + "".join(
    f"\nJOIN t{i} ON t{i}.id = t{i - 1}.id" for i in range(1, 200)
)

many_unions = "\nUNION ALL\n".join(f"SELECT {i} AS a, 's{i}' AS b FROM t{i}" for i in range(500))

short = "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x"

deep_arithmetic = "SELECT 1+"
deep_arithmetic += "+".join(str(i) for i in range(500))
deep_arithmetic += " AS a, 2*"
deep_arithmetic += "*".join(str(i) for i in range(500))
deep_arithmetic += " AS b FROM x"

nested_subqueries = "SELECT * FROM " + "".join("(SELECT * FROM " for _ in range(50)) + "t" + ")" * 50

many_columns = "SELECT " + ", ".join(f"c{i}" for i in range(1000)) + " FROM t"

large_case = "SELECT CASE " + " ".join(f"WHEN x = {i} THEN {i}" for i in range(1000)) + " ELSE -1 END FROM t"

complex_where = "SELECT * FROM t WHERE " + " AND ".join(
    f"(c{i} > {i} OR c{i} LIKE '%s{i}%' OR c{i} BETWEEN {i} AND {i+10} OR c{i} IS NULL)"
    for i in range(200)
)

many_ctes = (
    "WITH " + ", ".join(f"t{i} AS (SELECT {i} AS a FROM t{i-1 if i else 'base'})" for i in range(200))
    + " SELECT * FROM t199"
)

many_windows = "SELECT " + ", ".join(
    f"SUM(c{i}) OVER (PARTITION BY p{i % 10} ORDER BY o{i % 5}) AS w{i}" for i in range(200)
) + " FROM t"

nested_functions = "SELECT " + "COALESCE(" * 50 + "x" + ", NULL)" * 50 + " FROM t"

tpch = """
WITH "_e_0" AS (
  SELECT
    "partsupp"."ps_partkey" AS "ps_partkey",
    "partsupp"."ps_suppkey" AS "ps_suppkey",
    "partsupp"."ps_supplycost" AS "ps_supplycost"
  FROM "partsupp" AS "partsupp"
), "_e_1" AS (
  SELECT
    "region"."r_regionkey" AS "r_regionkey",
    "region"."r_name" AS "r_name"
  FROM "region" AS "region"
  WHERE
    "region"."r_name" = 'EUROPE'
)
SELECT
  "supplier"."s_acctbal" AS "s_acctbal",
  "supplier"."s_name" AS "s_name",
  "nation"."n_name" AS "n_name",
  "part"."p_partkey" AS "p_partkey",
  "part"."p_mfgr" AS "p_mfgr",
  "supplier"."s_address" AS "s_address",
  "supplier"."s_phone" AS "s_phone",
  "supplier"."s_comment" AS "s_comment"
FROM (
  SELECT
    "part"."p_partkey" AS "p_partkey",
    "part"."p_mfgr" AS "p_mfgr",
    "part"."p_type" AS "p_type",
    "part"."p_size" AS "p_size"
  FROM "part" AS "part"
  WHERE
    "part"."p_size" = 15
    AND "part"."p_type" LIKE '%BRASS'
) AS "part"
LEFT JOIN (
  SELECT
    MIN("partsupp"."ps_supplycost") AS "_col_0",
    "partsupp"."ps_partkey" AS "_u_1"
  FROM "_e_0" AS "partsupp"
  CROSS JOIN "_e_1" AS "region"
  JOIN (
    SELECT
      "nation"."n_nationkey" AS "n_nationkey",
      "nation"."n_regionkey" AS "n_regionkey"
    FROM "nation" AS "nation"
  ) AS "nation"
    ON "nation"."n_regionkey" = "region"."r_regionkey"
  JOIN (
    SELECT
      "supplier"."s_suppkey" AS "s_suppkey",
      "supplier"."s_nationkey" AS "s_nationkey"
    FROM "supplier" AS "supplier"
  ) AS "supplier"
    ON "supplier"."s_nationkey" = "nation"."n_nationkey"
    AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
  GROUP BY
    "partsupp"."ps_partkey"
) AS "_u_0"
  ON "part"."p_partkey" = "_u_0"."_u_1"
CROSS JOIN "_e_1" AS "region"
JOIN (
  SELECT
    "nation"."n_nationkey" AS "n_nationkey",
    "nation"."n_name" AS "n_name",
    "nation"."n_regionkey" AS "n_regionkey"
  FROM "nation" AS "nation"
) AS "nation"
  ON "nation"."n_regionkey" = "region"."r_regionkey"
JOIN "_e_0" AS "partsupp"
  ON "part"."p_partkey" = "partsupp"."ps_partkey"
JOIN (
  SELECT
    "supplier"."s_suppkey" AS "s_suppkey",
    "supplier"."s_name" AS "s_name",
    "supplier"."s_address" AS "s_address",
    "supplier"."s_nationkey" AS "s_nationkey",
    "supplier"."s_phone" AS "s_phone",
    "supplier"."s_acctbal" AS "s_acctbal",
    "supplier"."s_comment" AS "s_comment"
  FROM "supplier" AS "supplier"
) AS "supplier"
  ON "supplier"."s_nationkey" = "nation"."n_nationkey"
  AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
WHERE
  "partsupp"."ps_supplycost" = "_u_0"."_col_0"
  AND NOT "_u_0"."_u_1" IS NULL
ORDER BY
  "supplier"."s_acctbal" DESC,
  "nation"."n_name",
  "supplier"."s_name",
  "part"."p_partkey"
LIMIT 100
"""


def sqlglot_parse(sql):
    sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)


def sqltree_parse(sql):
    sqltree.api.sqltree(sql.replace('"', "`").replace("''", '"'))


def sqlparse_parse(sql):
    sqlparse.parse(sql)


def moz_sql_parser_parse(sql):
    moz_sql_parser.parse(sql)


def sqloxide_parse(sql):
    sqloxide.parse_sql(sql, dialect="ansi")


def sqlfluff_parse(sql):
    sqlfluff.parse(sql)


def polyglot_sql_parse(sql):
    polyglot_sql.parse(sql)


QUERIES = {
    "tpch": tpch,
    "short": short,
    "deep_arithmetic": deep_arithmetic,
    "large_in": large_in,
    "values": values,
    "many_joins": many_joins,
    "many_unions": many_unions,
    "nested_subqueries": nested_subqueries,
    "many_columns": many_columns,
    "large_case": large_case,
    "complex_where": complex_where,
    "many_ctes": many_ctes,
    "many_windows": many_windows,
    "nested_functions": nested_functions,
}


def _can_parse(fn, sql):
    try:
        fn(sql)
        return True
    except Exception:
        return False


LARGE_QUERIES = {"large_in", "values", "many_unions"}


def run_benchmarks():
    import sqlglot.expressions.core as _ec

    prefix = "sqlglotc" if _ec.__file__.endswith(".so") else "sqlglot"

    for query_name, sql in QUERIES.items():
        if query_name in LARGE_QUERIES:
            runner = pyperf.Runner(values=3, warmups=1, loops=1, processes=4)
        else:
            runner = pyperf.Runner(values=3, warmups=1, loops=10, processes=4)

        runner.bench_func(f"parse_{prefix}_{query_name}", sqlglot_parse, sql)
        if sqltree and _can_parse(sqltree_parse, sql):
            runner.bench_func(f"parse_sqltree_{query_name}", sqltree_parse, sql)
        if sqlparse and query_name != "deep_arithmetic" and _can_parse(sqlparse_parse, sql):
            runner.bench_func(f"parse_sqlparse_{query_name}", sqlparse_parse, sql)
        if moz_sql_parser and _can_parse(moz_sql_parser_parse, sql):
            runner.bench_func(f"parse_moz_sql_parser_{query_name}", moz_sql_parser_parse, sql)
        if sqloxide and _can_parse(sqloxide_parse, sql):
            runner.bench_func(f"parse_sqloxide_{query_name}", sqloxide_parse, sql)
        if polyglot_sql and _can_parse(polyglot_sql_parse, sql):
            runner.bench_func(f"parse_polyglot_sql_{query_name}", polyglot_sql_parse, sql)
        if sqlfluff and _can_parse(sqlfluff_parse, sql):
            runner.bench_func(f"parse_sqlfluff_{query_name}", sqlfluff_parse, sql)


if __name__ == "__main__":
    run_benchmarks()
