import argparse
import inspect
import json
import os
import subprocess
import sys
import tempfile
import time


# --- Query definitions ---

large_in = (
    "SELECT * FROM t WHERE x IN (" + ", ".join(f"'s{i}'" for i in range(20000)) + ")"
    " OR y IN (" + ", ".join(str(i) for i in range(20000)) + ")"
)

values = "INSERT INTO t VALUES " + ", ".join(
    "(" + ", ".join(f"'s{i}_{j}'" if j % 2 else str(i * 20 + j) for j in range(20)) + ")"
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

nested_subqueries = (
    "SELECT * FROM " + "".join("(SELECT * FROM " for _ in range(20)) + "t" + ")" * 20
)

many_columns = "SELECT " + ", ".join(f"c{i}" for i in range(1000)) + " FROM t"

large_case = (
    "SELECT CASE " + " ".join(f"WHEN x = {i} THEN {i}" for i in range(1000)) + " ELSE -1 END FROM t"
)

complex_where = "SELECT * FROM t WHERE " + " AND ".join(
    f"(c{i} > {i} OR c{i} LIKE '%s{i}%' OR c{i} BETWEEN {i} AND {i + 10} OR c{i} IS NULL)"
    for i in range(200)
)

many_ctes = (
    "WITH "
    + ", ".join(f"t{i} AS (SELECT {i} AS a FROM t{i - 1 if i else 'base'})" for i in range(200))
    + " SELECT * FROM t199"
)

many_windows = (
    "SELECT "
    + ", ".join(
        f"SUM(c{i}) OVER (PARTITION BY p{i % 10} ORDER BY o{i % 5}) AS w{i}" for i in range(200)
    )
    + " FROM t"
)

nested_functions = "SELECT " + "COALESCE(" * 20 + "x" + ", NULL)" * 20 + " FROM t"

large_strings = "SELECT " + ", ".join(f"'{'x' * 100}'" for i in range(500)) + " FROM t"

many_numbers = "SELECT " + ", ".join(str(i) for i in range(10000)) + " FROM t"

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
    "large_strings": large_strings,
    "many_numbers": many_numbers,
}


# --- Parser definitions ---


def sqlglot_parse(sql):
    import sqlglot

    sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)


def sqlglot_transpile(sql):
    import sqlglot

    sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.IGNORE)[0]


def sqltree_parse(sql):
    import sqltree

    sqltree.api.sqltree(sql.replace('"', "`").replace("''", '"'))


def sqlparse_parse(sql):
    import sqlparse

    sqlparse.parse(sql)


def moz_sql_parser_parse(sql):
    import moz_sql_parser

    moz_sql_parser.parse(sql)


def sqloxide_parse(sql):
    import sqloxide

    sqloxide.parse_sql(sql, dialect="ansi")


def sqlfluff_parse(sql):
    import sqlfluff

    sqlfluff.parse(sql)


def polyglot_sql_parse(sql):
    import polyglot_sql

    polyglot_sql.parse_one(sql)


def polyglot_sql_transpile(sql):
    import polyglot_sql

    polyglot_sql.transpile(sql)[0]


THIRD_PARTY_PARSERS = {
    "sqltree": sqltree_parse,
    "sqlparse": sqlparse_parse,
    "sqlfluff": sqlfluff_parse,
    "moz_sql_parser": moz_sql_parser_parse,
    "sqloxide": sqloxide_parse,
    "polyglot_sql": polyglot_sql_parse,
}

THIRD_PARTY_TRANSPILERS = {
    "polyglot_sql": polyglot_sql_transpile,
}

DISPLAY_NAMES = {
    "sqlglot": "sqlglot",
    "sqlglotc": "sqlglot[c]",
    "polyglot_sql": "polyglot-sql",
    "sqltree": "sqltree",
    "sqlparse": "sqlparse",
    "moz_sql_parser": "moz_sql_parser",
    "sqlfluff": "sqlfluff",
    "sqloxide": "sqloxide",
}


# --- Third-party parser discovery ---


def _check_parser(parse_fn, queries):
    """Check which queries a parser can handle, one subprocess per query (isolates segfaults).
    Returns None if not installed, else set of query names."""
    fn_name = parse_fn.__name__
    source = inspect.getsource(parse_fn)
    supported = set()
    installed = None

    for name, sql in queries.items():
        code = f"""import signal

def _timeout(signum, frame):
    raise TimeoutError()

signal.signal(signal.SIGALRM, _timeout)
signal.alarm(5)

{source}

{fn_name}({repr(sql)})
"""
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", suffix=".py", delete=True) as f:
            f.write(code)
            f.flush()
            try:
                result = subprocess.run([sys.executable, f.name], capture_output=True, timeout=10)
            except subprocess.TimeoutExpired:
                installed = True
                continue
            if b"ModuleNotFoundError" in result.stderr:
                return None
            installed = True
            if result.returncode == 0:
                supported.add(name)

    return supported if installed else None


def _discover_parsers(registry=None):
    """Discover available third-party parsers and which queries they support."""
    if registry is None:
        registry = THIRD_PARTY_PARSERS
    valid_pairs = set()
    available = []
    for parser_name, parse_fn in registry.items():
        supported = _check_parser(parse_fn, QUERIES)
        if supported is None:
            continue
        for query_name in supported:
            valid_pairs.add((parser_name, query_name))
        available.append(parser_name)
    return available, valid_pairs


# --- Benchmarking ---


_quiet = False


def _bench(name, fn, *args, iterations=5):
    """Benchmark fn(*args) and return the best time in seconds."""
    best = float("inf")
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn(*args)
        elapsed = time.perf_counter() - t0
        if elapsed < best:
            best = elapsed
        if elapsed > 1:
            break
    if not _quiet:
        print(f"  {name}: {_fmt_time(best)}")
    return best


def _bench_sqlglot(results, mode="parse"):
    """Benchmark sqlglot (or sqlglotc if .so loaded) and add to results."""
    import sqlglot.expressions.core as _ec

    prefix = "sqlglotc" if _ec.__file__.endswith(".so") else "sqlglot"
    fn = sqlglot_transpile if mode == "transpile" else sqlglot_parse
    for query_name, sql in QUERIES.items():
        key = f"{prefix}:{query_name}"
        results[key] = _bench(key, fn, sql)
    return prefix


def _bench_third_party(results, mode="parse"):
    """Benchmark third-party parsers/transpilers and add to results. Returns list of available names."""
    registry = THIRD_PARTY_TRANSPILERS if mode == "transpile" else THIRD_PARTY_PARSERS
    available, valid_pairs = _discover_parsers(registry)
    for query_name, sql in QUERIES.items():
        for name, fn in registry.items():
            if (name, query_name) in valid_pairs:
                key = f"{name}:{query_name}"
                results[key] = _bench(key, fn, sql)
    return available


# --- Table printing ---


def _fmt_ratio(ratio):
    return f"{ratio:.2f}"


def _fmt_time(seconds):
    if seconds >= 1:
        return f"{seconds:.2f} sec"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds * 1e6:.1f} us"


def _print_table(base_parser, all_parsers, results):
    query_width = max(len(q) for q in QUERIES)
    query_width = max(query_width, len("Query"))

    # Pre-compute all cells to determine column widths
    cells = {}
    for query_name in QUERIES:
        base_time = results.get(f"{base_parser}:{query_name}")
        for p in all_parsers:
            t = results.get(f"{p}:{query_name}")
            if t is not None and base_time:
                ratio = t / base_time
                cells[(p, query_name)] = f"{t:.6f} ({_fmt_ratio(ratio)})"
            else:
                cells[(p, query_name)] = "N/A"

    col_widths = {}
    for p in all_parsers:
        name = DISPLAY_NAMES.get(p, p)
        w = len(name)
        for query_name in QUERIES:
            w = max(w, len(cells[(p, query_name)]))
        col_widths[p] = w

    header = f"| {'Query':>{query_width}} |"
    sep = f"| {'-' * query_width} |"
    for p in all_parsers:
        name = DISPLAY_NAMES.get(p, p)
        header += f" {name:>{col_widths[p]}} |"
        sep += f" {'-' * col_widths[p]} |"

    print()
    print(header)
    print(sep)

    for query_name in QUERIES:
        row = f"| {query_name:>{query_width}} |"
        for p in all_parsers:
            row += f" {cells[(p, query_name)]:>{col_widths[p]}} |"
        print(row)


# --- Subprocess entry point for .so mode ---


def _has_so_files():
    import glob

    return bool(glob.glob("sqlglot/**/*.so", recursive=True))


def _run_subprocess():
    """Run sqlglot benchmarks and print results to stdout as key=value lines."""
    global _quiet
    _quiet = bool(os.environ.get("_BENCH_QUIET"))
    mode = os.environ.get("_BENCH_MODE", "parse")
    results = {}
    _bench_sqlglot(results, mode=mode)
    for key, value in results.items():
        print(f"{key}={value}")


# --- Main ---


def _parse_args():
    parser = argparse.ArgumentParser(description="SQLGlot parser benchmarks")
    parser.add_argument("--json", metavar="FILE", help="Write results as JSON to FILE")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    parser.add_argument(
        "--sqlglot-only",
        action="store_true",
        help="Only benchmark sqlglot/sqlglotc (skip third-party parsers)",
    )
    parser.add_argument(
        "--mode",
        choices=["parse", "transpile"],
        default="parse",
        help="Benchmark mode: parse or transpile (default: parse)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    if os.environ.get("_BENCH_SUBPROCESS"):
        _run_subprocess()
    else:
        args = _parse_args()
        _quiet = args.quiet

        mode = args.mode

        if _has_so_files():
            if not _quiet:
                print("=== Running sqlglot[c] ===", flush=True)
            env = {**os.environ, "_BENCH_SUBPROCESS": "1", "_BENCH_MODE": mode}
            if _quiet:
                env["_BENCH_QUIET"] = "1"
            proc = subprocess.run(
                [sys.executable, __file__], env=env, capture_output=True, text=True, check=True
            )
            results = {}
            for line in proc.stdout.splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    results[key] = float(value)
                elif not _quiet:
                    print(line)

            if not _quiet:
                print("\n=== Hiding .so files ===", flush=True)
            subprocess.run(["make", "hidec"], check=True, capture_output=True)

            try:
                if not _quiet:
                    print("\n=== Running pure Python ===", flush=True)
                _bench_sqlglot(results, mode=mode)
                if not args.sqlglot_only:
                    if not _quiet:
                        print(f"\n=== Running third-party ({mode}) ===", flush=True)
                    available = _bench_third_party(results, mode=mode)
                else:
                    available = []
            finally:
                subprocess.run(["make", "showc"], capture_output=True)

            if args.json:
                with open(args.json, "w") as f:
                    json.dump(results, f, indent=2)
            else:
                _print_table("sqlglot", ["sqlglot", "sqlglotc"] + available, results)
        else:
            results = {}
            prefix = _bench_sqlglot(results, mode=mode)
            if not args.sqlglot_only:
                available = _bench_third_party(results, mode=mode)
            else:
                available = []

            if args.json:
                with open(args.json, "w") as f:
                    json.dump(results, f, indent=2)
            else:
                _print_table(prefix, [prefix] + available, results)
