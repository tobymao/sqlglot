import typing as t
from argparse import ArgumentParser

from benchmarks.helpers import ascii_table
from sqlglot.optimizer import optimize
from sqlglot import parse_one
from tests.helpers import load_sql_fixture_pairs, TPCH_SCHEMA, TPCDS_SCHEMA
from timeit import Timer
import sys

# Deeply nested conditions currently require a lot of recursion
sys.setrecursionlimit(10000)


def gen_condition(n):
    return parse_one(" OR ".join(f"a = {i} AND b = {i}" for i in range(n)))


BENCHMARKS = {
    "tpch": lambda: (
        [parse_one(sql) for _, sql, _ in load_sql_fixture_pairs(f"optimizer/tpc-h/tpc-h.sql")],
        TPCH_SCHEMA,
        3,
    ),
    "tpcds": lambda: (
        [parse_one(sql) for _, sql, _ in load_sql_fixture_pairs(f"optimizer/tpc-ds/tpc-ds.sql")],
        TPCDS_SCHEMA,
        3,
    ),
    "condition_10": lambda: (
        [gen_condition(10)],
        {},
        10,
    ),
    "condition_100": lambda: (
        [gen_condition(100)],
        {},
        10,
    ),
    "condition_1000": lambda: (
        [gen_condition(1000)],
        {},
        3,
    ),
}


def bench() -> list[dict[str, t.Any]]:
    parser = ArgumentParser()
    parser.add_argument("-b", "--benchmark", choices=BENCHMARKS, action="append")
    args = parser.parse_args()
    benchmarks = list(args.benchmark or BENCHMARKS)

    table = []
    for benchmark in benchmarks:
        expressions, schema, n = BENCHMARKS[benchmark]()

        def func():
            for e in expressions:
                optimize(e, schema)

        timer = Timer(func)
        min_duration = min(timer.repeat(repeat=n, number=1))
        table.append({"Benchmark": benchmark, "Duration (s)": round(min_duration, 4)})

    return table


if __name__ == "__main__":
    print(ascii_table(bench()))
