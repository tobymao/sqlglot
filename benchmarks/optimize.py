import sys
import os
import pyperf

# Add the project root to the path so we can import from tests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify import qualify
from sqlglot import parse_one
from tests.helpers import load_sql_fixture_pairs, TPCH_SCHEMA, TPCDS_SCHEMA

# Deeply nested conditions currently require a lot of recursion
sys.setrecursionlimit(10000)


def gen_condition(n):
    return parse_one(" OR ".join(f"a = {i} AND b = {i}" for i in range(n)))


# Create benchmark functions that return the setup data
def get_tpch_setup():
    return (
        [parse_one(sql) for _, sql, _ in load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql")],
        TPCH_SCHEMA,
    )


def get_tpcds_setup():
    return (
        [parse_one(sql) for _, sql, _ in load_sql_fixture_pairs("optimizer/tpc-ds/tpc-ds.sql")],
        TPCDS_SCHEMA,
    )


def get_condition_10_setup():
    return ([gen_condition(10)], {})


def get_condition_100_setup():
    return ([gen_condition(100)], {})


def get_condition_1000_setup():
    return ([gen_condition(1000)], {})


# Optimizer functions that will be benchmarked
def optimize_queries(expressions, schema):
    for e in expressions:
        # optimize(e, schema)
        qualify(e, schema=schema)


def run_benchmarks():
    runner = pyperf.Runner()

    # Define benchmarks with their setup functions
    benchmarks = {
        "tpch": get_tpch_setup,
        # "tpcds": get_tpcds_setup,  # This is left out because it's too slow in CI
        # "condition_10": get_condition_10_setup,
        # "condition_100": get_condition_100_setup,
        # "condition_1000": get_condition_1000_setup,
    }

    for benchmark_name, benchmark_setup in benchmarks.items():
        expressions, schema = benchmark_setup()

        runner.bench_func(f"optimize_{benchmark_name}", optimize_queries, expressions, schema)


if __name__ == "__main__":
    run_benchmarks()
