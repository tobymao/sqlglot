import sys
import os
import pyperf

# Add the project root to the path so we can import from tests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify import qualify
from sqlglot import parse_one, exp
from tests.helpers import load_sql_fixture_pairs, TPCH_SCHEMA, TPCDS_SCHEMA

# Deeply nested conditions currently require a lot of recursion
sys.setrecursionlimit(10000)


def gen_condition(n):
    return parse_one(" OR ".join(f"a = {i} AND b = {i}" for i in range(n)))


# Create benchmark functions that return the setup data
def get_tpch_setup():
    return (
        [parse_one(sql) for _, sql, _ in load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql")] * 500,
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


# Expression tree traversal benchmarks
def walk_bfs(expressions, schema):
    """Benchmark BFS walk (default)"""
    for e in expressions:
        list(e.walk(bfs=True))


def walk_dfs(expressions, schema):
    """Benchmark DFS walk"""
    for e in expressions:
        list(e.walk(bfs=False))


def iter_expressions_bench(expressions, schema):
    """Benchmark iter_expressions on all nodes"""
    for e in expressions:
        for node in e.walk():
            list(node.iter_expressions())


def find_all_columns(expressions, schema):
    """Benchmark find_all for Column nodes"""
    for e in expressions:
        list(e.find_all(exp.Column))


def find_all_tables(expressions, schema):
    """Benchmark find_all for Table nodes"""
    for e in expressions:
        list(e.find_all(exp.Table))


def copy_expressions(expressions, schema):
    """Benchmark expression copy (deepcopy)"""
    for e in expressions:
        e.copy()


def hash_expressions(expressions, schema):
    """Benchmark expression hashing"""
    for e in expressions:
        # Reset hash to force recomputation
        for node in e.walk():
            node._hash = None
        hash(e)


def run_benchmarks():
    runner = pyperf.Runner()

    # Define setups
    setups = {
        "tpch": get_tpch_setup,
        # "tpcds": get_tpcds_setup,  # This is left out because it's too slow in CI
    }

    # Define benchmark functions
    benchmark_funcs = {
        "walk_bfs": walk_bfs,
        "walk_dfs": walk_dfs,
        "iter_expressions": iter_expressions_bench,
        "find_all_columns": find_all_columns,
        "find_all_tables": find_all_tables,
        "copy": copy_expressions,
        "hash": hash_expressions,
    }

    for setup_name, setup_func in setups.items():
        expressions, schema = setup_func()

        for bench_name, bench_func in benchmark_funcs.items():
            runner.bench_func(f"{bench_name}_{setup_name}", bench_func, expressions, schema)


if __name__ == "__main__":
    run_benchmarks()
