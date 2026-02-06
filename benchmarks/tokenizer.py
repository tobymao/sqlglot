import sys
import os
import pyperf

from sqlglot.tokens import Tokenizer


# Add the project root to the path so we can import from tests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.helpers import load_sql_fixture_pairs

# Deeply nested conditions currently require a lot of recursion
sys.setrecursionlimit(10000)


# Create benchmark functions that return the setup data
def get_tpch_setup():
    return [sql for _, sql, _ in load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql")] * 1000


def rs_tokenizer(expressions):
    tokenizer = Tokenizer(use_rs_tokenizer=True)
    for e in expressions:
        tokenizer.tokenize(e)


def python_tokenizer(expressions):
    tokenizer = Tokenizer(use_rs_tokenizer=False)
    for e in expressions:
        tokenizer.tokenize(e)


def run_benchmarks():
    runner = pyperf.Runner()

    benchmarks = {
        "tpch": get_tpch_setup,
    }

    for benchmark_name, benchmark_setup in benchmarks.items():
        expressions = benchmark_setup()

        # runner.bench_func(f"rs_tokenize_{benchmark_name}", rs_tokenizer, expressions)
        runner.bench_func(f"python_tokenize_{benchmark_name}", python_tokenizer, expressions)


if __name__ == "__main__":
    run_benchmarks()
