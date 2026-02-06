#!/usr/bin/env python3
"""
Self-sufficient tokenizer profiling script.

Usage:
    python benchmarks/profile_tokenizer.py [--cprofile | --time]

Options:
    --cprofile  Run cProfile and show top functions (default)
    --time      Simple timing comparison
"""

import os
import sys
import time
import cProfile
import pstats
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.helpers import load_sql_fixture_pairs


# Sample SQL statements for profiling
SQL_STATEMENTS = [s for _, s, _ in load_sql_fixture_pairs("optimizer/tpc-h/tpc-h.sql")] * 100


def run_cprofile(use_rs: bool = False):
    """Run cProfile and display results."""
    label = "RS" if use_rs else "Python"
    print(f"\n{'='*60}")
    print(f"cProfile results for {label} tokenizer")
    print("=" * 60)

    from sqlglot.tokens import Tokenizer

    tokenizer = Tokenizer(use_rs_tokenizer=use_rs)

    profiler = cProfile.Profile()
    profiler.enable()

    for stmt in SQL_STATEMENTS:
        tokenizer.tokenize(stmt)

    profiler.disable()

    # Save to .prof file for visualization (snakeviz, gprof2dot, etc.)
    prof_file = f"tokenizer_{label.lower()}.prof"
    profiler.dump_stats(prof_file)
    print(f"Profile saved to: {prof_file}")

    # Print stats
    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(30)
    print(stream.getvalue())


def run_timing(use_rs: bool = False):
    """Run simple timing."""
    from sqlglot.tokens import Tokenizer

    label = "RS" if use_rs else "Python"
    tokenizer = Tokenizer(use_rs_tokenizer=use_rs)

    # Warmup
    for stmt in SQL_STATEMENTS[:10]:
        tokenizer.tokenize(stmt)

    # Timed run
    iterations = 5
    start = time.perf_counter()
    for _ in range(iterations):
        for stmt in SQL_STATEMENTS:
            tokenizer.tokenize(stmt)
    elapsed = time.perf_counter() - start

    print(
        f"{label} tokenizer: {iterations} iterations in {elapsed:.3f}s ({elapsed/iterations*1000:.2f}ms per iteration)"
    )


def main():
    run_cprofile(use_rs=False)


if __name__ == "__main__":
    main()
