#!/usr/bin/env python3
"""Compare two benchmark JSON files and output a markdown table with diff indicators."""

import json
import sys


def _fmt_time(seconds):
    if seconds >= 1:
        return f"{seconds:.2f}s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.1f}ms"
    return f"{seconds * 1e6:.0f}us"


def _indicator(ratio):
    """Return an emoji indicator based on the speedup/slowdown ratio (pr_time / main_time)."""
    if ratio <= 0.95:
        return "\U0001f7e2\U0001f7e2"  # 5%+ faster
    if ratio <= 0.97:
        return "\U0001f7e2"  # 3-5% faster
    if ratio <= 0.99:
        return "\U0001f7e9"  # 1-3% faster
    if ratio <= 1.01:
        return "\u26aa"  # no significant change
    if ratio <= 1.03:
        return "\U0001f7e7"  # 1-3% slower
    if ratio <= 1.05:
        return "\U0001f534"  # 3-5% slower
    return "\U0001f534\U0001f534"  # 5%+ slower


def _diff_text(ratio):
    pct = (ratio - 1.0) * 100
    if abs(pct) < 0.05:
        return "0.0%"
    if pct < 0:
        return f"{-pct:.1f}% faster"
    return f"{pct:.1f}% slower"


def compare(main_file, pr_file):
    with open(main_file) as f:
        main = json.load(f)
    with open(pr_file) as f:
        pr = json.load(f)

    # Collect all query names across both parsers
    parsers = set()
    queries = []
    seen_queries = set()
    for key in list(main.keys()) + list(pr.keys()):
        parser, query = key.split(":", 1)
        parsers.add(parser)
        if query not in seen_queries:
            queries.append(query)
            seen_queries.add(query)

    # Sort parsers: sqlglot first, then sqlglotc
    parser_order = sorted(parsers, key=lambda p: (p != "sqlglot", p != "sqlglotc", p))

    # Build table
    lines = []
    lines.append("## Benchmark Results\n")
    lines.append(
        "**Legend:** \U0001f7e2\U0001f7e2 = 5%+ faster | \U0001f7e2 = 3-5% faster | \U0001f7e9 = 1-3% faster | \u26aa = unchanged | \U0001f7e7 = 1-3% slower | \U0001f534 = 3-5% slower | \U0001f534\U0001f534 = 5%+ slower\n"
    )

    for parser in parser_order:
        display = (
            "sqlglot" if parser == "sqlglot" else "sqlglot[c]" if parser == "sqlglotc" else parser
        )
        lines.append(f"\n### {display}\n")
        lines.append("| Query | main | PR | diff |  |")
        lines.append("| ----- | ---: | ---: | ---: | --- |")

        for query in queries:
            key = f"{parser}:{query}"
            main_time = main.get(key)
            pr_time = pr.get(key)

            if main_time is None and pr_time is None:
                continue

            main_str = _fmt_time(main_time) if main_time else "N/A"
            pr_str = _fmt_time(pr_time) if pr_time else "N/A"

            if main_time and pr_time:
                ratio = pr_time / main_time
                diff_str = _diff_text(ratio)
                ind = _indicator(ratio)
            else:
                diff_str = "N/A"
                ind = ""

            lines.append(f"| {query} | {main_str} | {pr_str} | {diff_str} | {ind} |")

    lines.append("\n---\n*Comment `/benchmark` to re-run.*")

    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: compare_benchmarks.py <main.json> <pr.json>", file=sys.stderr)
        sys.exit(1)
    print(compare(sys.argv[1], sys.argv[2]))
