"""CLI entry point for the validation loop."""

from __future__ import annotations

import argparse
import os
import sys

from .harness import (
    print_stratified_report,
    regression_test,
    run_validation,
)


def main():
    parser = argparse.ArgumentParser(
        description="SQL Validation Loop — verify round-trip semantic preservation"
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Path to dev.json (Spider or BIRD format)",
    )
    parser.add_argument(
        "--db-dir",
        required=True,
        help="Path to database directory (e.g., data/spider/database)",
    )
    parser.add_argument(
        "--source",
        default="spider",
        choices=["spider", "bird"],
        help="Dataset source format (default: spider)",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Run baseline mode (SQLGlot round-trip only, no decompiler)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: validation_output/<mode>/results.json)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-query execution timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of queries to process (0 = all)",
    )
    parser.add_argument(
        "--regression",
        default=None,
        help="Path to previous results.json for regression check",
    )
    parser.add_argument(
        "--iteration",
        type=int,
        default=None,
        help="Iteration number for output directory (e.g., --iteration 1 → validation_output/iteration_001/)",
    )

    args = parser.parse_args()

    # Set up decompiler if not baseline
    decompiler = None
    if not args.baseline:
        from pipe_decompiler import decompile

        decompiler = decompile

    # Determine output path
    if args.output:
        output_path = args.output
    elif args.iteration is not None:
        output_path = os.path.join(
            "validation_output", f"iteration_{args.iteration:03d}", "results.json"
        )
    else:
        mode = "baseline" if args.baseline else "decompiler"
        output_path = os.path.join("validation_output", mode, "results.json")

    print("=== SQL Validation Loop ===")
    print(f"  Data:      {args.data}")
    print(f"  DB dir:    {args.db_dir}")
    print(f"  Source:    {args.source}")
    print(
        f"  Mode:      {'baseline (SQLGlot round-trip)' if args.baseline else 'decompiler (pipe SQL)'}"
    )
    print(f"  Output:    {output_path}")
    if args.iteration is not None:
        print(f"  Iteration: {args.iteration}")
    if args.limit:
        print(f"  Limit:     {args.limit}")
    print()

    records = run_validation(
        dev_json_path=args.data,
        db_dir=args.db_dir,
        output_path=output_path,
        source=args.source,
        decompiler=decompiler,
        baseline=args.baseline,
        timeout=args.timeout,
        limit=args.limit,
    )

    print_stratified_report(records, args.source)

    # Regression check
    if args.regression:
        passed, regressions, improvements = regression_test(args.regression, records)
        print()
        print("=== Regression Check ===")
        print(f"  Improvements: {len(improvements)}")
        print(f"  Regressions:  {len(regressions)}")
        print()

        if improvements:
            print("Improvements (now passing):")
            for imp in improvements[:20]:
                print(f"  + {imp}")
            if len(improvements) > 20:
                print(f"  ... and {len(improvements) - 20} more")
            print()

        if passed:
            print("Regression check: PASSED (no regressions)")
        else:
            print(f"Regression check: FAILED ({len(regressions)} regressions)")
            for r in regressions[:20]:
                print(f"  - {r}")
            if len(regressions) > 20:
                print(f"  ... and {len(regressions) - 20} more")
            sys.exit(1)


if __name__ == "__main__":
    main()
