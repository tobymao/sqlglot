#!/usr/bin/env bash
# Test runner for the Postgres Unicode escape string literal challenge.
#
# Usage:
#   ./test.sh [--output_path <junit.xml>] <base|new>
#
#   base  runs the existing Postgres dialect regression suite plus the core
#         tokenizer/transpile tests, excluding the new challenge test; these
#         must pass both before and after the solution is applied.
#   new   runs the new challenge test; fails before the solution, passes after.
set -uo pipefail

cd "$(dirname "$0")"

OUTPUT_PATH=""
if [ "${1:-}" = "--output_path" ]; then
  OUTPUT_PATH="$2"
  shift 2
fi

MODE="${1:-new}"

case "$MODE" in
  base)
    python -m pytest tests/dialects/test_postgres.py tests/test_transpile.py tests/test_tokens.py \
      --deselect tests/dialects/test_postgres.py::TestPostgres::test_unicode_string \
      -v ${OUTPUT_PATH:+--junitxml="$OUTPUT_PATH"}
    ;;
  new)
    python -m pytest tests/dialects/test_postgres.py::TestPostgres::test_unicode_string -v \
      ${OUTPUT_PATH:+--junitxml="$OUTPUT_PATH"}
    ;;
  *)
    echo "unknown mode: $MODE (expected base or new)" >&2
    exit 2
    ;;
esac
