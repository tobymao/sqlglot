#!/bin/bash -e

python -m autoflake -i -r -c --quiet \
  --expand-star-imports \
  --remove-all-unused-imports \
  --ignore-init-module-imports \
  --remove-duplicate-keys \
  --remove-unused-variables \
  sqlglot/ tests/
python -m isort --profile black sqlglot/ tests/
python -m black --check --line-length 120 sqlglot/ tests/
python -m unittest
