#!/bin/bash -e

python -m autoflake -i -r \
  --expand-star-imports \
  --remove-all-unused-imports \
  --ignore-init-module-imports \
  --remove-duplicate-keys \
  --remove-unused-variables \
  sqlglot/ tests/
python -m isort --profile black sqlglot/ tests/
python -m black sqlglot/ tests/
python -m unittest
