#!/bin/bash -e

[[ -z "${GITHUB_ACTIONS}" ]] && RETURN_ERROR_CODE='' || RETURN_ERROR_CODE='--check'

python -m autoflake -i -r ${RETURN_ERROR_CODE} \
  --expand-star-imports \
  --remove-all-unused-imports \
  --ignore-init-module-imports \
  --remove-duplicate-keys \
  --remove-unused-variables \
  sqlglot/ tests/
python -m isort --profile black sqlglot/ tests/
python -m black ${RETURN_ERROR_CODE} --line-length 120 sqlglot/ tests/
python -m unittest
