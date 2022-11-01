#!/bin/bash -e
[[ -z "${GITHUB_ACTIONS}" ]] && RETURN_ERROR_CODE='' || RETURN_ERROR_CODE='--check'
TARGETS="sqlglot/ tests/"
python -m mypy $TARGETS
python -m autoflake -i -r ${RETURN_ERROR_CODE} $TARGETS
python -m isort $TARGETS
python -m black --line-length 100 ${RETURN_ERROR_CODE} $TARGETS
python -m unittest
