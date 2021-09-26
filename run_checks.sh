#!/bin/bash -e

python -m pylint sqlglot/ tests/
python -m black --check sqlglot/ tests/
python -m unittest
