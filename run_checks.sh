#!/bin/bash -e

pylint sqlglot/ tests/
black --check sqlglot/ tests/
python -m unittest
