.PHONY: install install-dev install-pre-commit test style check docs docs-serve
# PYTHON=/usr/lib64/python3.8


install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python -m unittest

style:
	pre-commit run --all-files

check: style test

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
