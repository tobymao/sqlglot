.PHONY: install install-dev install-pre-commit test style check docs docs-serve

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
	pdoc/cli.py -o pdoc/docs

docs-serve:
	pdoc/cli.py
