.PHONY: install install-dev install-pre-commit test style docs docs-serve

install:
	pip install sqlglot

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python -m unittest

style:
	pre-commit run --all-files

docs:
	pdoc/cli.py -o pdoc/docs

docs-serve:
	pdoc/cli.py
