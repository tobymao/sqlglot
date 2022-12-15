.PHONY: install install-dev tests style docs docs-serve

install:
	pip install sqlglot

install-dev:
	pip install -e ".[dev]"
	pre-commit install

tests:
	python -m unittest

style:
	pre-commit run --all-files

docs:
	pdoc/cli.py -o pdoc/docs

docs-serve:
	pdoc/cli.py
