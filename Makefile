.PHONY: install install-dev install-pre-commit test style check docs docs-serve

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python3.8 -m unittest

style:
	pre-commit run --all-files

check: style test

docs:
	python3.8 pdoc/cli.py -o docs

docs-serve:
	python3.8 pdoc/cli.py --port 8002
