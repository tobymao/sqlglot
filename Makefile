.PHONY: install install-dev install-pre-commit test lint check docs docs-serve

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python -m unittest

test-unit:
	SKIP_INTEGRATION=1 python -m unittest

lint:
	pre-commit run --all-files

check: lint test

check-unit: lint test-unit

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
