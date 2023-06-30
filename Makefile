.PHONY: install install-dev install-pre-commit test unit style check docs docs-serve

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	python -m unittest

unit:
	SKIP_INTEGRATION=1 python -m unittest

style:
	pre-commit run --all-files

check: style test

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
