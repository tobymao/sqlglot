.PHONY: install install-dev install-pre-commit test unit style check docs docs-serve

install:
	pip install -e .

gen-rs-token-type:
	python gen_rs_token_type.py sqlglotrs/src/token_type.rs

install-dev-rs: gen-rs-token-type
	cd sqlglotrs/ && python -m maturin develop

install-dev-core:
	pip install -e ".[dev]"

install-dev: install-dev-core install-dev-rs

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
