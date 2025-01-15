.PHONY: install install-dev install-pre-commit test unit style check docs docs-serve

install:
	pip install -e .

bench: install-dev-rs-release
	python benchmarks/bench.py

install-dev-rs-release:
	cd sqlglotrs/ && python -m maturin develop -r

install-dev-rs:
	@unset CONDA_PREFIX && \
	cd sqlglotrs/ && python -m maturin develop

install-dev-core:
	pip install -e ".[dev]"

install-dev: install-dev-core install-dev-rs

install-pre-commit:
	pre-commit install

test:
	SQLGLOTRS_TOKENIZER=0 python -m unittest

test-rs:
	RUST_BACKTRACE=1 python -m unittest

unit:
	SKIP_INTEGRATION=1 SQLGLOTRS_TOKENIZER=0 python -m unittest

unit-rs:
	SKIP_INTEGRATION=1 RUST_BACKTRACE=1 python -m unittest

style:
	pre-commit run --all-files

check: style test test-rs

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
