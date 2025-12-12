.PHONY: install install-dev install-pre-commit test test-fast test-fast-rs unit style check docs docs-serve

ifdef UV
    PIP := uv pip
else
    PIP := pip
endif

install:
	$(PIP) install -e .

bench: install-dev-rs-release
	python -m benchmarks.bench

bench-optimize: install-dev-rs-release
	python -m benchmarks.optimize

install-dev-rs-release:
	cd sqlglotrs/ && python -m maturin develop -r

install-dev-rs:
	@unset CONDA_PREFIX && \
	cd sqlglotrs/ && python -m maturin develop

install-dev-core:
	$(PIP) install -e ".[dev]"

install-dev: install-dev-core install-dev-rs

install-pre-commit:
	pre-commit install

test:
	SQLGLOTRS_TOKENIZER=0 python -m unittest

test-fast:
	SQLGLOTRS_TOKENIZER=0 python -m unittest --failfast

test-rs:
	RUST_BACKTRACE=1 python -m unittest

test-fast-rs:
	RUST_BACKTRACE=1 python -m unittest --failfast

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
