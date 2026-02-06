.PHONY: install install-dev install-pre-commit bench bench-parse bench-optimize test test-fast test-fast-rs unit style check docs docs-serve

ifdef UV
    PIP := uv pip
else
    PIP := pip
endif

install:
	$(PIP) install -e .

install-dev: install-dev-core install-dev-rs

install-dev-rs-release:
	cd sqlglotrs/ && python -m maturin develop -r

install-dev-rs:
	@unset CONDA_PREFIX && \
	cd sqlglotrs/ && python -m maturin develop

install-dev-core:
	$(PIP) install -e ".[dev]"

install-pre-commit:
	pre-commit install

bench: bench-parse bench-optimize

bench-parse: install-dev-rs-release
	python -m benchmarks.parse

bench-optimize: install-dev-rs-release
	python -m benchmarks.optimize

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

clean:
	rm -rf build/lib* sqlglot/*.so

mypyc:
	rm -rf build/lib* sqlglot/*.so
	python3 setup.py build_ext --inplace
