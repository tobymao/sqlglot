.PHONY: install install-dev install-devc install-pre-commit bench bench-parse bench-optimize test test-fast unit testc unitc style check docs docs-serve hidec showc clean

ifdef UV
    PIP := uv pip
else
    PIP := pip
endif

SO_BACKUP := /tmp/sqlglot_so_backup

hidec:
	mkdir -p $(SO_BACKUP) && find sqlglot -name "*.so" | xargs -I{} mv -f {} $(SO_BACKUP)/ 2>/dev/null; true

showc:
	mv -f $(SO_BACKUP)/*.so sqlglot/ 2>/dev/null; true

clean:
	rm -rf sqlglotc/build sqlglotc/dist sqlglotc/*.egg-info sqlglotc/sqlglot
	find sqlglot -name "*.so" -delete

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"

install-devc: clean
	cd sqlglotc && $(PIP) install -e .

install-pre-commit:
	pre-commit install

bench: bench-parse bench-optimize

bench-parse:
	python -m benchmarks.parse

bench-optimize:
	python -m benchmarks.optimize

test: hidec
	trap '$(MAKE) showc' EXIT; python -m unittest

test-fast:
	python -m unittest --failfast

unit: hidec
	trap '$(MAKE) showc' EXIT; SKIP_INTEGRATION=1 python -m unittest

testc: install-devc
	python -m unittest

unitc: install-devc
	SKIP_INTEGRATION=1 python -m unittest

style:
	pre-commit run --all-files

check: style test testc

docs:
	python pdoc/cli.py -o docs

docs-serve:
	python pdoc/cli.py --port 8002
