.PHONY: install install-dev install-devc install-pre-commit bench bench-parse bench-optimize test test-fast unit testc unitc style check docs docs-serve hidec showc clean resolve-integration-conflicts

ifdef UV
    PIP := uv pip
else
    PIP := pip
endif

SO_BACKUP := /tmp/sqlglot_so_backup

hidec:
	rm -rf $(SO_BACKUP) && find sqlglot sqlglotc -name "*.so" | tar cf $(SO_BACKUP) -T - --remove-files 2>/dev/null; true

showc:
	tar xf $(SO_BACKUP) 2>/dev/null; rm -f $(SO_BACKUP); true

clean:
	rm -rf build sqlglotc/build sqlglotc/dist sqlglotc/*.egg-info sqlglotc/sqlglot
	find sqlglot sqlglotc build -name "*.so" -delete 2>/dev/null; true

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"
	git submodule update --init 2>/dev/null || true

install-devc: clean
	cd sqlglotc && $(PIP) install -e .

install-pre-commit:
	pre-commit install
	pre-commit install --hook-type post-checkout
	pre-commit install --hook-type pre-push
	pre-commit install --hook-type post-merge

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

resolve-integration-conflicts:
	cd sqlglot-integration-tests && git pull --rebase --autostash
