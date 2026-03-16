.PHONY: install install-dev install-devc install-devc-release install-pre-commit bench bench-parse bench-optimize test test-fast unit testc unitc style check docs docs-serve hidec showc clean resolve-integration-conflicts

ifdef UV
    PIP := uv pip
else
    PIP := pip
endif

SO_BACKUP := /tmp/sqlglot_so_backup

hidec:
	rm -rf $(SO_BACKUP) && (find sqlglot sqlglotc -name "*.so"; ls *__mypyc*.so 2>/dev/null) | tar cf $(SO_BACKUP) -T - 2>/dev/null && find sqlglot sqlglotc -name "*.so" -delete && rm -f *__mypyc*.so; true

showc:
	tar xf $(SO_BACKUP) 2>/dev/null; rm -f $(SO_BACKUP); true

clean:
	rm -rf build sqlglotc/build sqlglotc/dist sqlglotc/*.egg-info sqlglotc/sqlglot
	find sqlglot sqlglotc build -name "*.so" -delete 2>/dev/null; rm -f *__mypyc*.so; true

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"
	git submodule update --init 2>/dev/null || true
	@if ! command -v gh >/dev/null 2>&1; then \
		echo ""; \
		echo "gh (GitHub CLI) is not installed. It is needed to auto-create PRs for integration tests."; \
		printf "Install it via brew? [y/N] "; \
		read answer; \
		if [ "$$answer" = "y" ] || [ "$$answer" = "Y" ]; then \
			brew install gh; \
		else \
			echo "Skipping. You can install it later: https://cli.github.com/"; \
		fi; \
	fi

install-devc:
	cd sqlglotc && MYPYC_OPT=0 python setup.py build_ext --inplace

install-devc-release: clean
	cd sqlglotc && $(PIP) install -e .

install-pre-commit:
	pre-commit install
	pre-commit install --hook-type post-checkout
	pre-commit install --hook-type pre-push
	pre-commit install --hook-type post-merge
	@printf '#!/bin/bash\n.github/scripts/integration_tests_sync.sh post-commit\n' > .git/hooks/post-commit
	@chmod +x .git/hooks/post-commit

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
