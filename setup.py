from setuptools import setup
from setuptools_scm import get_version

version = get_version(local_scheme="no-local-version")

setup(
    extras_require={
        "dev": [
            "duckdb>=0.6",
            # sqlglot-mypy 1.20+ is the build dep for sqlglotc and only ships
            # for py3.10+; on py3.9 just use upstream mypy for type checking.
            "sqlglot-mypy >= 1.20.0; python_version >= '3.10'",
            "mypy; python_version < '3.10'",
            "setuptools_scm",
            "pandas",
            "pandas-stubs",
            "python-dateutil",
            "pytz",
            "pdoc",
            "pre-commit",
            "ruff==0.15.6",
            "types-python-dateutil",
            "types-pytz",
            "typing_extensions",
            "pyperf",
        ],
        # Compiles from source on the user's machine. Requires Python 3.10+
        # because the build dep (sqlglot-mypy 1.20+) dropped 3.9; on 3.9
        # `pip install sqlglot[c]` is a no-op and you just get pure-Python sqlglot.
        "c": [f"sqlglotc=={version}; python_version >= '3.10'"],
        # Deprecated: the Rust tokenizer has been replaced by sqlglotc.
        "rs": ["sqlglotrs==0.13.0", f"sqlglotc=={version}; python_version >= '3.10'"],
    },
)
