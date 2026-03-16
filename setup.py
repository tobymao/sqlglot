from setuptools import setup
from setuptools_scm import get_version

version = get_version(local_scheme="no-local-version")

setup(
    extras_require={
        "dev": [
            "duckdb>=0.6",
            "sqlglot-mypy>=1.19.1.post1",
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
        # Compiles from source on the user's machine.
        "c": [f"sqlglotc=={version}"],
        # Deprecated: the Rust tokenizer has been replaced by sqlglotc.
        "rs": ["sqlglotrs==0.13.0", f"sqlglotc=={version}"],
    },
)
