from setuptools import setup

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
        "c": ["sqlglotc"],
        # Deprecated: the Rust tokenizer has been replaced by sqlglotc.
        "rs": ["sqlglotrs==0.13.0", "sqlglotc"],
    },
)
