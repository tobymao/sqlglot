from setuptools import setup

setup(
    extras_require={
        "dev": [
            "duckdb>=0.6",
            "mypy",
            "pandas",
            "pandas-stubs",
            "python-dateutil",
            "pytz",
            "pdoc",
            "pre-commit",
            "ruff",
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
