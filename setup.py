from setuptools import setup


def companion_version() -> str:
    """Return the version to pin for sqlglotc.

    The companion package is built from the same git repo and tagged alongside
    sqlglot, so it shares the same version string.
    """
    try:
        from setuptools_scm import get_version

        return get_version(root=".", relative_to=__file__, fallback_version="0.0.0")
    except Exception:
        return "0.0.0"


_version = companion_version()

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
            "ruff==0.7.2",
            "types-python-dateutil",
            "types-pytz",
            "typing_extensions",
            "pyperf",
        ],
        # Compiles from source on the user's machine.
        "c": [f"sqlglotc=={_version}"],
        # Deprecated: the Rust tokenizer has been replaced by sqlglotc.
        "rs": ["sqlglotrs==0.13.0"],
    },
)
