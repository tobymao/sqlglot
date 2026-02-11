from setuptools import setup

ext_modules = []
try:
    from mypyc.build import mypycify

    ext_modules = mypycify(
        [
            "sqlglot/tokenizer_core.py",
            "sqlglot/trie.py",
            "sqlglot/expressions_core.py",
        ]
    )
except ImportError as e:
    print(e)


def sqlglotrs_version():
    with open("sqlglotrs/Cargo.toml", encoding="utf-8") as fd:
        for line in fd.readlines():
            if line.strip().startswith("version"):
                return line.split("=")[1].strip().strip('"')
    raise ValueError("Could not find version in Cargo.toml")


# Everything is defined in pyproject.toml except the extras because for the [rs] extra we need to dynamically
# read the sqlglotrs version. [dev] has to be specified here as well because you cant specify some extras groups
# dynamically and others statically, it has to be either all dynamic or all static
# ref: https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html#dynamic-metadata
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
            "maturin>=1.4,<2.0",
            "pyperf",
        ],
        "rs": [f"sqlglotrs=={sqlglotrs_version()}"],
    },
    ext_modules=ext_modules,
)
