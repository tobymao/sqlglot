from setuptools import find_packages, setup

setup(
    name="sqlglot",
    description="An easily customizable SQL parser and transpiler",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tobymao/sqlglot",
    author="Toby Mao",
    author_email="toby.mao@gmail.com",
    license="MIT",
    packages=find_packages(include=["sqlglot", "sqlglot.*"]),
    package_data={"sqlglot": ["py.typed"]},
    use_scm_version={
        "write_to": "sqlglot/_version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    extras_require={
        "dev": [
            "autoflake",
            "black",
            "duckdb>=0.6",
            "isort",
            "mypy>=0.990",
            "pandas",
            "pyspark",
            "python-dateutil",
            "pdoc",
            "pre-commit",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
