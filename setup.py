from setuptools import setup

version = (
    open("sqlglot/__init__.py")
    .read()
    .split("__version__ = ")[-1]
    .split("\n")[0]
    .strip("")
    .strip("'")
    .strip('"')
)

setup(
    name="sqlglot",
    version=version,
    description="An easily customizable SQL parser and transpiler",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tobymao/sqlglot",
    author="Toby Mao",
    author_email="toby.mao@gmail.com",
    license="MIT",
    packages=["sqlglot"],
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
