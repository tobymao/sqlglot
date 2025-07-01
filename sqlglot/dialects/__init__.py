# ruff: noqa: F401
"""
## Dialects

While there is a SQL standard, most SQL engines support a variation of that standard. This makes it difficult
to write portable SQL code. SQLGlot bridges all the different variations, called "dialects", with an extensible
SQL transpilation framework.

The base `sqlglot.dialects.dialect.Dialect` class implements a generic dialect that aims to be as universal as possible.

Each SQL variation has its own `Dialect` subclass, extending the corresponding `Tokenizer`, `Parser` and `Generator`
classes as needed.

### Implementing a custom Dialect

Creating a new SQL dialect may seem complicated at first, but it is actually quite simple in SQLGlot:

```python
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class Custom(Dialect):
    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']  # Strings can be delimited by either single or double quotes
        IDENTIFIERS = ["`"]  # Identifiers can be delimited by backticks

        # Associates certain meaningful words with tokens that capture their intent
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
        }

    class Generator(Generator):
        # Specifies how AST nodes, i.e. subclasses of exp.Expression, should be converted into SQL
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
        }

        # Specifies how AST nodes representing data types should be converted into SQL
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.TEXT: "STRING",
        }
```

The above example demonstrates how certain parts of the base `Dialect` class can be overridden to match a different
specification. Even though it is a fairly realistic starting point, we strongly encourage the reader to study existing
dialect implementations in order to understand how their various components can be modified, depending on the use-case.

----
"""

import importlib
import threading

DIALECTS = [
    "Athena",
    "BigQuery",
    "ClickHouse",
    "Databricks",
    "Doris",
    "Dremio",
    "Drill",
    "Druid",
    "DuckDB",
    "Dune",
    "Fabric",
    "Hive",
    "Materialize",
    "MySQL",
    "Oracle",
    "Postgres",
    "Presto",
    "PRQL",
    "Redshift",
    "RisingWave",
    "Snowflake",
    "Spark",
    "Spark2",
    "SQLite",
    "StarRocks",
    "Tableau",
    "Teradata",
    "Trino",
    "TSQL",
    "Exasol",
]

MODULE_BY_DIALECT = {name: name.lower() for name in DIALECTS}
DIALECT_MODULE_NAMES = MODULE_BY_DIALECT.values()

MODULE_BY_ATTRIBUTE = {
    **MODULE_BY_DIALECT,
    "Dialect": "dialect",
    "Dialects": "dialect",
}

__all__ = list(MODULE_BY_ATTRIBUTE)

# We use a reentrant lock because a dialect may depend on (i.e., import) other dialects.
# Without it, the first dialect import would never be completed, because subsequent
# imports would be blocked on the lock held by the first import.
_import_lock = threading.RLock()


def __getattr__(name):
    module_name = MODULE_BY_ATTRIBUTE.get(name)
    if module_name:
        with _import_lock:
            module = importlib.import_module(f"sqlglot.dialects.{module_name}")
        return getattr(module, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
