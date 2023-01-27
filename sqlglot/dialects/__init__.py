"""
## Dialects

One of the core abstractions in SQLGlot is the concept of a "dialect". The `Dialect` class essentially implements a
"SQLGlot dialect", which aims to be as generic and ANSI-compliant as possible. It relies on the base `Tokenizer`,
`Parser` and `Generator` classes to achieve this goal, so these need to be very lenient when it comes to consuming
SQL code.

However, there are cases where the syntax of different SQL dialects varies wildly, even for common tasks. One such
example is the date/time functions, which can be hard to deal with. For this reason, it's sometimes necessary to
override the base dialect in order to specialize its behavior. This can be easily done in SQLGlot: supporting new
dialects is as simple as subclassing from `Dialect` and overriding its various components (e.g. the `Parser` class),
in order to implement the target behavior.


### Implementing a custom Dialect

Consider the following example:

```python
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class Custom(Dialect):
    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
        }

    class Generator(Generator):
        TRANSFORMS = {exp.Array: lambda self, e: f"[{self.expressions(e)}]"}

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

This is a typical example of adding a new dialect implementation in SQLGlot: we specify its identifier and string
delimiters, as well as what tokens it uses for its types and how they're associated with SQLGlot types. Since
the `Expression` classes are common for each dialect supported in SQLGlot, we may also need to override the generation
logic for some expressions; this is usually done by adding new entries to the `TRANSFORMS` mapping.

----
"""

from sqlglot.dialects.bigquery import BigQuery
from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.dialects.databricks import Databricks
from sqlglot.dialects.dialect import Dialect, Dialects
from sqlglot.dialects.drill import Drill
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.dialects.hive import Hive
from sqlglot.dialects.mysql import MySQL
from sqlglot.dialects.oracle import Oracle
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.presto import Presto
from sqlglot.dialects.redshift import Redshift
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.sqlite import SQLite
from sqlglot.dialects.starrocks import StarRocks
from sqlglot.dialects.tableau import Tableau
from sqlglot.dialects.teradata import Teradata
from sqlglot.dialects.trino import Trino
from sqlglot.dialects.tsql import TSQL
