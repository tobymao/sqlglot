![SQLGlot logo](sqlglot.svg)

SQLGlot is a no-dependency SQL parser, transpiler, optimizer, and engine. It can be used to format SQL or translate between [19 different dialects](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/__init__.py) like [DuckDB](https://duckdb.org/), [Presto](https://prestodb.io/), [Spark](https://spark.apache.org/), [Snowflake](https://www.snowflake.com/en/), and [BigQuery](https://cloud.google.com/bigquery/). It aims to read a wide variety of SQL inputs and output syntactically and semantically correct SQL in the targeted dialects.

It is a very comprehensive generic SQL parser with a robust [test suite](https://github.com/tobymao/sqlglot/blob/main/tests/). It is also quite [performant](#benchmarks), while being written purely in Python.

You can easily [customize](#custom-dialects) the parser, [analyze](#metadata) queries, traverse expression trees, and programmatically [build](#build-and-modify-sql) SQL.

Syntax [errors](#parser-errors) are highlighted and dialect incompatibilities can warn or raise depending on configurations. However, it should be noted that SQL validation is not SQLGlot’s goal, so some syntax errors may go unnoticed.

Learn more about the SQLGlot API in the [documentation](https://sqlglot.com/).

Contributions are very welcome in SQLGlot; read the [contribution guide](https://github.com/tobymao/sqlglot/blob/main/CONTRIBUTING.md) to get started!

## Table of Contents

* [Install](#install)
* [Versioning](#versioning)
* [Get in Touch](#get-in-touch)
* [Examples](#examples)
   * [Formatting and Transpiling](#formatting-and-transpiling)
   * [Metadata](#metadata)
   * [Parser Errors](#parser-errors)
   * [Unsupported Errors](#unsupported-errors)
   * [Build and Modify SQL](#build-and-modify-sql)
   * [SQL Optimizer](#sql-optimizer)
   * [AST Introspection](#ast-introspection)
   * [AST Diff](#ast-diff)
   * [Custom Dialects](#custom-dialects)
   * [SQL Execution](#sql-execution)
* [Used By](#used-by)
* [Documentation](#documentation)
* [Run Tests and Lint](#run-tests-and-lint)
* [Benchmarks](#benchmarks)
* [Optional Dependencies](#optional-dependencies)

## Install

From PyPI:

```
pip3 install sqlglot
```

Or with a local checkout:

```
make install
```

Requirements for development (optional):

```
make install-dev
```

## Versioning

Given a version number `MAJOR`.`MINOR`.`PATCH`, SQLGlot uses the following versioning strategy:

- The `PATCH` version is incremented when there are backwards-compatible fixes or feature additions.
- The `MINOR` version is incremented when there are backwards-incompatible fixes or feature additions.
- The `MAJOR` version is incremented when there are significant backwards-incompatible fixes or feature additions.

## Get in Touch

We'd love to hear from you. Join our community [Slack channel](https://tobikodata.com/slack)!

## Examples

### Formatting and Transpiling

Easily translate from one dialect to another. For example, date/time functions vary between dialects and can be hard to deal with:

```python
import sqlglot
sqlglot.transpile("SELECT EPOCH_MS(1618088028295)", read="duckdb", write="hive")[0]
```

```sql
'SELECT FROM_UNIXTIME(1618088028295 / 1000)'
```

SQLGlot can even translate custom time formats:

```python
import sqlglot
sqlglot.transpile("SELECT STRFTIME(x, '%y-%-m-%S')", read="duckdb", write="hive")[0]
```

```sql
"SELECT DATE_FORMAT(x, 'yy-M-ss')"
```

As another example, let's suppose that we want to read in a SQL query that contains a CTE and a cast to `REAL`, and then transpile it to Spark, which uses backticks for identifiers and `FLOAT` instead of `REAL`:

```python
import sqlglot

sql = """WITH baz AS (SELECT a, c FROM foo WHERE a = 1) SELECT f.a, b.b, baz.c, CAST("b"."a" AS REAL) d FROM foo f JOIN bar b ON f.a = b.a LEFT JOIN baz ON f.a = baz.a"""
print(sqlglot.transpile(sql, write="spark", identify=True, pretty=True)[0])
```

```sql
WITH `baz` AS (
  SELECT
    `a`,
    `c`
  FROM `foo`
  WHERE
    `a` = 1
)
SELECT
  `f`.`a`,
  `b`.`b`,
  `baz`.`c`,
  CAST(`b`.`a` AS FLOAT) AS `d`
FROM `foo` AS `f`
JOIN `bar` AS `b`
  ON `f`.`a` = `b`.`a`
LEFT JOIN `baz`
  ON `f`.`a` = `baz`.`a`
```

Comments are also preserved on a best-effort basis when transpiling SQL code:

```python
sql = """
/* multi
   line
   comment
*/
SELECT
  tbl.cola /* comment 1 */ + tbl.colb /* comment 2 */,
  CAST(x AS INT), # comment 3
  y               -- comment 4
FROM
  bar /* comment 5 */,
  tbl #          comment 6
"""

print(sqlglot.transpile(sql, read='mysql', pretty=True)[0])
```

```sql
/* multi
   line
   comment
*/
SELECT
  tbl.cola /* comment 1 */ + tbl.colb /* comment 2 */,
  CAST(x AS INT), /* comment 3 */
  y /* comment 4 */
FROM bar /* comment 5 */, tbl /*          comment 6 */
```


### Metadata

You can explore SQL with expression helpers to do things like find columns and tables:

```python
from sqlglot import parse_one, exp

# print all column references (a and b)
for column in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Column):
    print(column.alias_or_name)

# find all projections in select statements (a and c)
for select in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Select):
    for projection in select.expressions:
        print(projection.alias_or_name)

# find all tables (x, y, z)
for table in parse_one("SELECT * FROM x JOIN y JOIN z").find_all(exp.Table):
    print(table.name)
```

### Parser Errors

When the parser detects an error in the syntax, it raises a ParserError:

```python
import sqlglot
sqlglot.transpile("SELECT foo( FROM bar")
```

```
sqlglot.errors.ParseError: Expecting ). Line 1, Col: 13.
  select foo( FROM bar
              ~~~~
```

Structured syntax errors are accessible for programmatic use:

```python
import sqlglot
try:
    sqlglot.transpile("SELECT foo( FROM bar")
except sqlglot.errors.ParseError as e:
    print(e.errors)
```

```python
[{
  'description': 'Expecting )',
  'line': 1,
  'col': 16,
  'start_context': 'SELECT foo( ',
  'highlight': 'FROM',
  'end_context': ' bar',
  'into_expression': None,
}]
```

### Unsupported Errors

Presto `APPROX_DISTINCT` supports the accuracy argument which is not supported in Hive:

```python
import sqlglot
sqlglot.transpile("SELECT APPROX_DISTINCT(a, 0.1) FROM foo", read="presto", write="hive")
```

```sql
APPROX_COUNT_DISTINCT does not support accuracy
'SELECT APPROX_COUNT_DISTINCT(a) FROM foo'
```

### Build and Modify SQL

SQLGlot supports incrementally building sql expressions:

```python
from sqlglot import select, condition

where = condition("x=1").and_("y=1")
select("*").from_("y").where(where).sql()
```

```sql
'SELECT * FROM y WHERE x = 1 AND y = 1'
```

You can also modify a parsed tree:

```python
from sqlglot import parse_one
parse_one("SELECT x FROM y").from_("z").sql()
```

```sql
'SELECT x FROM z'
```

There is also a way to recursively transform the parsed tree by applying a mapping function to each tree node:

```python
from sqlglot import exp, parse_one

expression_tree = parse_one("SELECT a FROM x")

def transformer(node):
    if isinstance(node, exp.Column) and node.name == "a":
        return parse_one("FUN(a)")
    return node

transformed_tree = expression_tree.transform(transformer)
transformed_tree.sql()
```

```sql
'SELECT FUN(a) FROM x'
```

### SQL Optimizer

SQLGlot can rewrite queries into an "optimized" form. It performs a variety of [techniques](https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/optimizer.py) to create a new canonical AST. This AST can be used to standardize queries or provide the foundations for implementing an actual engine. For example:

```python
import sqlglot
from sqlglot.optimizer import optimize

print(
    optimize(
        sqlglot.parse_one("""
            SELECT A OR (B OR (C AND D))
            FROM x
            WHERE Z = date '2021-01-01' + INTERVAL '1' month OR 1 = 0
        """),
        schema={"x": {"A": "INT", "B": "INT", "C": "INT", "D": "INT", "Z": "STRING"}}
    ).sql(pretty=True)
)
```

```sql
SELECT
  (
    "x"."a" <> 0 OR "x"."b" <> 0 OR "x"."c" <> 0
  )
  AND (
    "x"."a" <> 0 OR "x"."b" <> 0 OR "x"."d" <> 0
  ) AS "_col_0"
FROM "x" AS "x"
WHERE
  CAST("x"."z" AS DATE) = CAST('2021-02-01' AS DATE)
```

### AST Introspection

You can see the AST version of the sql by calling `repr`:

```python
from sqlglot import parse_one
print(repr(parse_one("SELECT a + 1 AS z")))
```

```python
(SELECT expressions:
  (ALIAS this:
    (ADD this:
      (COLUMN this:
        (IDENTIFIER this: a, quoted: False)), expression:
      (LITERAL this: 1, is_string: False)), alias:
    (IDENTIFIER this: z, quoted: False)))
```

### AST Diff

SQLGlot can calculate the difference between two expressions and output changes in a form of a sequence of actions needed to transform a source expression into a target one:

```python
from sqlglot import diff, parse_one
diff(parse_one("SELECT a + b, c, d"), parse_one("SELECT c, a - b, d"))
```

```python
[
  Remove(expression=(ADD this:
    (COLUMN this:
      (IDENTIFIER this: a, quoted: False)), expression:
    (COLUMN this:
      (IDENTIFIER this: b, quoted: False)))),
  Insert(expression=(SUB this:
    (COLUMN this:
      (IDENTIFIER this: a, quoted: False)), expression:
    (COLUMN this:
      (IDENTIFIER this: b, quoted: False)))),
  Move(expression=(COLUMN this:
    (IDENTIFIER this: c, quoted: False))),
  Keep(source=(IDENTIFIER this: b, quoted: False), target=(IDENTIFIER this: b, quoted: False)),
  ...
]
```

See also: [Semantic Diff for SQL](https://github.com/tobymao/sqlglot/blob/main/posts/sql_diff.md).

### Custom Dialects

[Dialects](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects) can be added by subclassing `Dialect`:

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

print(Dialect["custom"])
```

```
<class '__main__.Custom'>
```

### SQL Execution

One can even interpret SQL queries using SQLGlot, where the tables are represented as Python dictionaries. Although the engine is not very fast (it's not supposed to be) and is in a relatively early stage of development, it can be useful for unit testing and running SQL natively across Python objects. Additionally, the foundation can be easily integrated with fast compute kernels (arrow, pandas). Below is an example showcasing the execution of a SELECT expression that involves aggregations and JOINs:

```python
from sqlglot.executor import execute

tables = {
    "sushi": [
        {"id": 1, "price": 1.0},
        {"id": 2, "price": 2.0},
        {"id": 3, "price": 3.0},
    ],
    "order_items": [
        {"sushi_id": 1, "order_id": 1},
        {"sushi_id": 1, "order_id": 1},
        {"sushi_id": 2, "order_id": 1},
        {"sushi_id": 3, "order_id": 2},
    ],
    "orders": [
        {"id": 1, "user_id": 1},
        {"id": 2, "user_id": 2},
    ],
}

execute(
    """
    SELECT
      o.user_id,
      SUM(s.price) AS price
    FROM orders o
    JOIN order_items i
      ON o.id = i.order_id
    JOIN sushi s
      ON i.sushi_id = s.id
    GROUP BY o.user_id
    """,
    tables=tables
)
```

```python
user_id price
      1   4.0
      2   3.0
```

See also: [Writing a Python SQL engine from scratch](https://github.com/tobymao/sqlglot/blob/main/posts/python_sql_engine.md).

## Used By

* [SQLMesh](https://github.com/TobikoData/sqlmesh)
* [Fugue](https://github.com/fugue-project/fugue)
* [ibis](https://github.com/ibis-project/ibis)
* [mysql-mimic](https://github.com/kelsin/mysql-mimic)
* [Querybook](https://github.com/pinterest/querybook)
* [Quokka](https://github.com/marsupialtail/quokka)
* [Splink](https://github.com/moj-analytical-services/splink)

## Documentation

SQLGlot uses [pdoc](https://pdoc.dev/) to serve its API documentation.

A hosted version is on the [SQLGlot website](https://sqlglot.com/), or you can build locally with:

```
make docs-serve
```

## Run Tests and Lint

```
make style  # Only linter checks
make unit   # Only unit tests
make check  # Full test suite & linter checks
```

## Benchmarks

[Benchmarks](https://github.com/tobymao/sqlglot/blob/main/benchmarks/bench.py) run on Python 3.10.5 in seconds.

|           Query |         sqlglot |        sqlfluff |         sqltree |        sqlparse |  moz_sql_parser |        sqloxide |
| --------------- | --------------- | --------------- | --------------- | --------------- | --------------- | --------------- |
|            tpch |   0.01308 (1.0) | 1.60626 (122.7) | 0.01168 (0.893) | 0.04958 (3.791) | 0.08543 (6.531) | 0.00136 (0.104) |
|           short |   0.00109 (1.0) | 0.14134 (129.2) | 0.00099 (0.906) | 0.00342 (3.131) | 0.00652 (5.970) | 8.76E-5 (0.080) |
|            long |   0.01399 (1.0) | 2.12632 (151.9) | 0.01126 (0.805) | 0.04410 (3.151) | 0.06671 (4.767) | 0.00107 (0.076) |
|           crazy |   0.03969 (1.0) | 24.3777 (614.1) | 0.03917 (0.987) | 11.7043 (294.8) | 1.03280 (26.02) | 0.00625 (0.157) |


## Optional Dependencies

SQLGlot uses [dateutil](https://github.com/dateutil/dateutil) to simplify literal timedelta expressions. The optimizer will not simplify expressions like the following if the module cannot be found:

```sql
x + interval '1' month
```
