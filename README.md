# SQLGlot

SQLGlot is a no dependency Python SQL parser, transpiler, and optimizer. It can be used to format SQL or translate between different dialects like Presto, Spark, and Hive. It aims to read a wide variety of SQL inputs and output syntatically correct SQL in the targeted dialects.

It is currently the [fastest](#benchmarks) pure-Python SQL parser.

You can easily customize the parser to support UDF's across dialects as well through the transform API.

Syntax errors are highlighted and dialect incompatibilities can warn or raise depending on configurations.

## Install
From PyPI

```
pip3 install sqlglot
```

Or with a local checkout

```
pip3 install -e .
```

## Examples
Easily translate from one dialect to another. For example, date/time functions vary from dialects and can be hard to deal with.

```python
import sqlglot
sqlglot.transpile("SELECT EPOCH_MS(1618088028295)", read='duckdb', write='hive')
```

```sql
SELECT TO_UTC_TIMESTAMP(FROM_UNIXTIME(1618088028295 / 1000, 'yyyy-MM-dd HH:mm:ss'), 'UTC')
```

SQLGlot can even translate custom time formats.
```python
import sqlglot
sqlglot.transpile("SELECT STRFTIME(x, '%y-%-m-%S')", read='duckdb', write='hive')
```

```sql
SELECT DATE_FORMAT(x, 'yy-M-ss')"
```

## Formatting and Transpiling
Read in a SQL statement with a CTE and CASTING to a REAL and then transpiling to Spark.

Spark uses backticks as identifiers and the REAL type is transpiled to FLOAT.

```python
import sqlglot

sql = """WITH baz AS (SELECT a, c FROM foo WHERE a = 1) SELECT f.a, b.b, baz.c, CAST("b"."a" AS REAL) d FROM foo f JOIN bar b ON f.a = b.a LEFT JOIN baz ON f.a = baz.a"""
sqlglot.transpile(sql, write='spark', identify=True, pretty=True)[0]
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

## Metadata

You can explore SQL with expression helpers to do things like find columns and tables.

```python
from sqlglot import parse_one, exp

# print all column references (a and b)
for column in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Column):
  print(column.alias_or_name)

# find all projections in select statements (a and c)
for select in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Select):
  for projection in select.args["expressions"]:
    print(projection.alias_or_name)

# find all tables (x, y, z)
for table in parse_one("SELECT * FROM x JOIN y JOIN z").find_all(exp.Table):
  print(table.name)
```

## Parser Errors
A syntax error will result in a parser error.
```python
transpile("SELECT foo( FROM bar")
```
```
sqlglot.errors.ParseError: Expecting ). Line 1, Col: 13.
  SELECT foo( FROM bar
```
## Unsupported Errors
Presto APPROX_DISTINCT supports the accuracy argument which is not supported in Spark.

```python
transpile(
    'SELECT APPROX_DISTINCT(a, 0.1) FROM foo',
    read='presto',
    write='spark',
)
```

```sql
WARNING:root:APPROX_COUNT_DISTINCT does not support accuracy

SELECT APPROX_COUNT_DISTINCT(a) FROM foo
```

## Build and Modify SQL
SQLGlot supports incrementally building sql expressions.

```python
from sqlglot import select, condition

where = condition("x=1").and_("y=1")
select("*").from_("y").where(where).sql()
```
Which outputs:
```sql
SELECT * FROM y WHERE x = 1 AND y = 1
```

You can also modify a parsed tree:

```python
from sqlglot import parse_one

parse_one("SELECT x FROM y").from_("z").sql()
```
Which outputs:
```sql
SELECT x FROM y, z
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
Which outputs:
```sql
SELECT FUN(a) FROM x
```

## SQL Optimizer

SQLGlot can rewrite queries into an "optimized" form. It performs a variety of [techniques](sqlglot/optimizer/optimizer.py) to create a new canonical AST. This AST can be used to standaradize queries or provide the foundations for implementing an actual engine.

```python
import sqlglot
from sqlglot.optimizer import optimize

>>>
optimize(
    sqlglot.parse_one("""
    SELECT A OR (B OR (C AND D))
    FROM x
    WHERE Z = date '2021-01-01' + INTERVAL '1' month OR 1 = 0
    """),
    schema={"x": {"A": "INT", "B": "INT", "C": "INT", "D": "INT", "Z": "STRING"}}
).sql(pretty=True)

"""
SELECT
  (
    "x"."A"
    OR "x"."B"
    OR "x"."C"
  )
  AND (
    "x"."A"
    OR "x"."B"
    OR "x"."D"
  ) AS "_col_0"
FROM "x" AS "x"
WHERE
  "x"."Z" = CAST('2021-02-01' AS DATE)
"""
```

## SQL Annotations

SQLGlot supports annotations in the sql expression. This is an experimental feature that is not part of any of the SQL standards but it can be useful when needing to annotate what a selected field is supposed to be. Below is an example:

```sql
SELECT
  user #primary_key,
  country
FROM users
```

## AST Introspection

You can see the AST version of the sql by calling repr.

```python
from sqlglot import parse_one
repr(parse_one("SELECT a + 1 AS z"))

(SELECT expressions:
  (ALIAS this:
    (ADD this:
      (COLUMN this:
        (IDENTIFIER this: a, quoted: False)), expression:
      (LITERAL this: 1, is_string: False)), alias:
    (IDENTIFIER this: z, quoted: False)))
```

## Custom Dialects

[Dialects](sqlglot/dialects) can be added by subclassing Dialect.

```python
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class Custom(Dialect):
    identifier = "`"

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']

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


Dialects["custom"]
```

## Benchmarks

[Benchmarks](benchmarks) run on Python 3.10.5 in seconds.

| Query            | sqlglot          | [sqlparse](https://github.com/andialbrecht/sqlparse)         | [moz\_sql\_parser](https://github.com/klahnakoski/mo-sql-parsing) | [sqloxide](https://github.com/wseaton/sqloxide/) |
| ---------------- | ---------------- | ---------------- | ---------------- | ---------------- |
| short            | 0.00033          | 0.00099          | 0.00160          | 0.000063
| long             | 0.00426          | 0.01396          | 0.02023          | 0.000595
| crazy            | 0.01363          | 3.69641          | 0.34818          | 0.003121


## Run Tests and Lint
```
pip install -r requirements.txt
./format_code.sh
./run_checks.sh
```

## Optional Dependencies
SQLGlot uses [dateutil](https://github.com/dateutil/dateutil) to simplify literal timedelta expressions. The optimizer will not simplify expressions like

```sql
x + interval '1' month
```

if the module cannot be found.
