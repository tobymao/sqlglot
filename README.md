# SQLGlot

SQLGlot is a no dependency Python SQL parser and transpiler. It can be used to format SQL or translate between different dialects like Presto, Spark, and Hive. It aims to read a wide variety of SQL inputs and output syntatically correct SQL in the targeted dialects.

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

### Formatting and Transpiling
Read in a SQL statement with a CTE and CASTING to a REAL and then transpiling to Spark.

Spark uses backticks as identifiers and the REAL type is transpiled to FLOAT.

```python
import sqlglot

sql = """WITH baz AS (SELECT a, c FROM foo WHERE a = 1) SELECT f.a, b.b, baz.c, CAST("b"."a" AS REAL) d FROM foo f JOIN bar b ON f.a = b.a LEFT JOIN baz ON f.a = baz.a"""
sqlglot.transpile(sql, write='spark', identify=True, pretty=True)[0]
```

```sql
WITH baz AS (
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
  CAST(`b`.`a` AS FLOAT) AS d
FROM `foo` AS f
JOIN `bar` AS b ON
  `f`.`a` = `b`.`a`
LEFT JOIN `baz` ON
  `f`.`a` = `baz`.`a`
```

### Customization
#### Custom Types
A simple transform on types can be accomplished by providing a corresponding mapping:
```python

from sqlglot import *
from sqlglot import expressions as exp

transpile("SELECT CAST(a AS INT) FROM x", type_mapping={exp.DataType.Type.INT: "SPECIAL INT"})[0]
```

```sql
SELECT CAST(a AS SPECIAL INT) FROM x
```

More complicated transforms can be accomplished by using the Tokenizer, Parser, and Generator directly.
#### Custom Functions
In  this example, we want to parse a UDF SPECIAL_UDF and then output another version called SPECIAL_UDF_INVERSE with the arguments switched.

```python
from sqlglot import *
from sqlglot.expressions import Func

class SpecialUdf(Func):
    arg_types = {'a': True, 'b': True}

tokens = Tokenizer().tokenize("SELECT SPECIAL_UDF(a, b) FROM x")
```
Here is the output of the tokenizer:

```
[
    <Token token_type: TokenType.SELECT, text: SELECT, line: 0, col: 0>,
    <Token token_type: TokenType.VAR, text: SPECIAL_UDF, line: 0, col: 7>,
    <Token token_type: TokenType.L_PAREN, text: (, line: 0, col: 18>,
    <Token token_type: TokenType.VAR, text: a, line: 0, col: 19>,
    <Token token_type: TokenType.COMMA, text: ,, line: 0, col: 20>,
    <Token token_type: TokenType.VAR, text: b, line: 0, col: 22>,
    <Token token_type: TokenType.R_PAREN, text: ), line: 0, col: 23>,
    <Token token_type: TokenType.FROM, text: FROM, line: 0, col: 25>,
    <Token token_type: TokenType.VAR, text: x, line: 0, col: 30>,
]

```
```python
expression = Parser(functions={
    **SpecialUdf.default_parser_mappings(),
}).parse(tokens)[0]
```

The expression tree produced by the parser:

```
(SELECT distinct: False, expressions:
  (SPECIALUDF a:
    (COLUMN this:
      (IDENTIFIER this: a, quoted: False)), b:
    (COLUMN this:
      (IDENTIFIER this: b, quoted: False))), from:
  (FROM expressions:
    (TABLE this:
      (IDENTIFIER this: x, quoted: False))))
```

Finally generating the new SQL:

```python
Generator(transforms={
    SpecialUdf: lambda self, e: f"SPECIAL_UDF_INVERSE({self.sql(e, 'b')}, {self.sql(e, 'a')})"
}).generate(expression)
```

```sql
SELECT SPECIAL_UDF_INVERSE(b, a) FROM x
```

#### Syntax Tree Transformation
There is also a way to transform the parsed tree directly by applying a mapping function to each tree node recursively:
```python
import sqlglot
import sqlglot.expressions as exp

expression_tree = sqlglot.parse_one("SELECT a FROM x")

def transformer(node):
    if isinstance(node, exp.Column) and node.args["this"].text == "a":
        return sqlglot.parse_one("FUN(a)")
    return node

transformed_tree = expression_tree.transform(transformer)
transformed_tree.sql()
```
The snippet above produces the following transformed expression:
```sql
SELECT FUN(a) FROM x
```

### Parser Errors
A syntax error will result in a parser error.
```python
transpile("SELECT foo( FROM bar")
```
```
sqlglot.errors.ParseError: Expected )
  SELECT foo( __FROM__ bar
```
### Unsupported Errors
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

### Rewrite Sql
Modify sql expressions like adding a CTAS

```python
from sqlglot import Generator, parse_one
from sqlglot.rewriter import Rewriter

expression = parse_one("SELECT * FROM y")
Rewriter(expression).ctas('x').expression.sql()
```

```sql
CREATE TABLE x AS SELECT * FROM y
```

### Benchmarks

[Benchmarks](benchmarks) run on Python 3.9.6 in seconds.

| Query            | sqlglot          | [sqlparse](https://github.com/andialbrecht/sqlparse)         | [moz\_sql\_parser](https://github.com/klahnakoski/mo-sql-parsing) | [sqloxide](https://github.com/wseaton/sqloxide/) |
| ---------------- | ---------------- | ---------------- | ---------------- | ---------------- |
| short            | 0.00038          | 0.00104          | 0.00174          | 0.000060
| long             | 0.00508          | 0.01522          | 0.02162          | 0.000597
| crazy            | 0.01871          | 3.49415          | 0.35346          | 0.003104


## Run Tests and Lint
```python -m unittest && python -m pylint sqlglot/ tests/```
