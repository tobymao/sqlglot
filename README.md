# SQLGlot

SQLGlot is a no dependency Python SQL parser and transpiler. It can be used to format SQL or translate between different dialects like Presto, Spark, and SQLite. It aims to read a wide variety of SQL inputs and output syntatically correct SQL in the targeted dialects.

This project is actively in development and alpha level quality.

You can easily customize the parser to support UDF's across dialects as well through the transform API.

Syntax errors are highlighted and dialect incompatibilities can warn or raise depending on configurations.

## Examples

### Formatting and Transpiling
Read in a SQL statement with a CTE and CASTING to a REAL and then transpiling to Spark.

Spark uses backticks as identifiers and the REAL type is transpiled to FLOAT.

```python
import sqlglot

sql = """WITH baz AS (SELECT a, c FROM foo WHERE a = 1) SELECT f.a, b.b, baz.c, CAST("b"."a" AS REAL) d FROM foo f JOIN bar b ON f.a = b.a LEFT JOIN baz ON f.a = baz.a"""
sqlglot.transpile(sql, write='spark', identify=True, pretty=True)[0])
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

### Custom Transforms
A simple transform on types can be accomplished by providing a dict of Expression/TokenType => lambda/string
```python

from sqlglot import *

transpile("SELECT CAST(a AS INT) FROM x", transforms={TokenType.INT: 'SPECIAL INT'})[0]
```

```sql
SELECT CAST(a AS SPECIAL INT) FROM x
```

More complicated transforms can be accomplished by using the Tokenizer, Parser, and Generator directly.

In  this example, we want to parse a UDF SPECIAL_UDF and then output another version called SPECIAL_UDF_INVERSE with the arguments switched.

```python
from sqlglot import *
from sqlglot.expressions import Func

class SpecialUDF(Func):
    arg_types = {'a': True, 'b': True}

tokens = Tokenizer().tokenize("SELECT SPECIAL_UDF(a, b) FROM x")
```
Here is the output of the tokenizer.

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
    'SPECIAL_UDF': lambda args: SpecialUDF(a=args[0], b=args[1]),
}).parse(tokens)[0]
```

The expression tree produced by the parser.

```
(FROM this:
 (TABLE this: x, db: ), expression:
 (SELECT expressions:
  (COLUMN this:
   (FUNC a:
    (COLUMN this: a, db: , table: ), b:
    (COLUMN this: b, db: , table: )), db: , table: )))
```

Finally generating the new SQL.

```python
Generator(transforms={
    SpecialUDF: lambda self, e: f"SPECIAL_UDF_INVERSE({self.sql(e, 'b')}, {self.sql(e, 'a')})"
}).generate(expression)
```

```sql
SELECT SPECIAL_UDF_INVERSE(b, a) FROM x
```

### Parse Errors
A syntax error will result in an parse error.
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
