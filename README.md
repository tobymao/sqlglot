# sqlglot

SQLGlot is a pure Python SQL parser and transpiler. It can be used to format SQL or translate between different dialects like Presto, Spark, and SQLite.

You can easily customize the parser to support UDF's across dialects as well through the transform API.**h**h****

## Example

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
