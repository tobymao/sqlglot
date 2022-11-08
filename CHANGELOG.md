Changelog
=========

v10.0.0
------

Changes:

- Breaking: replaced SQLGlot annotations with comments. Now comments can be preserved after transpilation, and they can appear in other places besides SELECT's expressions.
- Breaking: renamed list_get to seq_get.
- Breaking: activated mypy type checking for SQLGlot.
- New: Azure Databricks support.
- New: placeholders can now be replaced in an expression.
- New: null safe equal operator (<=>).
- New: [SET statements](https://github.com/tobymao/sqlglot/pull/673) for MySQL.
- New: [SHOW commands](https://dev.mysql.com/doc/refman/8.0/en/show.html) for MySQL.
- New: [FORMAT function](https://www.w3schools.com/sql/func_sqlserver_format.asp) for TSQL.
- New: CROSS APPLY / OUTER APPLY [support](https://github.com/tobymao/sqlglot/pull/641) for TSQL.
- New: added formats for TSQL's [DATENAME/DATEPART functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/datename-transact-sql?view=sql-server-ver16)
- New: added styles for TSQL's [CONVERT function](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16).
- Improvement: [refactored the schema](https://github.com/tobymao/sqlglot/pull/668) to be more lenient; before it needed to do an exact match of db.table, now it finds table if there are no ambiguities.
- Improvement: allow functions to [inherit](https://github.com/tobymao/sqlglot/pull/674) their arguments' types, so that annotating CASE, IF etc. is possible.
- Improvement: allow [joining with same names](https://github.com/tobymao/sqlglot/pull/660) in the python executor.
- Improvement: the "using" field can now be set for the [join expression builders](https://github.com/tobymao/sqlglot/pull/636).
- Improvement: qualify_columns [now qualifies](https://github.com/tobymao/sqlglot/pull/635) only non-alias columns in the having clause.

v9.0.0
------

Changes:

- Breaking : Changed AST hierarchy of exp.Table with exp.Alias. Before Tables were children's of their aliases, but in order to simplify the AST and fix some issues, Tables now have an alias property.

v8.0.0
------

Changes:

- Breaking : New add\_table method in Schema ABC.
- New: SQLGlot now supports the [PySpark](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dataframe) dataframe API. This is still relatively experimental.

v7.1.0
------

Changes:

- Improvement: Pretty generator now takes max\_text\_width which breaks segments into new lines
- New: exp.to\_table helper to turn table names into table expression objects
- New: int[] type parsers
- New: annotations are now generated in sql

v7.0.0
------

Changes:

- Breaking: DISTINCT within functions now take in multiple values eg. COUNT(DISTINCT a, b).
    exp.Distinct no longer uses `this` and now uses the expressions property

- New: Expression False kwargs are now excluded from equality checks

- New: Parse DESCRIBE and CREATE SCHEMA

- New: DELETE and VALUES builder

- New: Unused CTE and JOINS are now removed in the optimizer

v6.3.0
------

Changes:

- New: Snowflake [table literals](https://docs.snowflake.com/en/sql-reference/literals-table.html)

- New: Anti and semi joins

- New: Vacuum as a command

- New: Stored procedures

- New: Reweriting derived tables as CTES

- Improvement: Various clickhouse improvements

- Improvement: Optimizer predicate pushdown

- Breaking: DATE\_DIFF default renamed to DATEDIFF


v6.2.0
------

Changes:

- New: TSQL support

- Breaking: Removed $ from tokenizer, added @ placeholders

- Improvement: Nodes can now be removed in transform and replace [8cd81c3](https://github.com/tobymao/sqlglot/commit/8cd81c36561463b9849a8e0c2d70248c5b1feb62)

- Improvement: Snowflake timestamp support

- Improvement: Property conversion for CTAS Builder

- Improvement: Tokenizers are now unique per dialect instance

v6.1.0
------

Changes:

- New: mysql group\_concat separator [49a4099](https://github.com/tobymao/sqlglot/commit/49a4099adc93780eeffef8204af36559eab50a9f)

- Improvement: Better nested select parsing [45603f](https://github.com/tobymao/sqlglot/commit/45603f14bf9146dc3f8b330b85a0e25b77630b9b)
