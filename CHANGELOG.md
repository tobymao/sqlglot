Changelog
=========

v10.2.0
------

Changes:

- Breaking: types inferred from annotate_types are now DataType objects, instead of DataType.Type.

- New: the optimizer can now simplify [BETWEEN expressions expressed as explicit comparisons](https://github.com/tobymao/sqlglot/commit/e24d0317dfa644104ff21d009b790224bf84d698).

- New: the optimizer now removes redundant casts.

- New: added support for Redshift's ENCODE/DECODE.

- New: the optimizer now [treats identifiers as case-insensitive](https://github.com/tobymao/sqlglot/commit/638ed265f195219d7226f4fbae128f1805ae8988).

- New: the optimizer now [handles nested CTEs](https://github.com/tobymao/sqlglot/commit/1bdd652792889a8aaffb1c6d2c8aa1fe4a066281).

- New: the executor can now execute SELECT DISTINCT expressions.

- New: added support for Redshift's COPY and UNLOAD commands.

- New: added ability to parse LIKE in CREATE TABLE statement.

- New: the optimizer now [unnests scalar subqueries as cross joins](https://github.com/tobymao/sqlglot/commit/4373ad8518ede4ef1fda8b247b648c680a93d12d).

- Improvement: fixed Bigquery's ARRAY function parsing, so that it can now handle a SELECT expression as an argument.

- Improvement: improved Snowflake's [ARRAY and MAP constructs](https://github.com/tobymao/sqlglot/commit/0506657dba55fe71d004c81c907e23cdd2b37d82).

- Improvement: fixed transpilation between STRING_AGG and GROUP_CONCAT.

- Improvement: the INTO clause can now be parsed in SELECT expressions.

- Improvement: improve executor; it currently executes all TPC-H queries up to TPC-H 17 (inclusive).

- Improvement: DISTINCT ON is now transpiled to a SELECT expression from a subquery for Redshift.

v10.1.0
------

Changes:

- Breaking: [refactored](https://github.com/tobymao/sqlglot/commit/6b0da1e1a2b5d6bdf7b5b918400456422d30a1d4) the way SQL comments are handled. Before at most one comment could be attached to an expression, now multiple comments may be stored in a list.

- Breaking: [refactored](https://github.com/tobymao/sqlglot/commit/be332d10404f36b43ea6ece956a73bf451348641) the way properties are represented and parsed. The argument `this` now stores a property's attributes instead of its name.

- New: added structured ParseError properties.

- New: the executor now handles set operations.

- New: sqlglot can [now execute SQL queries](https://github.com/tobymao/sqlglot/commit/62d3496e761a4f38dfa61af793062690923dce74) using python objects.

- New: added support for the [Drill dialect](https://github.com/tobymao/sqlglot/commit/543eca314546e0bd42f97c354807b4e398ab36ec).

- New: added a `canonicalize` method which leverages existing type information for an expression to apply various transformations to it.

- New: TRIM function support for Snowflake and Bigquery.

- New: added support for SQLite primary key ordering constraints (ASC, DESC).

- New: added support for Redshift DISTKEY / SORTKEY / DISTSTYLE properties.

- New: added support for SET TRANSACTION MySQL statements.

- New: added `null`, `true`, `false` helper methods to avoid using singleton expressions.

- Improvement: allow multiple aggregations in an expression.

- Improvement: execution of left / right joins.

- Improvement: execution of aggregations without the GROUP BY clause.

- Improvement: static query execution (e.g. SELECT 1, SELECT CONCAT('a', 'b') AS x, etc).

- Improvement: include a rule for type inference in the optimizer.

- Improvement: transaction, commit expressions parsed [at finer granularity](https://github.com/tobymao/sqlglot/commit/148282e710fd79512bb7d32e6e519d631df8115d).

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

- New: added formats for TSQL's [DATENAME/DATEPART functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/datename-transact-sql?view=sql-server-ver16).

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
