Changelog
=========

v11.6.0
------

- Breaking: [Removed](https://github.com/tobymao/sqlglot/commit/08b8623f11214573e17c6948bf71587c0c553c9f) table hints when not supported.

- New: Added support for Databricks' `:` operator.

- New: Added several spark functions, e.g. INT, DATE, TIMESTAMP etc.

- New: Added ON UPDATE CURRENT_TIMESTAMP column constraint in MySQL.

- New: Added date functions (YEAR, MONTH, etc) in the executor.

- New: Added support for Databricks ALTER TABLE .. ADD COLUMN .. FIRST/AFTER .. syntax.

- Improvement: [Made](https://github.com/tobymao/sqlglot/commit/c0fb040562cc7ea052eb7f75783326941282df36) adding index offsets for arrays more robust.

- Improvement: [Added](https://github.com/tobymao/sqlglot/commit/949eba7349d0eba292f2eadaf99ee716decdf275) columns to anonymous values.

- Improvement: [Added](https://github.com/tobymao/sqlglot/commit/9ee9e0a9e85d181c7cba0f709e5f5567dbfddade) support for T-SQL hints, fixed several transpilation issues.

- Improvement: Refactored generation of DATE_SUB for spark.

- Improvement: [Fixed](https://github.com/tobymao/sqlglot/commit/9a3f4c62f6e1b28fceac0adc0a8676013436611d) optimizer bug in simplify related to OR expressions.

- Improvement: [Transformed](https://github.com/tobymao/sqlglot/commit/42c76bac948090d8dffc874b1c89dfea37472229) postgres merge to remove table refs.

- Improvement: Converted VOLATILE from CREATE arg to property.

- Improvement: Fixed parsing of `::` as TryCast.

- Improvement: Improved performance of table addition for schema.

- Improvement: [Removed](https://github.com/tobymao/sqlglot/commit/d13a557e93ebe3eafd0bd80da05186e60caa4a94) default kind in `_parse_drop` to make it more lenient.

- Improvement: [Fixed](https://github.com/tobymao/sqlglot/commit/f040e90fd7b054396d418f3ca15381b1e417b83f) lateral view explode order.

- Improvement: Made `_expand_using` more lenient in qualify_columns.

- Improvement: Fixed various issues related to parsing and transpiling of INTERVAL expressions.

- Improvement: [Ensured](https://github.com/tobymao/sqlglot/commit/8e6f860083b7bbbf0702c090a0864f8d1cf72d8f) name normalization for schema methods.

- Improvement: [Added](https://github.com/tobymao/sqlglot/commit/67961fbf9776e349044dcbdc6029b1731ee78761) aliases to column lineage nodes. 

- Improvement: [Fixed](https://github.com/tobymao/sqlglot/commit/83700737b558756bca9810a2068f57987f2340c0#diff-c5dbbef7e9cbb2c64b384079070c3006398036d834774db5983a1e347b7747f8) transpilation of DATE_FORMAT for MySQL -> Snowflake.

- Improvement: [Fixed](https://github.com/tobymao/sqlglot/commit/02ab64ad927dc51c908a618ea3605d8688bbdc3a) math simplification (doesn't do int division anymore).

v11.5.0
------

- Breaking/Improvement: replace_placeholders now uses exp.convert instead of exp.to_identifier.

- New: Added support for the HLL aggregate function (Snowflake, Redshift).

- New: Added support for MATCH ... AGAINST ... expression (MySQL).

- New: Added TPC-DS in the test suite.

- New: Added support for BigQuery's SELECT AS [STRUCT|VALUE] expression.

- Improvement: Allow parsing `[]` for [nested types](https://github.com/tobymao/sqlglot/commit/709421aab7ef427619eb5feb95988a0f36792b84).

- Improvement: [Normalize](https://github.com/tobymao/sqlglot/commit/58fdbf05f5e7627936ebf2036410c60396d6da72) keyword texts in the tokenizer.

- Improvement: Parse DECODE function into CASE expression when it has >2 args.

- Improvement: [Don't expand aliases in inner scopes](https://github.com/tobymao/sqlglot/commit/9a574b0e79e7532fd49929cf86396d4485091118).

- Improvement: [Allow HAVING to alias selects](https://github.com/tobymao/sqlglot/commit/2b3eb31ca2ecb81a70bb462ff3987f0749a32d0e).

- Improvement: Added support for the limit argument in Spark's SPLIT function.

- Improvement: [Fix](https://github.com/tobymao/sqlglot/commit/68aea4ab95586723ed0c3d3ab0281577cc71e40c) related to expanding forward references in selects.

- Improvement: Generate ->> for JSON_EXTRACT_SCALAR (MySQL).

- Improvement: [Fix](https://github.com/tobymao/sqlglot/commit/68aea4ab95586723ed0c3d3ab0281577cc71e40c) related to expanding alias  references in the WHERE clause.

- Improvement: Fixed transpilation of some char types to Bigquery.

- Improvement: [Map MAX to GREATEST conditionally for some dialects](https://github.com/tobymao/sqlglot/commit/27483e9f3b7f3043ba1b325d58aa864db54255f1).

- Improvement: Make normalize default to CNF.

- Improvement: Fixed star expansion when USING clause is present.

- Improvement: Various performance improvements in the optimizer.

- Improvement: Various simplifications in the executor (e.g. scan, join).

v11.4.0
------

- Breaking/Improvement: Canonicalization now only quotes when safe.

v11.3.0
------

Changes:

- Breaking/Improvement: Fixed transpilation of the division ("/") operator, because some dialects do integer division while others do float division. For example, "a / b" will now be transpiled to "CAST(a AS DOUBLE PRECISION) / b" for Snowflake -> Postgres, and to "CAST(a / b AS INT)" for Postgres -> Snowflake.

- New: Added support for the COMMENT ON statement.

- New: Added support for old (+) JOIN syntax (Oracle).

- New: Added "root" helper method on Expression.

- New: Added "meta" property on Expression.

- Improvement: Refactored parsing of ALTER TABLE so it's easily extensible.

- Improvement: Merging subquery to out of scope join bug fixed.

- Improvement: Tokenize Jinja comments as comments.

- Improvement: Refactor POST_CREATE and POST_EXPRESSION properties.

- Improvement: Fix line numbers for strings that contain multi-line comments.

- Improvement: Fix aliasing for pivot/unpivot expressions.

- Improvement: Enhance AST diff (https://github.com/tobymao/sqlglot/commit/2093626e798fc12124fdf1f25ea63662f2953966).

- Improvement: Better python typing.

- Improvement: Added support for is_star helper on exp.Select.

v11.2.0
------

Changes:

- Breaking: Replaced "schema" arg from Column expression with "db" and "catalog".

- Breaking: Removed mapping of "{{" to BLOCK_START token.

- New: Added new optimizer pass: [unknown star expansion](https://github.com/tobymao/sqlglot/commit/0e24be8487b94e8ae09ba57b6756788f631043ec).

- New: Added support for INSERT OR ... syntax.

- New: Added support for ILIKE/LIKE ANY.

- Improvement: Made the optimizer more robust.

- Improvement: Improved parsing of GROUP BY to better handle ROLLUP, CUBE and GROUPING SETS.

- Improvement: [Refactored](https://github.com/tobymao/sqlglot/commit/d95317e1fe15f06768d613d1eee1ed88d0d1b95a) UDTF scope.

- Improvement: Fixed parsing for UDF typical parameters in order to handle constraints.

v11.1.0
------

Changes:

- Breaking: Although this is a breaking change, it fixes and makes the behavior of escaped characters more consistent. Now SQLGlot preserves all line break inputs as they come.

v11.0.0
------

Changes:

- Breaking: Renamed ESCAPES to STRING_ESCAPES in the Tokenizer class.

- New: Deployed pdoc documentation page.

- New: Add support for read locking using the FOR UPDATE/SHARE syntax (e.g. MySQL).

- New: Added support for CASCADE, SET NULL and SET DEFAULT constraints.

- New: Added "cast" expression helper.

- New: Add support for transpiling Postgres GENERATE_SERIES into Presto SEQUENCE.

- Improvement: Fix tokenizing of identifier escapes.

- Improvement: Fix eliminate_subqueries [bug](https://github.com/tobymao/sqlglot/commit/b5df65e3fb5ee1ebc3cbab64b6d89598cf47a10b) related to unions.

- Improvement: IFNULL is now transpiled to COALESCE by default for every dialect.

- Improvement: Refactored the way properties are handled. Now it's easier to add them and specify their position in a SQL expression.

- Improvement: Fixed alias quoting bug.

- Improvement: Fixed CUBE / ROLLUP / GROUPING SETS parsing and generation.

- Improvement: Fixed get_or_raise Dialect/t.Type[Dialect] argument bug.

- Improvement: Improved python type hints.

v10.6.0
------

Changes:

- Breaking: Change Power to binary expression.

- Breaking: Removed mapping of "}}" to BLOCK_END token.

- New: x GLOB y support.

v10.5.0
------

Changes:

- Breaking: Added python type hints in the parser module, which may result in some mypy errors.

- New: SQLGlot expressions can [now be serialized / deserialized into JSON](https://github.com/tobymao/sqlglot/commit/bac38151a8d72687247922e6898696be43ff4992).

- New: Added support for T-SQL [hints](https://github.com/tobymao/sqlglot/commit/3220ec1adb1e1130b109677d03c9be947b03f9ca) and [EOMONTH](https://github.com/tobymao/sqlglot/commit/1ac05d9265667c883b9f6db5d825a6d864c95c73).

- New: Added support for Clickhouse's parametric function syntax.

- New: Added [wider support](https://github.com/tobymao/sqlglot/commit/beb660f943b73c730f1b06fce4986e26642ee8dc) for timestr and datestr.

- New: CLI now accepts a flag [for parsing SQL from the standard input stream](https://github.com/tobymao/sqlglot/commit/f89b38ebf3e24ba951ee8b249d73bbf48685928a).

- Improvement: Fixed BigQuery transpilation for [parameterized types and unnest](https://github.com/tobymao/sqlglot/pull/924).

- Improvement: Hive / Spark identifiers can now begin with a digit.

- Improvement: Bug fixes in [date/datetime simplification](https://github.com/tobymao/sqlglot/commit/b26b8d88af14f72d90c0019ec332d268a23b078f).

- Improvement: Bug fixes in [merge_subquery](https://github.com/tobymao/sqlglot/commit/e30e21b6c572d0931bfb5873cc6ac3949c6ef5aa).

- Improvement: Schema identifiers are now [converted to lowercase](https://github.com/tobymao/sqlglot/commit/8212032968a519c199b461eba1a2618e89bf0326) unless they're quoted.

- Improvement: Identifiers with a leading underscore are now regarded as [safe](https://github.com/tobymao/sqlglot/commit/de3b0804bb7606673d0bbb989997c13744957f7c#diff-7857fedd1d1451b1b9a5b8efaa1cc292c02e7ee4f0d04d7e2f9d5bfb9565802c) and hence are not quoted.

v10.4.0
------

Changes:

- Breaking: Removed the quote_identities optimizer rule.

- New: ARRAYAGG, SUM, ARRAYANY support in the engine. SQLGlot is now able to execute all TPC-H queries.

- Improvement: Transpile DATEDIFF to postgres.

- Improvement: Right join pushdown fixes.

- Improvement: Have Snowflake generate VALUES columns without quotes.

- Improvement: Support NaN values in convert.

- Improvement: Recursive CTE scope [fixes](https://github.com/tobymao/sqlglot/commit/bec36391d85152fa478222403d06beffa8d6ddfb).


v10.3.0
------

Changes:

- Breaking: Json ops changed to binary expressions.

- New: Jinja tokenization.

- Improvement: More robust type inference.

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
