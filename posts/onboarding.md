# Table of Contents
- [Table of Contents](#table-of-contents)
  - [Onboarding](#onboarding)
  - [Tokenizer](#tokenizer)
  - [Parser](#parser)
    - [Token Index](#token-index)
    - [Matching text](#matching-text)
    - [Utility methods](#utility-methods)
    - [Command fallback](#command-fallback)
    - [Dialect-specific parsing](#dialect-specific-parsing)
  - [Generator](#generator)
    - [Generating queries](#generating-queries)
    - [Utility methods](#utility-methods-1)
    - [Pretty print](#pretty-print)
  - [Schema](#schema)
  - [Optimizer](#optimizer)
    - [Optimization rules](#optimization-rules)
      - [Qualify](#qualify)
        - [Identifier normalization](#identifier-normalization)
        - [Qualifying tables \& columns](#qualifying-tables--columns)
      - [Type annotation](#type-annotation)
  - [Dialects](#dialects)
    - [Implementing a custom dialect](#implementing-a-custom-dialect)
  - [Column Level Lineage](#column-level-lineage)
    - [Implementation details](#implementation-details)

## Onboarding
This document aims to familiarize the reader with SQLGlot's codebase & architecture.

Generally, some background knowledge about programming languages / compilers is required. A good starting point is [Crafting Interpreters](https://craftinginterpreters.com/) by Robert Nystrom, which served as the foundation when SQLGlot was initially created.

At a high level, SQLGlot transpilation involves three modules:

- [Tokenizer](#tokenizer): Converts raw code into a sequence of “tokens”, one for each word/symbol
- [Parser](#parser): Converts sequence of tokens into an abstract syntax tree representing the semantics of the raw code
- [Generator](#generator): Converts an abstract syntax tree into SQL code

SQLGlot can transpile SQL between different _SQL dialects_, which are usually associated with different database systems.

Each dialect has unique syntax that is implemented by overriding the base versions of the three modules. A dialect definition may override any or all of the three base modules.

The base versions of the modules implement the `sqlglot` dialect (sometimes referred to as "base dialect"), which is designed to accommodate as many common syntax elements as possible across the other supported dialects, thus preventing code duplication.

SQLGlot includes other modules which are not required for basic transpilation, such as the [Optimizer](#optimizer) and Executor. The Optimizer modifies an abstract syntax tree for other uses (e.g., inferring column-level lineage). The Executor runs SQL code in Python, but its engine is still in an experimental stage, so some functionality may be missing.

The rest of this document describes the three base modules, dialect-specific overrides, and the Optimizer module.

## Tokenizer
The tokenizer module (`tokens.py`) is responsible for breaking down SQL code into tokens (like keywords, identifiers, etc.), which are the smallest units of meaningful information. This process is also known as lexing/lexical analysis.

Python is not designed for maximum processing speed/performance, so SQLGlot maintains an equivalent [Rust version of the tokenizer](https://tobikodata.com/sqlglot-jumps-on-the-rust-bandwagon.html) in `sqlglotrs/tokenizer.rs` for improved performance.

> [!IMPORTANT]
> Changes in the tokenization logic must be reflected in both the Python and the Rust tokenizer. The goal is for the two implementations to be similar so that there is less cognitive effort to port code between them.

This example demonstrates the results of using the Tokenizer to tokenize the SQL query `SELECT b FROM table WHERE c = 1`:

```python
from sqlglot import tokenize

tokens = tokenize("SELECT b FROM table WHERE c = 1")
for token in tokens:
    print(token)

# <Token token_type: <TokenType.SELECT: 'SELECT'>, text: 'SELECT', line: 1, col: 6, start: 0, end: 5, comments: []>
# <Token token_type: <TokenType.VAR: 'VAR'>, text: 'b', line: 1, col: 8, start: 7, end: 7, comments: []>
# <Token token_type: <TokenType.FROM: 'FROM'>, text: 'FROM', line: 1, col: 13, start: 9, end: 12, comments: []>
# <Token token_type: <TokenType.TABLE: 'TABLE'>, text: 'table', line: 1, col: 19, start: 14, end: 18, comments: []>
# <Token token_type: <TokenType.WHERE: 'WHERE'>, text: 'WHERE', line: 1, col: 25, start: 20, end: 24, comments: []>
# <Token token_type: <TokenType.VAR: 'VAR'>, text: 'c', line: 1, col: 27, start: 26, end: 26, comments: []>
# <Token token_type: <TokenType.EQ: 'EQ'>, text: '=', line: 1, col: 29, start: 28, end: 28, comments: []>
# <Token token_type: <TokenType.NUMBER: 'NUMBER'>, text: '1', line: 1, col: 31, start: 30, end: 30, comments: []>
```

The Tokenizer scans the query and converts groups of symbols (or _lexemes_), such as words (e.g., `b`) and operators (e.g., `=`), into tokens. For example, the `VAR` token is used to represent the identifier `b`, whereas the `EQ` token is used to represent the "equals" operator.

Each token contains information such as its type (`token_type`), the lexeme (`text`) it encapsulates and other metadata such as the lexeme's line (`line`) and column (`col`), which are used to accurately report errors.

SQLGlot’s `TokenType` enum provides an indirection layer between lexemes and their types. For example, `!=` and `<>` are often used interchangeably to represent "not equals", so SQLGlot groups them together by mapping them both to `TokenType.NEQ`.

The `Tokenizer.KEYWORDS` and `Tokenizer.SINGLE_TOKENS` are two important dictionaries that map lexemes to the corresponding `TokenType` enum values.

## Parser
The parser module takes the list of tokens generated by the tokenizer and builds an [abstract syntax tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) from them.

Generally, an AST stores the _meaningful_ constituent components of a SQL statement. For example, to represent a `SELECT` query we can only store its projections, filters, aggregation expressions and so on, but the `SELECT`, `WHERE` and `GROUP BY` keywords don't need to be preserved, since they are implied.

In SQLGlot's case, the AST usually stores information that captures the _semantics_ of an expression, i.e. its meaning, which is what allows it to transpile SQL code between different dialects. A blog post that explains this idea at a high-level can [be found here](https://tobikodata.com/transpiling_sql1.html).

The parser processes the tokens corresponding to a SQL statement sequentially and produces an AST to represent it. For example, if a statement’s first token is `CREATE`, the parser will determine what is being created by examining the next token (which could be `SCHEMA`, `TABLE`, `VIEW`, or something else).

Each semantic concept is represented by an _expression_ in the AST. Converting tokens to expressions is the primary task of the parser.

As the AST is a core concept of SQLGlot, a more detailed tutorial on its representation, traversal, and transformation can [be found here](https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md).

The next sections describe how the parser processes a SQL statement’s list of tokens, followed by an explanation of how dialect-specific parsing works.

### Token Index
The parser processes a token list sequentially, and maintains an index/cursor that points to the next token to be consumed.

Once the current token has been successfully processed, the parser moves to the next token in the sequence by calling `_advance()` to increment the index.

If a token has been consumed but could not be parsed, the parser can move backward to the previous token by calling `_retreat()` to decrement the index.

In some scenarios, the parser’s behavior depends on both the current and surrounding tokens. The parser can “peek” at the previous and next tokens without consuming them by accessing the `_prev` and `_next` properties, respectively.

### Matching tokens and text
When parsing a SQL statement, the parser must identify specific sets of keywords to correctly build the AST. It does so by “matching” sets of tokens or keywords to infer the structure of the statement.

For example, these matches occur when parsing the window specification `SUM(x) OVER (PARTITION BY y)`:
1. The `SUM` and `(` tokens are matched and `x` is parsed as an identifier
2. The `)` token is matched
2. The `OVER`  and `(` tokens are matched
2. The `PARTITION` and `BY` tokens are matched
3. The partition clause `y` is parsed
4. The `)` token is matched

The family of `_match` methods perform different varieties of matching. They return `True` and advance the index if there is a match, or return `None` if the tokens do not match.

The most commonly used `_match` methods are:

Method                | Use
-------------------------|-----
**`_match(type)`**       | Attempt to match a single token with a specific `TokenType`
**`_match_set(types)`**  | Attempt to match a single token in a set of `TokenType`s
**`_match_text_seq(texts)`**   | Attempt to match a continuous sequence of strings/texts
**`_match_texts(texts)`**   | Attempt to match a keyword in a set of strings/texts

### `_parse` methods
SQLGlot’s parser follows the “recursive descent” approach, meaning that it relies on mutually recursive methods in order to understand SQL syntax and the various precedence levels between combined SQL expressions.

For example, the SQL statement `CREATE TABLE a (col1 INT)` will be parsed into an `exp.Create` expression that includes the kind of object being created (table) and its schema (table name, column names and types). The schema will be parsed into an `exp.Schema` node that contains one column definition `exp.ColumnDef` node for each column.

The relationships between SQL constructs are encoded in methods whose names begin with “_parse_”, such as `_parse_create()`. We refer to these as “parsing methods”.

For instance, to parse the statement above the entrypoint method `_parse_create()` is called. Then:
- In `_parse_create()`, the parser determines that a `TABLE` is being created
- Because a table is being created, the table name is expected next and thus `_parse_table_parts()` is invoked; This is because the table name may have multiple parts such as `catalog.schema.table`
- Having processed the table name, the parser continues by parsing the schema definition using `_parse_schema()`

A key aspect of the SQLGlot parser is that the parsing methods are composable and can be passed as arguments. We describe how that works in the next section.

### Utility methods
SQLGlot provides helper methods for common parsing tasks. They should be used whenever possible to reduce code duplication and manual token/text matching.

Many helper methods take a parsing method argument, which is invoked to parse part(s) of the clause they're supposed to parse.

For example, these helper methods parse collections of SQL objects like `col1, col2`, `(PARTITION BY y)`, and `(colA TEXT, colB INT)`, respectively:

Method                | Use
-------------------------|-----
**`_parse_csv(parse_method)`**  | Attempt to parse comma-separated values using the appropriate `parse_method` callable
**`_parse_wrapped(parse_method)`**  | Attempt to parse a specific construct that is (optionally) wrapped in parentheses
**`_parse_wrapped_csv(parse_method)`**  | Attempt to parse comma-separated values that are (optionally) wrapped in parentheses


### Command fallback
The SQL language specification and dialect variations are vast, and SQLGlot cannot parse every possible statement.

Instead of erroring on statements it cannot parse, SQLGlot falls back to storing the code that cannot be parsed in an `exp.Command` expression.

This allows SQLGlot to return the code unmodified even though it cannot parse it. Because the code is unmodified, any dialect-specific components will not be transpiled.

### Dialect-specific parsing
The base parser’s goal is to represent as many common constructs from different SQL dialects as possible. This makes the parser more lenient and less-repetitive/concise.

Dialect-specific parser behavior is implemented in two ways: feature flags and parser overrides.

If two different parsing behaviors are common across dialects, the base parser may implement both and use feature flags to determine which should be used for a specific dialect. In contrast, parser overrides directly replace specific base parser methods.

Therefore, each dialect’s Parser class may specify:

- Feature flags such as `SUPPORTS_IMPLICIT_UNNEST` or `SUPPORTS_PARTITION_SELECTION`, which are used to control the behavior of base parser methods.

- Sets of tokens that play a similar role, such as `RESERVED_TOKENS` that cannot be used as object names.

- Sets of `token -> Callable` mappings, implemented with Python lambda functions
    - When the parser comes across the token (string or `TokenType`) on the left-hand side it invokes the mapping function on the right to create the corresponding Expression / AST node.
    - The lambda function may return an expression directly or a `_parse_ method` that determines what expression should be returned.
    - The mappings are grouped by general semantic type (e.g., functions, parsers for `ALTER` statements, etc.), with each type having its own dictionary.

An example `token -> Callable` mapping is the `FUNCTIONS` dictionary that builds the appropriate `exp.Func` node based on the string key:

```Python3
 FUNCTIONS: t.Dict[str, t.Callable] = {
    "LOG2": lambda args: exp.Log(this=exp.Literal.number(2), expression=seq_get(args, 0)),
    "LOG10": lambda args: exp.Log(this=exp.Literal.number(10), expression=seq_get(args, 0)),
    "MOD": build_mod,
     …,
 }
```

In this dialect, the `LOG2()` function calculates a logarithm of base 2, which is represented in SQLGlot by the general `exp.Log` expression.

The logarithm base and the user-specified value to `log` are stored in the node's arguments. The former is stored as an integer literal in the `this` argument, and the latter is stored in the `expression` argument.

## Generator
After the parser has created an AST, the generator module is responsible for converting it back into SQL code.

Each dialect’s Generator class may specify:

- A set of flags such as `ALTER_TABLE_INCLUDE_COLUMN_KEYWORD`. As with the parser, these flags control the behavior of base Generator methods.

- Set of `Expression -> str` mappings
    - The generator traverses the AST recursively and produces the equivalent string for each expression it visits. This can be thought of as the reverse operation of the parser, where expressions / AST nodes are converted back to strings.
    - The mappings are grouped by general semantic type (e.g., data type expressions), with each type having its own dictionary.

The `Expression -> str` mappings can be implemented as either:
- Entries in the `TRANSFORMS` dictionary, which are best suited for single-line generations.
- Functions with names of the form `<expr_name>_sql(...)`. For example, to generate SQL for an `exp.SessionParameter` node we can override the base generator method by defining the method `def sessionparameter_sql(...)` in a dialect’s Generator class.

### Generating queries
The key method in the Generator module is the `sql()` function, which is responsible for generating the string representation of each expression.

First, it locates the mapping of expression to string or generator `Callable` by examining the keys of the `TRANSFORMS` dictionary and searching the `Generator`'s methods for an `<exprname>_sql()` method.

It then calls the appropriate callable to produce the corresponding SQL code.

> [!IMPORTANT]
> If there is both a `TRANSFORM` entry and an `<expr_name>_sql(...)` method for a given expression, the generator will use the former to convert the expression into a string.

### Utility methods
The Generator defines helper methods containing quality-of-life abstractions over the `sql()` method, such as:

Method                | Use
-------------------------|-----
**`expressions()`**  | Generates the string representation for a list of `Expression`s, employing a series of arguments to help with their formatting
**`func()`, `rename_func()`**  | Given a function name and an `Expression`, returns the string representation of a function call by generating the expression's args as the func args.

These methods should be used whenever possible to reduce code duplication.

### Pretty print
Pretty printing refers to the process of generating formatted and consistently styled SQL, which improves readability, debugging, and code reviewing.

SQLGlot generates an AST’s SQL code on a single line by default, but users may specify that it be `pretty` and include new lines and indenting.

It is up to the developer to ensure that whitespaces and newlines are embedded in the SQL correctly. To aid in that process, the Generator class provides the `sep()` and `seg()` helper methods.

## Dialects
We briefly mentioned dialect-specific behaviors while describing the SQLGlot base Tokenizer, Parser, and Generator modules. This section expands on how to specify a new SQL dialect.

As [described above](#dialect-specific-parsing), SQLGlot attempts to bridge dialect-specific variations in a "superset" dialect, which we refer to as the _sqlglot_ dialect.

All other SQL dialects have their own Dialect subclass whose Tokenizer, Parser and Generator components extend or override the base modules as needed.

The base dialect components are defined in `tokens.py`, `parser.py` and `generator.py`. Dialect definitions can be found under `dialects/<dialect>.py`.

When adding features that will be used by multiple dialects, it's best to place the common/overlapping parts in the base _sqlglot_ dialect to prevent code repetition across other dialects. Any dialect-specific features that cannot (or should not) be repeated can be specified in the dialect’s subclass.

The Dialect class contains flags that are visible to its Tokenizer, Parser and Generator components. Flags are only added in a Dialect when they need to be visible to at least two components, in order to avoid code duplication.

### Implementing a custom dialect
Creating a new SQL dialect may seem complicated at first, but it is actually quite simple in SQLGlot.

This example demonstrates defining a new dialect named `Custom` that extends or overrides components of the base Dialect modules:

```Python
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

      class Parser(Parser):
           # Specifies how certain tokens are mapped to function AST nodes
           FUNCTIONS = {
             **parser.Parser.FUNCTIONS,
             "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
           }

          # Specifies how a specific construct e.g. CONSTRAINT is parsed
          def _parse_constraint(self) -> t.Optional[exp.Expression]:
            return super()._parse_constraint() or self._parse_projection_def()

    class Generator(Generator):
        # Specifies how AST nodes, i.e. subclasses of exp.Expression, should be converted into SQL
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
        }

        # Specifies how AST nodes representing data types should be converted into SQL
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            ...
        }
```


Even though this example is a fairly realistic starting point, we strongly encourage you to study existing dialect implementations to understand how their various components can be modified.

## Schema
Previous sections described the SQLGlot components used for transpilation. This section and the next describe components needed to implement other SQLGlot functionality like column-level lineage.

A Schema represents the structure of a database schema, including the tables/views it contains and their column names and data types.

The file `schema.py` defines the abstract classes `Schema` and `AbstractMappingSchema`; the implementing class is `MappingSchema`.

A `MappingSchema` object is defined with a nested dictionary, as in this example:

```Python
schema = MappingSchema({"t": {"x": "int", "y": "datetime"}})
```

The keys of the top-level dictionary are table names (`t`), the keys of `t`’s dictionary are its column names (`x` and `y`), and the values are the column data types (`int` and `datetime`).

A schema provides information required by other SQLGlot modules (most importantly, the optimizer and column level lineage).

Schema information is used to enrich ASTs with actions like replacing stars (`SELECT *`) with the names of the columns being selected from the upstream tables.

## Optimizer
The optimizer module in SQLGlot (`optimizer.py`) is responsible for producing canonicalized and efficient SQL queries by applying a series of optimization rules.

Optimizations include simplifying expressions, removing redundant operations, and rewriting queries for better performance.

The optimizer operates on the abstract syntax tree (AST) returned by the parser, transforming it into a more compact form while preserving the semantics of the original query.

> [!NOTE]
> The optimizer performs only logical optimization; The underlying engine is almost always better at optimizing for performance.

### Optimization rules
The optimizer essentially applies [a list of rules](https://sqlglot.com/sqlglot/optimizer.html) in a sequential manner, where each rule takes in an AST and produces a canonicalized and optimized version of it.

> [!WARNING]
> The rule order is important, as some rules depend on others to work properly. We discourage manually applying individual rules, which may result in erroneous or unpredictable behavior.

The first, foundational optimization task is to _standardize_ an AST, which is required by other optimization steps. These rules implement canonicalization:

#### Qualify
The most important rule in the optimizer is `qualify`, which is responsible for rewriting the AST such that all tables and columns are _normalized_ and _qualified_.

##### Identifier normalization
The normalization step (`normalize_identifiers.py`) transforms some identifiers by converting them into lower or upper case, ensuring the semantics are preserved in each case (e.g. by respecting case-sensitivity).

This transformation reflects how identifiers would be resolved by the engine corresponding to each SQL dialect, and plays a very important role in the standardization of the AST.

> [!NOTE]
> Some dialects (e.g. DuckDB) treat identifiers as case-insensitive even when they're quoted, so all identifiers are normalized.

Different dialects may have different normalization strategies. For instance, in Snowflake unquoted identifiers are normalized to uppercase.

This example demonstrates how the identifier `Foo` is normalized to lowercase `foo` in the default sqlglot dialect and uppercase `FOO` in the Snowflake dialect:

```Python
import sqlglot
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

normalize_identifiers("Foo").sql()
# 'foo'

normalize_identifiers("Foo", dialect="snowflake").sql("snowflake")
# 'FOO'
```

##### Qualifying tables and columns
After normalizing the identifiers, all table and column names are qualified so there is no ambiguity about the data sources referenced by the query.

This means that all table references are assigned an alias and all column names are prefixed with their source table’s name.

This example demonstrates qualifying the query `SELECT col FROM tbl`, where `tbl` contains a single column `col`. Note that the table’s schema is passed as an argument to `qualify()`:

```Python
import sqlglot
from sqlglot.optimizer.qualify import qualify

schema = {"tbl": {"col": "INT"}}
expression = sqlglot.parse_one("SELECT col FROM tbl")
qualify(expression, schema=schema).sql()

# 'SELECT "tbl"."col" AS "col" FROM "tbl" AS "tbl"'
```

The `qualify` rule also offers a set of sub-rules that further canonicalize the query.

For instance, `qualify()` can expand stars such that each `SELECT *` is replaced by a projection list of the selected columns.

This example modifies the previous example’s query to use `SELECT *`. `qualify()` expands the star and returns the same output as before:

```Python
import sqlglot
from sqlglot.optimizer.qualify import qualify


schema = {"tbl": {"col": "INT"}}
expression = sqlglot.parse_one("SELECT * FROM tbl")
qualify(expression, schema=schema).sql()


# 'SELECT "tbl"."col" AS "col" FROM "tbl" AS "tbl"'
```

#### Type Inference
Some SQL operators’ behavior changes based on the data type of its inputs.

For example, in some SQL dialects `+` can be used for either adding numbers or concatenating strings. The database uses its knowledge of column data types to determine which operation should be executed in a specific situation.

Like the database, SQLGlot needs to know column data types to perform some optimizations. In many cases, both transpilation and optimization need type information to work properly.

Type annotation begins with user-provided information about column data types for at least one table. SQLGlot then traverses the AST, propagating that type information and attempting to infer the type returned by each AST expression.

Users may need to provide column type information for multiple tables to successfully annotate all expressions in the AST.

## Column Level Lineage
Column-level lineage (CLL) traces the flow of data from column to column as tables select and modify columns from one another in a SQL codebase.

CLL provides critical information about data lineage that helps in understanding how data is derived, transformed, and used across different parts of a data pipeline or database system.

SQLGlot’s CLL implementation is defined in `lineage.py`. It operates on a collection of queries that `SELECT` from one another. It assumes that the graph/network of queries forms a directed acyclic graph (DAG) where no two tables `SELECT` from each other.

The user must provide:
A “target” query whose upstream column lineage should be traced
The queries linking all tables upstream of the target query
The schemas (column names and types) of the root tables in the DAG

In this example, we trace the lineage of the column `traced_col`:

```Python
from sqlglot.lineage import lineage

target_query = “””
WITH cte AS (
  SELECT
    traced_col
  FROM
    intermediate_table
)
SELECT
  traced_col
FROM
  cte
“””

node = lineage(
    column="traced_col",
    sql=target_query,
    sources={"intermediate_table": "SELECT * FROM root_table"},
    schema={"root_table": {"traced_col": "int"}},
)
```

The `lineage()` function’s `sql` argument takes the target query, the `sources` argument takes a dictionary of source table names and the query that produces each table, and the `schema` argument takes the column names/types of the graph’s root tables.

### Implementation details
This section describes how SQLGlot traces the lineage in the previous example.

Before lineage can be traced, the following preparatory steps must be carried out:

1. Replace the `sql` query’s table references with their `sources`’ queries, such that we have a single standalone query.

This example demonstrates how the earlier example’s reference to `intermediate_table` inside the CTE is replaced with the query that generated it, `SELECT * FROM root_table`, and given the alias `intermediate_table`:

```SQL
WITH cte AS (
  SELECT
    traced_col
  FROM (
    SELECT
      *
    FROM
      root_table
  ) AS intermediate_table
)
SELECT
  traced_col
FROM
  cte
```

2. Qualify the query produced by the earlier step, expanding `*`s and qualifying all column references with the corresponding source tables:

```SQL
WITH cte AS (
  SELECT
    intermediate_table.traced_col
  FROM (
    SELECT
      root_table.traced_col
    FROM
      root_table AS root_table
  ) AS intermediate_table
)
SELECT
  cte.traced_col
FROM
  cte AS cte
```

After these operations, the query is canonicalized and the lineage can be traced.

Tracing is a top-down process, meaning that the search starts from `cte.traced_col` (outer scope) and traverse inwards to find that its origin is `intermediate_table.traced_col`, which in turn is based on `root_table.traced_col`.

The search continues until the innermost scope has been visited. At that point all `sources` are exhausted, so the `schema` is searched to validate that the root table (`root_table` in our example) exists and that the target column `traced_col` is defined in it.

At each step, the column of interest (i.e., `cte.traced_col`, `intermediate_table.traced_col`. etc) is wrapped in a lineage object `Node` that is linked to its upstream nodes.

This approach incrementally builds a linked list of `Nodes` that traces the lineage for the target column: `root_table.traced_col -> intermediate_table.traced_col -> cte.traced_col`.

The `lineage()` output can be visualized using the `node.to_html()` function, which generates an HTML lineage graph using `vis.js`.

![Lineage](onboarding_images/lineage_img.png)
