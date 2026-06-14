# Dialect Extension Cookbook

> How to extend SQLGlot with a new warehouse dialect — a step-by-step guide
> using the Redshift-to-Snowflake transpilation path as a running example.

---

## Table of Contents

- [Introduction](#introduction)
- [Architecture Overview](#architecture-overview)
- [Transpilation Pipeline Diagram](#transpilation-pipeline-diagram)
- [Step 0: Register Your Dialect](#step-0-register-your-dialect)
- [Step 1: The Dialect Class](#step-1-the-dialect-class)
- [Step 2: Extend the Tokenizer](#step-2-extend-the-tokenizer)
- [Step 3: Extend the Parser](#step-3-extend-the-parser)
- [Step 4: Extend the Generator](#step-4-extend-the-generator)
- [Step 5: End-to-End Test](#step-5-end-to-end-test)
- [Plugin Dialects (External Packages)](#plugin-dialects-external-packages)
- [Checklist](#checklist)
- [Reference: Key Source Files](#reference-key-source-files)

---

## Introduction

SQLGlot supports 34+ SQL dialects out of the box, but production data
pipelines often encounter proprietary extensions — stored-procedure keywords,
custom functions, vendor-specific data types — that no built-in dialect covers.

This cookbook shows you how to **extend an existing dialect** (or create a new
one from scratch) by walking through the three extension points that every
dialect owns:

| Layer | Source file | Responsibility |
|-------|-------------|----------------|
| **Tokenizer** | `sqlglot/tokens.py` | Break raw SQL into tokens |
| **Parser** | `sqlglot/parser.py` | Convert tokens into an AST |
| **Generator** | `sqlglot/generator.py` | Convert an AST back into SQL |

We use **Redshift → Snowflake** as the running example because:

1. Redshift inherits from Postgres (`sqlglot/dialects/redshift.py` → `class Redshift(Postgres)`), giving you a realistic inheritance chain.
2. Snowflake is a direct `Dialect` subclass (`sqlglot/dialects/snowflake.py` → `class Snowflake(Dialect)`), showing the "from scratch" path.
3. Both dialects are already in the codebase, so every code snippet below references **real module paths and function names** you can grep for.

By the end of this guide you will be able to answer:

- "My source dialect has a keyword the tokenizer doesn't recognize — how do I add it?"
- "My target dialect generates the wrong SQL for a standard AST node — how do I override it?"
- "How do I register a brand-new dialect so `sqlglot.transpile(sql, read='mydb', write='mydb')` works?"

---

## Architecture Overview

Every call to `sqlglot.transpile(sql, read=..., write=...)` runs three phases:

```
 ┌──────────┐      ┌──────────┐      ┌───────────┐
 │ Tokenizer│      │  Parser  │      │ Generator │
 │ (read)   │ ──>  │  (read)  │ ──>  │  (write)  │
 │          │      │          │      │           │
 │ SQL str  │      │ Token[]  │      │  AST      │
 │  ───>    │      │  ───>    │      │  ───>     │
 │ Token[]  │      │   AST    │      │  SQL str  │
 └──────────┘      └──────────┘      └───────────┘
```

The **read** dialect controls the Tokenizer and Parser; the **write** dialect
controls the Generator. A dialect class wires all three together.

The base dialect (`sqlglot.dialects.dialect.Dialect`) aims to be a **superset**
of common SQL syntax so that individual dialects only need to override what
they *differ* on. This is why `Redshift` can inherit from `Postgres` and
override only the parts that are Redshift-specific.

> **Key source file**: `sqlglot/dialects/dialect.py` contains the `Dialect`
> base class, the `_Dialect` metaclass (which handles auto-registration), and
> the `NormalizationStrategy` enum.

---

## Transpilation Pipeline Diagram

The sequence below traces a single `transpile()` call from Redshift to Snowflake.
Each box is a method call; arrows show data flow.

```
User code
  │
  │  sqlglot.transpile(sql, read="redshift", write="snowflake")
  ▼
┌─────────────────────────────────────────────────────────────┐
│ Dialect.get_or_raise("redshift")                            │
│   └─> _Dialect._try_load("redshift")                       │
│         └─> importlib.import_module("sqlglot.dialects.redshift")
│   └─> _Dialect._classes["redshift"]  →  Redshift class      │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 1 — Tokenizer  (Redshift.Tokenizer)                   │
│                                                             │
│   Redshift.Tokenizer  extends  Postgres.Tokenizer           │
│     KEYWORDS = { ...Postgres.Tokenizer.KEYWORDS,            │
│                  "(+)":   TokenType.JOIN_MARKER,            │
│                  "MINUS": TokenType.EXCEPT,                 │
│                  "SUPER": TokenType.SUPER, ... }            │
│                                                             │
│   tokenize(sql) ──> list[Token]                             │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 2 — Parser  (RedshiftParser)                          │
│                                                             │
│   RedshiftParser  extends  PostgresParser                   │
│     FUNCTIONS = { ...PostgresParser.FUNCTIONS,              │
│                   "DATEADD":   _build_date_delta(TsOrDsAdd),│
│                   "DATEDIFF":  _build_date_delta(TsOrDsDiff),│
│                   "GETDATE":   CurrentTimestamp.from_arg_list│
│                   ... }                                     │
│                                                             │
│   _parse_statement()                                        │
│     └─> _parse_select() / _parse_create() / ...             │
│           └─> _parse_expression()                           │
│                 └─> _parse_conjunction()                    │
│                       └─> ... (recursive descent)           │
│                                                             │
│   list[Token] ──> exp.Expression (AST)                      │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 3 — Generator  (SnowflakeGenerator)                   │
│                                                             │
│   SnowflakeGenerator  extends  generator.Generator          │
│     TRANSFORMS = { ...Generator.TRANSFORMS,                 │
│       exp.DateAdd:    date_delta_sql("DATEADD"),            │
│       exp.If:         if_sql(name="IFF", false_value="NULL"),│
│       exp.Select:     transforms.preprocess([...]),         │
│       ...                                                   │
│     }                                                       │
│                                                             │
│   generate(ast)                                             │
│     └─> sql(node)                                           │
│           └─> _build_dispatch(cls)                          │
│                 ├─> TRANSFORMS lookup  (priority)           │
│                 └─> <expr>_sql() auto-discovery (fallback)  │
│                                                             │
│   exp.Expression ──> SQL string (Snowflake dialect)         │
└─────────────────────────────────────────────────────────────┘
```

---

## Step 0: Register Your Dialect

Before writing any logic, your dialect must be **discoverable** by SQLGlot's
lazy-loading machinery. Two files need updating:

### 0a. Add to the `Dialects` enum

In `sqlglot/dialects/dialect.py`, locate the `Dialects` enum (around line 83)
and add your dialect name:

```python
class Dialects(str, Enum):
    ...
    REDSHIFT = "redshift"
    MYNEWDB  = "mynewdb"      # <— add here
    SNOWFLAKE = "snowflake"
```

### 0b. Add to the `DIALECTS` list

In `sqlglot/dialects/__init__.py`, add the class name to the `DIALECTS` list
(alphabetical order):

```python
DIALECTS = [
    ...
    "MySQL",
    "MyNewDB",           # <— add here (class name, not module name)
    "Oracle",
    ...
]
```

### How auto-discovery works

The `_Dialect` metaclass (`sqlglot/dialects/dialect.py`, line 137) intercepts
every `class` definition that subclasses `Dialect`. Its `__new__` method:

1. **Registers** the class into `_Dialect._classes[key]` using the lowercase
   class name (or the `Dialects` enum value if one exists).
2. **Builds time-format tries** (`TIME_TRIE`, `FORMAT_TRIE`, etc.) from the
   dialect's `TIME_MAPPING`.
3. **Wires up** the inner `Tokenizer`, `Parser`, and `Generator` classes —
   creating empty subclasses if they are not explicitly defined.

The lazy loader (`_Dialect._try_load`, line 162) uses a three-tier strategy:
1. Standard module import: `importlib.import_module(f"sqlglot.dialects.{key}")`.
2. Entry-point lookup for plugin dialects (`entry_points(group="sqlglot.dialects")`).
3. Direct import fallback.

> **For built-in dialects**, steps 0a and 0b are sufficient.
> For **plugin dialects**, see [Plugin Dialects](#plugin-dialects-external-packages).

---

## Step 1: The Dialect Class

Create `sqlglot/dialects/mynewdb.py`. The dialect class is the **entry point**
that ties Tokenizer, Parser, and Generator together.

### Minimal skeleton

```python
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.generators.mynewdb import MyNewDBGenerator
from sqlglot.parsers.mynewdb import MyNewDBParser


class MyNewDB(Dialect):
    # How unquoted identifiers are normalized
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    # Array index base offset (0-based or 1-based)
    INDEX_OFFSET = 0

    # Time format for TO_TIMESTAMP / TO_DATE
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    TIME_MAPPING = {
        "YYYY": "%Y",
        "MM": "%m",
        "DD": "%d",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
    }

    # Wire up the three components
    class Tokenizer(tokens.Tokenizer):
        ...  # see Step 2

    Parser = MyNewDBParser        # see Step 3
    Generator = MyNewDBGenerator   # see Step 4
```

### Choosing a parent dialect

The single most impactful decision is which existing dialect to inherit from.
Inheritance means you **only override what differs**.

| If your new dialect is… | Inherit from… | Example in codebase |
|-------------------------|--------------|---------------------|
| A fork of an existing DB | That DB's dialect | `Redshift(Postgres)` |
| Close to standard SQL | `Dialect` directly | `Snowflake(Dialect)` |
| A Presto/Trino variant | `Presto` | `Databricks(Spark)` → `Spark(Hive)` → `Hive(Presto)` |

### Key configuration flags on `Dialect`

These flags live on the `Dialect` class and control cross-cutting behavior.
Override them as needed:

| Flag | Type | Meaning |
|------|------|---------|
| `NORMALIZATION_STRATEGY` | `NormalizationStrategy` | Identifier case handling |
| `INDEX_OFFSET` | `int` | Array base index (0 or 1) |
| `NULL_ORDERING` | `str` | `"nulls_are_small"`, `"nulls_are_large"`, `"nulls_are_last"` |
| `DPIPE_IS_STRING_CONCAT` | `bool` | Whether `\|\|` concatenates strings |
| `SAFE_DIVISION` | `bool` | Division by zero returns NULL |
| `SUPPORTS_USER_DEFINED_TYPES` | `bool` | Whether UDTs are allowed |
| `CONCAT_COALESCE` | `bool` | NULL arg in CONCAT → empty string |
| `TRY_CAST_REQUIRES_STRING` | `bool` | TRY_CAST LHS must be string type |

**Real example** — Redshift's flags (`sqlglot/dialects/redshift.py`):

```python
class Redshift(Postgres):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    SUPPORTS_USER_DEFINED_TYPES = False
    INDEX_OFFSET = 0
    HEX_LOWERCASE = True
    ARRAY_FUNCS_PROPAGATES_NULLS = True
```

**Real example** — Snowflake's flags (`sqlglot/dialects/snowflake.py`):

```python
class Snowflake(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    NULL_ORDERING = "nulls_are_large"
    TRY_CAST_REQUIRES_STRING = True
    PREFER_CTE_ALIAS_COLUMN = True
    SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = True
```

---

## Step 2: Extend the Tokenizer

The tokenizer converts a SQL string into a list of `Token` objects. Most
dialect customization happens through two dictionaries:

| Dictionary | Purpose | Example |
|------------|---------|---------|
| `KEYWORDS` | Multi-character lexemes → `TokenType` | `"MINUS": TokenType.EXCEPT` |
| `SINGLE_TOKENS` | Single-character symbols → `TokenType` | `"#": TokenType.HASH` |

### How it works internally

The base `_TokenizerBase` (in `sqlglot/tokens.py`) uses `__init_subclass__`
(line 93) to automatically compute derived data structures whenever a
`Tokenizer` subclass is defined:

- `_KEYWORD_TRIE` — a trie built from all multi-word keywords for fast lookup
- `_QUOTES`, `_IDENTIFIERS` — normalized string/identifier delimiters
- `_FORMAT_STRINGS` — bit/hex/byte/raw string prefixes
- `_COMMENTS` — comment delimiters

This means you only need to set class variables; the metaclass handles the rest.

### Adding keywords

```python
class Tokenizer(ParentTokenizer):
    KEYWORDS = {
        **ParentTokenizer.KEYWORDS,
        # New keywords specific to your dialect
        "MY_FUNC":   TokenType.MY_FUNC,
        "WAREHOUSE": TokenType.WAREHOUSE,
    }
```

**Real example** — Redshift adds vendor-specific keywords
(`sqlglot/dialects/redshift.py`):

```python
class Tokenizer(Postgres.Tokenizer):
    KEYWORDS = {
        **Postgres.Tokenizer.KEYWORDS,
        "(+)":            TokenType.JOIN_MARKER,   # Oracle-style outer join
        "BINARY VARYING": TokenType.VARBINARY,
        "MINUS":          TokenType.EXCEPT,         # MINUS = EXCEPT in Redshift
        "SUPER":          TokenType.SUPER,          # Semi-structured type
        "TOP":            TokenType.TOP,
        "UNLOAD":         TokenType.COMMAND,
    }
```

### Removing inherited keywords

Sometimes a parent dialect defines a keyword that your dialect doesn't use:

```python
KEYWORDS = {
    **ParentTokenizer.KEYWORDS,
    ...
}
KEYWORDS.pop("VALUES")   # Redshift treats VALUES differently
```

### Adjusting single-token behavior

```python
SINGLE_TOKENS = ParentTokenizer.SINGLE_TOKENS.copy()
SINGLE_TOKENS.pop("#")   # Redshift allows # as a table identifier prefix
```

### Other tokenizer class variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `QUOTES` | `["'"]` | String literal delimiters |
| `IDENTIFIERS` | `['"']` | Quoted-identifier delimiters |
| `STRING_ESCAPES` | `["'"]` | Characters that escape within strings |
| `COMMENTS` | `["--", ("/*", "*/")]` | Comment delimiters |
| `RAW_STRINGS` | `[]` | Raw string prefixes (e.g., Snowflake's `$$`) |
| `HEX_STRINGS` | `[]` | Hex literal prefixes |
| `VAR_SINGLE_TOKENS` | `set()` | Tokens allowed inside VAR names (e.g., `$` in Snowflake) |

**Real example** — Snowflake tokenizer (`sqlglot/dialects/snowflake.py`):

```python
class Tokenizer(tokens.Tokenizer):
    STRING_ESCAPES = ["\\", "'"]
    HEX_STRINGS = [("x'", "'"), ("X'", "'")]
    RAW_STRINGS = ["$$"]
    COMMENTS = ["--", "//", ("/*", "*/")]
    VAR_SINGLE_TOKENS = {"$"}
    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "FILE://":       TokenType.URI_START,
        "FILE FORMAT":   TokenType.FILE_FORMAT,
        "WAREHOUSE":     TokenType.WAREHOUSE,
        "FLOAT":         TokenType.DOUBLE,
        ...
    }
```

---

## Step 3: Extend the Parser

The parser converts a token list into an AST using **recursive descent**.
The base `Parser` class (`sqlglot/parser.py`) provides the parsing framework;
dialect-specific parsers override dictionaries and `_parse_*` methods.

### Parser inheritance in the codebase

```
parser.Parser                              (base, in sqlglot/parser.py)
    └── BaseParser                         (sqlglot/parsers/base.py)
        └── PostgresParser                 (sqlglot/parsers/postgres.py)
            └── RedshiftParser             (sqlglot/parsers/redshift.py)
```

`BaseParser` (`sqlglot/parsers/base.py`) is a thin layer that adds common
no-paren functions like `LOCALTIME`, `LOCALTIMESTAMP`, `CURRENT_CATALOG`:

```python
class BaseParser(parser.Parser):
    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.LOCALTIME:        exp.Localtime,
        TokenType.LOCALTIMESTAMP:   exp.Localtimestamp,
        TokenType.CURRENT_CATALOG:  exp.CurrentCatalog,
        TokenType.SESSION_USER:     exp.SessionUser,
    }
```

### The five key dictionaries

| Dictionary | Maps | To | Purpose |
|------------|------|----|---------|
| `FUNCTIONS` | Function name `str` | `Callable → exp.Expr` | Standard function calls: `MY_FUNC(a, b)` |
| `FUNCTION_PARSERS` | Function name `str` | `Callable(self) → exp.Expr` | Non-standard arg syntax: `CAST(x AS INT)` |
| `NO_PAREN_FUNCTIONS` | `TokenType` | `exp.Expr` subclass | No-arg functions: `CURRENT_DATE` |
| `NO_PAREN_FUNCTION_PARSERS` | Keyword `str` | `Callable(self) → exp.Expr` | Special no-paren constructs: `CASE ... END` |
| `STATEMENT_PARSERS` | `TokenType` | `Callable(self) → exp.Expr` | Top-level statements: `CREATE`, `INSERT` |

### Overriding `FUNCTIONS` — the most common extension

When your dialect has a function whose name or argument semantics differ from
the base, add an entry to `FUNCTIONS`:

```python
class MyParser(ParentParser):
    FUNCTIONS = {
        **ParentParser.FUNCTIONS,
        # Map function name → builder callable
        "NVL":        lambda args: exp.Coalesce(this=seq_get(args, 0), expressions=[seq_get(args, 1)]),
        "DATEADD":    _build_date_delta(exp.TsOrDsAdd),
        "GETDATE":    exp.CurrentTimestamp.from_arg_list,
        "LISTAGG":    exp.GroupConcat.from_arg_list,
    }
```

**Real example** — Redshift parser (`sqlglot/parsers/redshift.py`):

```python
class RedshiftParser(PostgresParser):
    FUNCTIONS = {
        **{k: v for k, v in PostgresParser.FUNCTIONS.items() if k != "GET_BIT"},
        "ADD_MONTHS":  lambda args: exp.TsOrDsAdd(...),
        "DATEADD":     _build_date_delta(exp.TsOrDsAdd),
        "DATEDIFF":    _build_date_delta(exp.TsOrDsDiff),
        "GETDATE":     exp.CurrentTimestamp.from_arg_list,
        "LISTAGG":     exp.GroupConcat.from_arg_list,
        "REGEXP_SUBSTR": lambda args: exp.RegexpExtract(...),
    }
```

Notice the pattern `{k: v for k, v in Parent.FUNCTIONS.items() if k != "..."}`
— this is how you **remove** a parent function mapping that conflicts with your
dialect.

### Builder helpers from `dialect.py`

The `sqlglot.dialects.dialect` module provides factory functions for common
parser patterns:

| Factory | Returns | Use case |
|---------|---------|----------|
| `build_formatted_time(exp_class)` | `Callable` | Functions with time-format args |
| `build_date_delta(exp_class)` | `Callable` | `DATEADD`/`DATEDIFF`-style functions |
| `_build_bitwise(exp_class, name)` | `Callable` | Bitwise functions (`BITAND`, `BITOR`) |

### Overriding `_parse_*` methods

When a function has non-standard argument syntax (not just a name mapping),
override the parsing method directly:

```python
class MyParser(ParentParser):
    FUNCTION_PARSERS = {
        **ParentParser.FUNCTION_PARSERS,
        "SPECIAL_FUNC": lambda self: self._parse_special_func(),
    }

    def _parse_special_func(self):
        # Custom parsing logic using token-matching methods
        this = self._parse_bitwise()
        self._match(TokenType.COMMA)
        fmt = self._parse_string()
        return self.expression(exp.SpecialFunc, this=this, format=fmt)
```

### Token-matching methods (recursive descent toolkit)

These methods are the building blocks of any custom `_parse_*` method:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `_match(token_type)` | `(TokenType, advance=True) → bool` | Match one token by type |
| `_match_set(types)` | `(set[TokenType]) → bool` | Match any token in a set |
| `_match_texts(texts)` | `(set[str]) → bool` | Match a keyword string |
| `_match_text_seq(*texts)` | `(*str) → bool` | Match a sequence of keywords |
| `_advance(n=1)` | — | Move cursor forward |
| `_retreat(index)` | — | Move cursor back |
| `_parse_csv(method)` | `(Callable) → list` | Parse comma-separated values |
| `_parse_wrapped(method)` | `(Callable) → result` | Parse `(...)` wrapped construct |

### Token sets that control identifier resolution

The parser uses several token sets to decide which tokens can serve as
identifiers, aliases, etc. Extend these when your dialect reserves fewer (or
more) keywords:

```python
class MyParser(ParentParser):
    ID_VAR_TOKENS = ParentParser.ID_VAR_TOKENS | {TokenType.MY_KEYWORD}
    TABLE_ALIAS_TOKENS = ParentParser.TABLE_ALIAS_TOKENS - {TokenType.RESERVED}
```

---

## Step 4: Extend the Generator

The generator converts an AST back into a SQL string for the target dialect.
It has two customization mechanisms that work together:

1. **`TRANSFORMS` dictionary** — maps `exp.Expr` subclass → `Callable(self, e) → str`
2. **Auto-discovered `*_sql` methods** — methods named `<lowercase_expr>_sql`

### Generator feature flags

These boolean flags on the Generator class control structural SQL generation:

| Flag | Default | Effect |
|------|---------|--------|
| `LIMIT_IS_TOP` | `False` | Generate `TOP n` instead of `LIMIT n` |
| `JOIN_HINTS` | `True` | Allow join hints (`/*+ HASH_JOIN */`) |
| `TABLE_HINTS` | `True` | Allow table hints (`WITH (NOLOCK)`) |
| `QUERY_HINTS` | `True` | Allow query-level hints |
| `VALUES_AS_TABLE` | `True` | Whether `VALUES` can appear as a table |
| `AGGREGATE_FILTER_SUPPORTED` | `True` | Support `FILTER (WHERE ...)` on aggregates |
| `SINGLE_STRING_INTERVAL` | `False` | Interval as a single string literal |

**Real example** — Snowflake generator flags (`sqlglot/generators/snowflake.py`):

```python
class SnowflakeGenerator(generator.Generator):
    JOIN_HINTS = False
    TABLE_HINTS = False
    QUERY_HINTS = False
    AGGREGATE_FILTER_SUPPORTED = False
    SINGLE_STRING_INTERVAL = True
    STAR_EXCEPT = "EXCLUDE"
```

---

### Dispatch priority

When the generator encounters an AST node of type `exp.Foo`:

1. **Check `TRANSFORMS`**: If `exp.Foo` has an entry, use it.
2. **Check auto-discovery**: If a method named `foo_sql` exists on the class, use it.
3. **Fallback**: Use `function_fallback_sql` (for `Func` subclasses) or raise.

The dispatch table is built by `_build_dispatch()` (`sqlglot/generator.py`,
line 77):

```python
def _build_dispatch(cls):
    dispatch = dict(cls.TRANSFORMS)          # TRANSFORMS take priority
    for attr_name in dir(cls):
        if not attr_name.endswith("_sql") or attr_name.startswith("_"):
            continue
        expr_key = attr_name[:-4]
        expr_cls = exp.EXPR_CLASSES.get(expr_key)
        if expr_cls and expr_cls not in dispatch:
            dispatch[expr_cls] = getattr(cls, attr_name)
    return dispatch
```

### Pattern A: Simple function rename via `TRANSFORMS`

Use `rename_func()` for one-to-one function renames:

```python
from sqlglot.dialects.dialect import rename_func

class MyGenerator(ParentGenerator):
    TRANSFORMS = {
        **ParentGenerator.TRANSFORMS,
        exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
    }
```

### Pattern B: Argument reshaping via `TRANSFORMS`

Use helper factories when the target function has different argument order:

```python
from sqlglot.dialects.dialect import date_delta_sql, if_sql

TRANSFORMS = {
    exp.DateAdd:  date_delta_sql("DATEADD"),
    exp.DateDiff: date_delta_sql("DATEDIFF"),
    exp.If:       if_sql(name="IFF", false_value="NULL"),
}
```

### Pattern C: AST preprocessing via `transforms.preprocess()`

For structural transformations (not just renaming), chain AST rewriters:

```python
from sqlglot import transforms

TRANSFORMS = {
    exp.Select: transforms.preprocess([
        transforms.eliminate_window_clause,      # Inline window → subquery
        transforms.eliminate_distinct_on,        # DISTINCT ON → ROW_NUMBER
        transforms.eliminate_semi_and_anti_joins, # SEMI/ANTI → EXISTS
        transforms.explode_projection_to_unnest(),
    ]),
}
```

**Real example** — Snowflake (`sqlglot/generators/snowflake.py`):

```python
TRANSFORMS = {
    **generator.Generator.TRANSFORMS,
    exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
    exp.ArrayConcat:    array_concat_sql("ARRAY_CAT"),
    exp.AtTimeZone:     lambda self, e: self.func("CONVERT_TIMEZONE",
                            e.args.get("zone"), e.this),
    exp.DateAdd:        date_delta_sql("DATEADD"),
    exp.If:             if_sql(name="IFF", false_value="NULL"),
    exp.Map:            lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
    exp.Select:         transforms.preprocess([
        transforms.eliminate_window_clause,
        transforms.eliminate_distinct_on,
        transforms.explode_projection_to_unnest(),
        _transform_generate_date_array,
        _qualify_unnested_columns,
        _eliminate_dot_variant_lookup,
    ]),
}
```

### Pattern D: Auto-discovered `*_sql` methods

For complex generation logic that needs multiple lines, define a method named
after the expression class (lowercased, with `_sql` suffix):

```python
class MyGenerator(ParentGenerator):
    # No TRANSFORMS entry needed — auto-discovered by name
    def myfunction_sql(self, expression: exp.MyFunction) -> str:
        arg = expression.this
        return self.func("MY_FUNC", arg, expression.args.get("format"))
```

> **Important**: `TRANSFORMS` entries take precedence over `*_sql` methods.
> Only use `TRANSFORMS` for one-liners or lambdas. For multi-line logic, use
> an auto-discovered method.

### Pattern E: Overriding `TYPE_MAPPING`

Map AST data types to target-dialect type names:

```python
TYPE_MAPPING = {
    **ParentGenerator.TYPE_MAPPING,
    exp.DType.BINARY:    "VARBYTE",
    exp.DType.INT:       "INTEGER",
    exp.DType.TIMESTAMPTZ: "TIMESTAMP",
}
```

### Generator helper methods

Use these instead of building SQL strings manually:

| Method | Purpose | Example |
|--------|---------|---------|
| `self.func(name, *args)` | Render a function call | `self.func("DATEADD", unit, n, date)` |
| `self.binary(e, op)` | Render a binary operation | `self.binary(e, "||")` |
| `self.expressions(e, key)` | Render a list of child expressions | Used in SELECT lists |
| `self.sql(e, key)` | Recursively generate SQL for a child | `self.sql(expression.this)` |
| `self.seg(sql, sep)` | Add segment separator (newline) | Pretty-printing |

### The `@unsupported_args` decorator

When a target dialect doesn't support certain arguments, use this decorator
to emit a warning instead of silently dropping them:

```python
from sqlglot.generator import unsupported_args

@unsupported_args("accuracy")
def approx_count_distinct_sql(self, expression):
    return self.func("APPROX_COUNT_DISTINCT", expression.this)
```

---

## Step 5: End-to-End Test

Add tests to `tests/dialects/test_<dialect>.py` to verify transpilation works
correctly in both directions.

### Round-trip test (`validate_identity`)

Verify that SQL survives a parse → generate cycle in the same dialect:

```python
class TestMyDialect(unittest.TestCase):
    def test_my_feature(self):
        self.validate_identity("SELECT MY_FUNC(a, b) FROM t")
```

### Cross-dialect test (`validate_all`)

Verify that Redshift SQL transpiles correctly to Snowflake:

```python
def test_redshift_to_snowflake(self):
    self.validate_all(
        "SELECT DATEADD(day, 7, created_at) FROM events",  # Redshift SQL
        read={
            "redshift": "SELECT DATEADD(day, 7, created_at) FROM events",
        },
        write={
            "snowflake": "SELECT DATEADD(day, 7, created_at) FROM events",
        },
    )
```

### Type-annotated tests

When transpilation depends on `is_type()` checks, use `annotate_types`:

```python
from sqlglot.optimizer import annotate_types

expr = self.validate_identity("SELECT BASE64_ENCODE(col)")
annotated = annotate_types(expr, dialect="mydialect")
self.assertEqual(annotated.sql("snowflake"), "SELECT ...")
```

### Running tests

```bash
# Run specific dialect test file
python -m unittest tests.dialects.test_mydialect

# Run specific test method
python -m unittest tests.dialects.test_mydialect.TestMyDialect.test_my_feature

# Run all tests
make test
```

---

## Plugin Dialects (External Packages)

If your dialect lives in a separate Python package, you don't need to modify
SQLGlot's source. Instead, register it via Python **entry points**.

In your package's `pyproject.toml`:

```toml
[project.entry-points."sqlglot.dialects"]
mynewdb = "my_package.mynewdb:MyNewDB"
```

The `_Dialect._try_load()` method (line 162 in `sqlglot/dialects/dialect.py`)
searches entry points under the group `"sqlglot.dialects"` as its second
resolution tier:

```python
# 2. Try entry points (for plugins)
all_eps = entry_points()
eps = all_eps.select(group="sqlglot.dialects", name=key)
for entry_point in eps:
    dialect_class = entry_point.load()
    if isinstance(dialect_class, type) and issubclass(dialect_class, Dialect):
        cls._classes[key] = dialect_class
```

Once installed, `sqlglot.transpile(sql, read="mynewdb")` works automatically.

---

## Checklist

Use this checklist when adding a new dialect or extending an existing one:

- [ ] **Registration**
  - [ ] Added enum value to `Dialects` in `sqlglot/dialects/dialect.py`
  - [ ] Added class name to `DIALECTS` list in `sqlglot/dialects/__init__.py`
  - [ ] (Plugin only) Configured `pyproject.toml` entry point

- [ ] **Dialect class** (`sqlglot/dialects/<name>.py`)
  - [ ] Chosen correct parent dialect
  - [ ] Set `NORMALIZATION_STRATEGY`
  - [ ] Set `TIME_MAPPING` and `TIME_FORMAT`
  - [ ] Set other relevant flags (`INDEX_OFFSET`, `NULL_ORDERING`, etc.)
  - [ ] Wired up `Parser` and `Generator` references

- [ ] **Tokenizer** (inner class or `sqlglot/tokens.py` override)
  - [ ] Added dialect-specific `KEYWORDS`
  - [ ] Removed conflicting inherited keywords (via `.pop()`)
  - [ ] Set `STRING_ESCAPES`, `COMMENTS`, etc. if different from parent

- [ ] **Parser** (`sqlglot/parsers/<name>.py`)
  - [ ] Added function mappings to `FUNCTIONS`
  - [ ] Added custom `FUNCTION_PARSERS` for non-standard syntax
  - [ ] Extended `ID_VAR_TOKENS` / `TABLE_ALIAS_TOKENS` if needed
  - [ ] Overridden `_parse_*` methods for dialect-specific constructs

- [ ] **Generator** (`sqlglot/generators/<name>.py`)
  - [ ] Added `TRANSFORMS` entries for simple renames / argument reshaping
  - [ ] Added auto-discovered `*_sql` methods for complex logic
  - [ ] Overridden `TYPE_MAPPING` for data type differences
  - [ ] Set feature flags (`JOIN_HINTS`, `LIMIT_IS_TOP`, etc.)
  - [ ] Added `RESERVED_KEYWORDS` set if needed

- [ ] **Testing** (`tests/dialects/test_<name>.py`)
  - [ ] Added `validate_identity()` round-trip tests
  - [ ] Added `validate_all()` cross-dialect tests
  - [ ] Added type-annotated tests where `is_type()` is used
  - [ ] All tests pass: `make test`
  - [ ] Linter passes: `make style`

---

## Reference: Key Source Files

| File | Role |
|------|------|
| `sqlglot/dialects/dialect.py` | `Dialect` base class, `_Dialect` metaclass, `Dialects` enum, helper factories |
| `sqlglot/dialects/__init__.py` | `DIALECTS` list, lazy import via `__getattr__` |
| `sqlglot/tokens.py` | `Tokenizer`, `TokenType` enum, `_TokenizerBase` with `__init_subclass__` |
| `sqlglot/parser.py` | `Parser` base class, `_parse_*` methods, token-matching methods |
| `sqlglot/parsers/base.py` | `BaseParser` — thin layer adding common no-paren functions |
| `sqlglot/generator.py` | `Generator` base class, `TRANSFORMS`, `_build_dispatch`, `*_sql` auto-discovery |
| `sqlglot/expressions.py` | All AST node types (`exp.Expr` subclasses) |
| `sqlglot/dialects/redshift.py` | Redshift dialect — example of inheriting from Postgres |
| `sqlglot/dialects/snowflake.py` | Snowflake dialect — example of inheriting from Dialect directly |
| `posts/onboarding.md` | Architecture deep-dive (recommended prerequisite) |
