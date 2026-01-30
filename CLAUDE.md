# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About SQLGlot

SQLGlot is a no-dependency SQL parser, transpiler, optimizer, and engine written in pure Python. It supports 31+ SQL dialects and can transpile between them while preserving semantics. The codebase is performance-critical despite being pure Python, with an optional Rust tokenizer for speed improvements.

## Development Commands

### Installation
```bash
# Basic installation
make install

# Development installation (includes dev dependencies + Rust tokenizer in debug mode)
make install-dev

# Install pre-commit hooks
make install-pre-commit

# With uv (faster):
UV=1 make install-dev
```

### Testing
```bash
# Run all tests (Python tokenizer)
make test

# Run all tests (Rust tokenizer)
make test-rs

# Run only unit tests (skip integration tests, Python tokenizer)
make unit

# Run only unit tests (Rust tokenizer)
make unit-rs

# Run specific test file
python -m unittest tests.test_expressions

# Run specific test class
python -m unittest tests.test_expressions.TestExpressions

# Run specific test method
python -m unittest tests.test_expressions.TestExpressions.test_alias
```

### Linting & Type Checking
```bash
# Run linter and formatter only
make style

# Run full checks (style + all tests with both tokenizers)
make check
```

### Benchmarks
```bash
# Run parsing benchmark
make bench

# Run optimization benchmark
make bench-optimize
```

## Architecture Overview

SQLGlot follows a classic compiler architecture with three main phases:

### 1. Tokenizer (`tokens.py`)
- Converts SQL strings into a sequence of tokens (lexical analysis)
- Two implementations: Python (`tokens.py`) and Rust (`sqlglotrs/tokenizer.rs`)
- **IMPORTANT**: Changes to tokenization logic MUST be reflected in BOTH implementations
- Maps lexemes to `TokenType` enum values via `KEYWORDS` and `SINGLE_TOKENS` dictionaries
- Dialects can override tokenizer behavior by customizing these mappings

### 2. Parser (`parser.py`)
- Converts tokens into an Abstract Syntax Tree (AST)
- Uses recursive descent parsing approach
- Parsing methods follow `_parse_*` naming convention (e.g., `_parse_create()`, `_parse_select()`)
- Token matching methods: `_match()`, `_match_set()`, `_match_text_seq()`, `_match_texts()`
- Helper methods for common patterns: `_parse_csv()`, `_parse_wrapped()`, `_parse_wrapped_csv()`
- Maintains index/cursor with `_advance()` and `_retreat()` methods
- Falls back to `exp.Command` for unparseable SQL (preserves original text)

### 3. Generator (`generator.py`)
- Converts AST back to SQL strings
- Traverses AST recursively, generating SQL for each expression node
- Two ways to customize generation:
  - `TRANSFORMS` dictionary for single-line generations
  - `<expr_name>_sql()` methods for complex generations
- Helper methods: `expressions()`, `func()`, `rename_func()`
- Use `sep()` and `seg()` for proper whitespace/newline handling in pretty-printed output

### 4. Expressions (`expressions.py`)
- Defines all AST node types as Python classes inheriting from `Expression`
- Each expression represents a semantic SQL concept (e.g., `Select`, `Join`, `Column`)
- Expressions can be traversed using `.find()`, `.find_all()`, `.walk()`, `.transform()`
- Building SQL programmatically: use helper functions like `select()`, `from_()`, `where()`, etc.

### 5. Dialects (`dialects/`)
- 34 dialect implementations in `dialects/<dialect>.py`
- Each dialect subclasses base `Dialect` and can override Tokenizer, Parser, and Generator
- Base "sqlglot" dialect acts as a superset to minimize duplication
- Dialect customization via:
  - Feature flags (e.g., `SUPPORTS_IMPLICIT_UNNEST`)
  - Token sets (e.g., `RESERVED_TOKENS`)
  - `token -> Callable` mappings (e.g., `FUNCTIONS`, `STATEMENTS`)
  - `Expression -> str` mappings in Generator

### 6. Optimizer (`optimizer/`)
- Canonicalizes and optimizes queries while preserving semantics
- Applies sequential optimization rules (order matters!)
- Key rules:
  - `qualify`: Normalizes identifiers and qualifies all tables/columns (most important rule)
  - `annotate_types`: Infers data types throughout the AST
  - `pushdown_predicates`, `pushdown_projections`: Optimization rewrites
  - `simplify`: Simplifies boolean expressions and arithmetic
- Rules depend on schema information for best results
- Optimizer performs logical optimization only (not physical/performance)

### 7. Schema (`schema.py`)
- Represents database structure (tables, columns, types)
- Used by optimizer and lineage analysis
- `MappingSchema` takes nested dict: `{"table": {"col": "type"}}`

### 8. Lineage (`lineage.py`)
- Traces column-level lineage through queries
- Requires target query, upstream queries, and root table schemas
- Builds linked list of `Node` objects representing data flow
- Can visualize with `node.to_html()`

## Key Concepts

### The "sqlglot" Dialect
- Base dialect that accommodates common syntax across all dialects
- All other dialects extend this base
- When adding multi-dialect features, prefer adding to base dialect to avoid duplication
- Only add dialect-specific features to individual dialect classes

### AST-First Approach
- SQLGlot preserves _semantics_ not syntax
- Parse SQL → AST (semantic representation) → Generate SQL in target dialect
- This enables accurate cross-dialect transpilation
- Comments are preserved on best-effort basis
- See `posts/ast_primer.md` for detailed AST tutorial

### Testing Philosophy
- Comprehensive test suite in `tests/` directory
- Dialect-specific tests in `tests/dialects/`
- Tests are critical - "robust test suite" is a core feature
- Use `tests/fixtures/` for test data
- `tests/helpers.py` contains test utilities

### Parser/Generator Symmetry
- Parser: `token -> Callable` mappings (builds AST from tokens)
- Generator: `Expression -> str` mappings (builds SQL from AST)
- Customization follows similar patterns in both

### Type Annotations
- Type inference is crucial for some transpilations (e.g., `+` can mean addition or concatenation)
- Optimizer's `annotate_types` rule propagates type information through AST
- Requires schema information to work effectively

## Common Usage Patterns

### Reading SQL
```python
import sqlglot
expression = sqlglot.parse_one("SELECT * FROM table", dialect="spark")
```

### Validate Function Expression
```python
import sqlglot
tree = sqlglot.parse_one("SELECT NULLIF(1, 2)", dialect="snowflake")
if "Anonymous" in repr(tree):
    print("Function expression exists")
else:
    print("Function expression does not exist")
```

### Writing SQL
```python
expression.sql(dialect="duckdb", pretty=True)
```

### Building SQL Programmatically
```python
from sqlglot import select, condition
select("*").from_("y").where(condition("x=1").and_("y=1")).sql()
```

### Traversing AST
```python
from sqlglot import parse_one, exp
tree = parse_one("SELECT a, b + 1 AS c FROM d", dialect="dialect")
for column in tree.find_all(exp.Column):
    print(column.alias_or_name)
```

### Transforming AST
```python
def transformer(node):
    if isinstance(node, exp.Column) and node.name == "a":
        return parse_one("FUN(a)", dialect="dialect")
    return node

transformed = tree.transform(transformer)
```

## Development Guidelines

- Follow [Conventional Commits](https://www.conventionalcommits.org/) for PR titles
- Keep PRs minimal in scope - one well-defined change per PR
- Add tests for non-trivial changes
- Update docstrings if APIs change
- Run `make check` before submitting
- Use comments for complex logic only

## Important Files

- `posts/ast_primer.md`: Detailed AST tutorial
- `posts/onboarding.md`: Architecture deep-dive (HIGHLY RECOMMENDED)
- `.pre-commit-config.yaml`: Pre-commit hooks (ruff, ruff-format, mypy)
- `pyproject.toml`: Project metadata and build config
- `Makefile`: All development commands

## Performance Considerations

- Pure Python implementation with optional Rust tokenizer (`sqlglotrs/`)
- Install with `pip install "sqlglot[rs]"` for Rust tokenizer speed boost
- Performance is a key feature despite Python implementation
- Benchmarks compare against other SQL parsers - see `benchmarks/`
- Avoid use of typing.Protocol, prefer Union Type and Duck Typing

---

## SQLGlot Coding Rules

The following patterns are based on PR review feedback. Follow these to minimize review iterations.

### 1. Use Automatic Naming Convention for Generator Methods

**Don't do this (module-level function with TRANSFORMS):**
```python
def _my_func_sql(self: MyDialect.Generator, expression: exp.MyFunc) -> str:
    ...

class Generator:
    TRANSFORMS = {
        exp.MyFunc: _my_func_sql,
    }
```

**Don't do this (method with TRANSFORMS):**
```python
class Generator:
    TRANSFORMS = {
        exp.MyFunc: lambda self, e: self._my_func_sql(e),
    }

    def _my_func_sql(self, expression):
        ...
```

**Do this (auto-discovered method):**
```python
class Generator:
    # No TRANSFORMS entry needed - automatic discovery by name

    def myfunc_sql(self, expression: exp.MyFunc) -> str:
        ...
```

Generator methods named `<lowercase_expr_name>_sql` are automatically discovered. 

Important: Only use TRANSFORMS for simple one-liners like `rename_func("OTHER_NAME")` or lambdas or functions with multiple entry points. For any single entry point function, always use an auto-discovered method inside the Generator class. 

SQLGlot automatically applies transformations based on the structure of the name, but when this fails, you must rename the function.  This is only when the SQL name is not covered by auto mapping:

**Do this:**
```python
class Generator:
    TRANSFORMS = {
        exp.ArrayLength: rename_func("LENGTH"),
    }
```

**Don't do this:**
```python
exp.ArrayLength: lambda self, e: self.func("LENGTH", e.this),
```

### 2. Use Existing Expression Classes, Not Anonymous

**Don't do this:**
```python
from_base64 = exp.Anonymous(this="FROM_BASE64", expressions=[input_expr])
```

**Do this:**
```python
from_base64 = exp.FromBase64(this=input_expr)
```

Always check if an expression class exists in `expressions.py` before using `exp.Anonymous`. Anonymous should only be used for functions that don't have a dedicated class. Search for the function name in expressions.py first.

### 3. SQL Generation: Choose the Right Approach

Use the appropriate method based on complexity. From simplest to most complex:

#### Level 1: Generator Helper Methods
For generating function calls in generator methods, use `self.func()`:
```python
def myfunc_sql(self, expression):
    # Don't: return self.sql(exp.Func(this="MY_FUNC", expressions=[expression.this]))
    # Do:
    return self.func("MY_FUNC", expression.this)
```

#### Level 2: Expression Builders
For building expressions, use helper functions instead of direct class construction:

| Helper | Instead of | Benefits |
|--------|-----------|----------|
| `exp.func("name", *args)` | `exp.Anonymous(...)` | Finds proper Func class |
| `exp.array(e1, e2, ...)` | `exp.Array(expressions=[...])` | Parses automatically |
| `exp.and_(e1, e2, ...)` | `exp.And(this=..., expression=...)` | Handles nesting |
| `exp.or_(e1, e2, ...)` | `exp.Or(this=..., expression=...)` | Handles nesting |
| `exp.case().when(cond, val).else_(default)` | `exp.Case(ifs=[...])` | Fluent interface |
| `exp.cast(expr, "TYPE")` | `exp.Cast(this=..., to=...)` | Builds DataType |
| `exp.column("col", "table")` | `exp.Column(...)` | Handles identifiers |
| `exp.null()` | `exp.Null()` | Simple factory |

Also use expression operators for cleaner code:
```python
# Arithmetic: exp.column("x") + 1  instead of  exp.Add(this=..., expression=...)
# Indexing:   arr[index]           instead of  exp.Bracket(this=arr, expressions=[index])
# Comparison: arg.is_(exp.Null())  instead of  exp.Is(this=arg, expression=exp.Null())
```

#### Level 3: SQL Templates
When expressions become complex, use templates with `exp.maybe_parse()` and `exp.replace_placeholders()`:

```python
# Define template with :placeholder syntax
MY_TEMPLATE: exp.Expression = exp.maybe_parse(
    "CASE WHEN :arg IS NULL THEN NULL ELSE :result END"
)

# In generator method
def myfunc_sql(self, expression):
    result = exp.replace_placeholders(
        self.MY_TEMPLATE.copy(),
        arg=expression.this,
        result=some_expression,
    )
    return self.sql(result)
```

#### Avoid: F-strings with SQL Fragments
You should rarely, if ever, build SQL with f-strings - it breaks quoting, escaping, and dialect handling:
```python
# NEVER do this:
def my_func_sql(self, expression):
    return f"CAST({self.sql(expression.this)} AS TIME)"

# Do this instead:
def my_func_sql(self, expression):
    return self.sql(exp.cast(expression.this, "TIME"))
```

### 4. Type Checking: `is_string` vs `is_type()`

These serve **different purposes**:

**`is_type()`** - Semantic type check:
```python
# Returns True if expression's type is text (columns, function results, etc.)
# Requires annotate_types() to populate type info
if arg.is_type(*exp.DataType.TEXT_TYPES):
    ...
```

**`is_string`** - Syntactic check for string literals:
```python
# Returns True only for literal strings like 'hello'
# Works without type annotation
if arg.is_string:
    value = arg.name  # Extract the string value
```


**When to use each:**

| Use Case | Method |
|----------|--------|
| Check if node is a string literal to extract its value | `is_string` |
| Check if node is a literal vs column/expression | `is_string` |
| Check semantic type (works for columns, functions) | `is_type()` |
| Cover both literals and typed expressions | `is_string or is_type()` |

**Combined pattern (from `length_sql`):**
```python
# Fast check for string literals (no annotation needed)
if arg.is_string:
    return self.func("LENGTH", arg)

# For non-literals, get type info if needed
if not arg.type:
    arg = annotate_types(arg, dialect=self.dialect)

# Then check semantic type
if arg.is_type(*exp.DataType.TEXT_TYPES):
    return self.func("LENGTH", arg)
```

**Don't do direct type comparisons:**
```python
# Bad
if input_expr.type and input_expr.type.this in exp.DataType.TEXT_TYPES:

# Good
if input_expr.is_type(*exp.DataType.TEXT_TYPES):
```

### 5. Use `to_py()` for Literal Value Extraction

**Don't do this:**
```python
if isinstance(arg, exp.Literal):
    value = int(arg.this.strip("'"))
```

**Do this:**
```python
if isinstance(arg, exp.Literal) and arg.is_number:
    value = int(arg.to_py())
```

### 6. Avoid Compile-Time NULL Checks

Don't check for `exp.Null()` or literal NULL values in Python during transpilation. NULL handling should happen at query time in the generated SQL using `IS NULL` checks.

**Don't do this:**
```python
def myfunc_sql(self, expression):
    # Bad: checking for literal NULL at transpile time
    if any(isinstance(arg, exp.Null) for arg in expression.expressions):
        return self.sql(exp.Null())
```

**Do this:**
```python
# Good: generate SQL that handles NULL at query time
TEMPLATE = exp.maybe_parse("CASE WHEN :arg IS NULL THEN NULL ELSE ... END")
```

Compile-time checks only handle literal `NULL` values in the SQL text, not NULL values that come from columns, parameters, or expressions at runtime. Generate SQL with `IS NULL` checks to handle all cases.

### 7. Type Annotations in Tests

When transpilation depends on `is_type()` checks, tests need `annotate_types()`:

```python
from sqlglot.optimizer import annotate_types

# Without annotation - is_type() returns False for literals
expr = self.validate_identity("SELECT BASE64_ENCODE('Hello World')")

# With annotation - types are inferred, is_type() works
annotated = annotate_types(expr, dialect="snowflake")
self.assertEqual(annotated.sql("duckdb"), "SELECT TO_BASE64(ENCODE('Hello World'))")
```

### 8. Use `find_ancestor` with Scope Boundaries

When searching for ancestors, include scope boundaries to avoid crossing into parent queries:

```python
# Stop at Select to stay within current query scope
ancestor = expression.find_ancestor(exp.Where, exp.Having, exp.Select)
if ancestor and not isinstance(ancestor, exp.Select):
    # Found restricted context within current scope
    ...
```

### 9. Use @unsupported_args for unsupported arguments

When arguments are not supported do this:

```python
        @unsupported_args("ins_cost", "del_cost", "sub_cost")
        def levenshtein_sql(self, expression: exp.Levenshtein) -> str:
```

### 10. Keep Code Minimal

- Remove unused imports, variables, and dead code
- Don't add comments for obvious code
- Don't add docstrings unless the function is complex or public API
- Prefer inline expressions over intermediate variables when readable
- Don't add backwards-compatibility shims for removed code

### 11. Test Patterns

- Add tests to the appropriate dialect test file (e.g., `tests/dialects/test_snowflake.py`)
- Use `self.validate_all()` for cross-dialect tests
- Use `self.validate_identity()` for round-trip tests
- Don't add tests for functionality that already has coverage

### 12. Ensure Test Validity

- Make sure all tests added to tests/dialects/*.py actually run in the relevant databases, such as snowflake or duckdb
