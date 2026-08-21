# Qualified Star `EXCEPT` Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Spark and Databricks optimizer star expansion resolve qualified column and nested struct-field exclusions without deleting same-named columns from unrelated sources.

**Architecture:** Replace the shared name-only exclusion set with a fresh per-star exclusion plan resolved against the current `Scope` through `Resolver`. Spark enables resolver-backed exclusions and native nested reconstruction; nested paths expand into semantic `exp.Struct` expressions containing `source.column.* EXCEPT (relative.path)` so output shape, field order, and NULL behavior remain engine-native.

**Tech Stack:** Python 3.9+, SQLGlot expressions and optimizer scopes, `unittest`, Ruff, mypyc-compatible type annotations.

## Global Constraints

- Preserve the parser AST for `Star.except_`; do not add parser or generator rewrites.
- Resolve relation-qualified top-level columns before interpreting the same parts as a struct path.
- Reject ambiguous, unresolved, duplicate, and overlapping exclusions with `OptimizeError`.
- Keep each exclusion plan local to one select expression; modifiers must not leak between repeated stars over the same source.
- Preserve quoted `Identifier` nodes and build nested paths as expression trees, never dotted strings.
- Use `STRUCT(source.column.* EXCEPT (field_path)) AS column` for Spark-family nested reconstruction; do not enumerate fields or generate `DROP_FIELDS`.
- Other dialects retain current behavior until they explicitly enable resolver-backed exclusion.
- No compatibility aliases, deprecated paths, unrelated qualification refactors, or new dependencies.
- Every commit message ends with `[CODEX]`.

---

## File Map

- `sqlglot/dialects/dialect.py`: declares independent resolver-backed and nested-reconstruction capabilities, both disabled by default.
- `sqlglot/dialects/spark.py`: enables both capabilities for Spark; Databricks inherits them through `class Databricks(Spark)`.
- `sqlglot/optimizer/resolver.py`: exposes source-column type lookup needed to validate known struct paths without leaking private traversal details.
- `sqlglot/optimizer/qualify_columns.py`: resolves each star's exclusions, validates collisions, and applies whole-column or nested-field expansion.
- `tests/test_optimizer.py`: contains focused optimizer contract tests, including the complete reported CTE and error behavior.

### Task 1: Resolve qualified whole-column exclusions per source

**Files:**
- Modify: `sqlglot/dialects/dialect.py:607-619`
- Modify: `sqlglot/dialects/spark.py:12-18`
- Modify: `sqlglot/optimizer/qualify_columns.py:970-1177,1226-1235`
- Test: `tests/test_optimizer.py` in `TestOptimizer`

**Interfaces:**
- Produces: `Dialect.STAR_EXCEPT_REQUIRES_RESOLUTION: bool`
- Produces: `Dialect.SUPPORTS_STRUCT_STAR_EXCEPT: bool`
- Produces: `_resolve_except_columns(expression: exp.Expr, tables: list[str], resolver: Resolver) -> dict[str, dict[str, list[exp.Expr]]]`
- Produces: `_build_except_path(parts: list[exp.Expr]) -> exp.Expr`
- Consumes: `Resolver.get_source_columns(name: str, only_visible: bool = False)`

- [ ] **Step 1: Add a failing complete-query regression test**

Add this method to `TestOptimizer` in `tests/test_optimizer.py`:

```python
    def test_qualify_star_except_qualified_column(self):
        sql = """
            WITH r AS (SELECT 1 AS pk, 2 AS x),
                 d AS (SELECT 1 AS pk, 3 AS y),
                 j AS (SELECT * EXCEPT (r.pk) FROM r JOIN d ON r.pk = d.pk)
            SELECT j.pk FROM j
        """

        self.assertEqual(
            qualify(
                parse_one(sql, dialect="databricks"),
                dialect="databricks",
                quote_identifiers=False,
            ).sql(dialect="databricks"),
            "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y), "
            "j AS (SELECT r.x AS x, d.pk AS pk, d.y AS y FROM r AS r JOIN d AS d "
            "ON r.pk = d.pk) SELECT j.pk AS pk FROM j AS j",
        )
```

The outer `SELECT j.pk` is mandatory: a minimized inner query would not prove that the surviving `d.pk` remains addressable.

- [ ] **Step 2: Run the regression test and verify the current failure**

Run:

```bash
python -m unittest tests.test_optimizer.TestOptimizer.test_qualify_star_except_qualified_column
```

Expected: error with `sqlglot.errors.OptimizeError: Unknown column: pk`.

- [ ] **Step 3: Declare the two dialect capabilities**

Add adjacent to `SUPPORTS_STRUCT_STAR_EXPANSION` in `Dialect`:

```python
    STAR_EXCEPT_REQUIRES_RESOLUTION = False
    """Whether star EXCEPT paths must be resolved against individual selected sources."""

    SUPPORTS_STRUCT_STAR_EXCEPT = False
    """Whether nested exclusions can be represented as STRUCT(column.* EXCEPT (field_path))."""
```

Enable both in `Spark`:

```python
    STAR_EXCEPT_REQUIRES_RESOLUTION = True
    SUPPORTS_STRUCT_STAR_EXCEPT = True
```

Do not duplicate them in `Databricks`; inheritance is the contract.

- [ ] **Step 4: Replace global name-only collection with a fresh per-star plan**

Remove the function-wide `except_columns` state from `_expand_stars`. For each star expression, resolve a local plan:

```python
        except_columns: dict[str, dict[str, list[exp.Expr]]] = {}

        if isinstance(expression, exp.Star):
            tables.extend(scope.selected_sources)
            except_columns = _resolve_except_columns(expression, tables, resolver)
            _add_replace_columns(expression, tables, replace_columns)
            _add_rename_columns(expression, tables, rename_columns)
            ilike_pattern = _add_ilike_columns(expression, dialect)
        elif expression.is_star and isinstance(expression, exp.Column):
            tables.append(expression.table)
            except_columns = _resolve_except_columns(expression.this, tables, resolver)
            _add_replace_columns(expression.this, tables, replace_columns)
            _add_rename_columns(expression.this, tables, rename_columns)
            ilike_pattern = _add_ilike_columns(expression.this, dialect)
```

Inside the source loop, replace `id(table)` lookup with the local source name:

```python
            columns_to_exclude = except_columns.get(table, {})
```

For Task 1, whole-column entries use an empty list and the column loop skips them:

```python
            for name in columns:
                if name in columns_to_exclude and not columns_to_exclude[name]:
                    continue
```

Leave nested-list handling for Task 3.

- [ ] **Step 5: Implement relation-qualified and unqualified whole-column resolution**

Replace `_add_except_columns` with `_build_except_path` and `_resolve_except_columns`. Keep both implementations local to `qualify_columns.py`; do not add a new module.

The path builder preserves a uniform `.parts` interface for one or more relative identifiers:

```python
def _build_except_path(parts: list[exp.Expr]) -> exp.Expr:
    identifiers = [identifier.copy() for identifier in parts]
    column = exp.Column(this=identifiers[0])
    return exp.Dot.build([column, *identifiers[1:]]) if len(identifiers) > 1 else column
```

The Task 1 resolver is:

```python
def _resolve_except_columns(
    expression: exp.Expr,
    tables: list[str],
    resolver: Resolver,
) -> dict[str, dict[str, list[exp.Expr]]]:
    except_ = expression.args.get("except_")
    if not except_:
        return {}

    if not resolver.dialect.STAR_EXCEPT_REQUIRES_RESOLUTION:
        columns = {exclusion.name for exclusion in except_}
        return {table: {column: [] for column in columns} for table in tables}

    source_columns = {
        table: resolver.get_source_columns(table, only_visible=True) for table in tables
    }
    resolved: dict[str, dict[str, list[exp.Expr]]] = {}

    for exclusion in except_:
        parts = exclusion.parts
        if not parts:
            raise OptimizeError(f"Unknown column: {exclusion.sql()}")

        source = None
        column = None
        remaining_parts = []

        if len(parts) > 1:
            candidate_source = parts[0].name
            candidate_columns = source_columns.get(candidate_source)
            if candidate_columns is not None and parts[1].name in candidate_columns:
                source = candidate_source
                column = parts[1].name
                remaining_parts = parts[2:]

        if source is None:
            candidates = [
                table for table, columns in source_columns.items() if parts[0].name in columns
            ]
            if not candidates:
                raise OptimizeError(f"Unknown column: {exclusion.sql()}")
            if len(candidates) > 1:
                raise OptimizeError(f"Ambiguous column: {exclusion.sql()}")
            source = candidates[0]
            column = parts[0].name
            remaining_parts = parts[1:]

        path = _build_except_path(remaining_parts) if remaining_parts else None
        resolved.setdefault(source, {})[column] = [] if path is None else [path]

    return resolved
```

Task 1 intentionally allows a later duplicate to overwrite an earlier one; Task 2 replaces that assignment with collision-aware insertion. Resolution is restricted to `tables`, so `r.* EXCEPT (d.pk)` cannot target source `d`. Identifiers in `remaining_parts` are copied as nodes for Task 3 rather than converted to dotted strings.

- [ ] **Step 6: Add the symmetric source control**

Extend the test method with:

```python
        inner_sql = """
            WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y)
            SELECT * EXCEPT (d.pk) FROM r JOIN d ON r.pk = d.pk
        """
        self.assertEqual(
            qualify(
                parse_one(inner_sql, dialect="databricks"),
                dialect="databricks",
                quote_identifiers=False,
            ).sql(dialect="databricks"),
            "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y) "
            "SELECT r.pk AS pk, r.x AS x, d.y AS y FROM r AS r JOIN d AS d ON r.pk = d.pk",
        )
```

- [ ] **Step 7: Run the focused tests**

Run:

```bash
python -m unittest tests.test_optimizer.TestOptimizer.test_qualify_star_except_qualified_column
```

Expected: PASS.

- [ ] **Step 8: Commit Task 1**

```bash
git add sqlglot/dialects/dialect.py sqlglot/dialects/spark.py sqlglot/optimizer/qualify_columns.py tests/test_optimizer.py
git commit -m "fix(spark): resolve qualified star exclusions [CODEX]"
```

### Task 2: Reject ambiguous, unresolved, and duplicate whole-column exclusions

**Files:**
- Modify: `sqlglot/optimizer/qualify_columns.py` in `_resolve_except_columns`
- Test: `tests/test_optimizer.py` in `TestOptimizer`

**Interfaces:**
- Consumes: `_resolve_except_columns(expression, tables, resolver)` from Task 1
- Produces: `_add_resolved_except(resolved, source, column, path, exclusion) -> None`
- Error contract: invalid exclusion resolution raises `OptimizeError`

- [ ] **Step 1: Add failing error-contract tests**

Add:

```python
    def test_qualify_star_except_errors(self):
        prefix = """
            WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y)
        """
        invalid = (
            f"{prefix} SELECT * EXCEPT (pk) FROM r JOIN d ON r.pk = d.pk",
            f"{prefix} SELECT * EXCEPT (z.pk) FROM r JOIN d ON r.pk = d.pk",
            f"{prefix} SELECT * EXCEPT (r.pk, r.pk) FROM r JOIN d ON r.pk = d.pk",
            f"{prefix} SELECT r.* EXCEPT (d.pk) FROM r JOIN d ON r.pk = d.pk",
        )

        for sql in invalid:
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    qualify(parse_one(sql, dialect="databricks"), dialect="databricks")
```

- [ ] **Step 2: Run the tests and verify each current behavior is wrong**

Run:

```bash
python -m unittest tests.test_optimizer.TestOptimizer.test_qualify_star_except_errors
```

Expected: FAIL because at least the ambiguous, unknown-qualifier, and duplicate cases do not raise at exclusion resolution.

- [ ] **Step 3: Add collision-aware plan insertion**

Implement a focused helper:

```python
def _add_resolved_except(
    resolved: dict[str, dict[str, list[exp.Expr]]],
    source: str,
    column: str,
    path: exp.Expr | None,
    exclusion: exp.Expr,
) -> None:
    source_exclusions = resolved.setdefault(source, {})
    if column in source_exclusions:
        raise OptimizeError(f"Duplicate or overlapping star exclusion: {exclusion.sql()}")
    source_exclusions[column] = [] if path is None else [path]
```

Task 3 will extend the existing-entry branch to allow non-overlapping nested siblings. Do not use a set: it would discard order and quoted identifier structure.

- [ ] **Step 4: Make resolution failures explicit**

In `_resolve_except_columns`:

- Collect candidate sources from the current `tables` only.
- Zero candidates raises `OptimizeError(f"Unknown column: {exclusion.sql()}")`.
- More than one candidate raises `OptimizeError(f"Ambiguous column: {exclusion.sql()}")`.
- A source-qualified prefix whose top-level column is absent may fall back to struct-root resolution; if that also fails, raise.
- Never apply the final `.name` to every source.

- [ ] **Step 5: Run success and error tests together**

Run:

```bash
python -m unittest \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_qualified_column \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_errors
```

Expected: 2 tests PASS, including all subtests.

- [ ] **Step 6: Commit Task 2**

```bash
git add sqlglot/optimizer/qualify_columns.py tests/test_optimizer.py
git commit -m "fix(spark): validate star exclusion resolution [CODEX]"
```

### Task 3: Reconstruct nested struct-field exclusions

**Files:**
- Modify: `sqlglot/optimizer/resolver.py:133-221,394-431`
- Modify: `sqlglot/optimizer/qualify_columns.py` in exclusion resolution and star expansion
- Test: `tests/test_optimizer.py` in `TestOptimizer`

**Interfaces:**
- Produces: `Resolver.get_source_column_type(source_name: str, column_name: str) -> exp.DataType | None`
- Extends: `_add_resolved_except` to store non-overlapping sibling paths
- Produces: `_struct_star_except(source: str, column: str, paths: list[exp.Expr]) -> exp.Struct`
- Consumes: `Dialect.SUPPORTS_STRUCT_STAR_EXCEPT`

- [ ] **Step 1: Add failing nested-field output tests**

Add:

```python
    def test_qualify_star_except_struct_fields(self):
        cases = {
            "SELECT * EXCEPT (s.pk) "
            "FROM VALUES(named_struct('pk', 1, 'z', 2), 3) AS t(s, y)": [
                "STRUCT(t.s.* EXCEPT (pk)) AS s",
                "t.y AS y",
            ],
            "SELECT * EXCEPT (s.a.b) "
            "FROM VALUES(named_struct('a', named_struct('b', 1, 'c', 2), 'z', 3)) "
            "AS t(s)": [
                "STRUCT(t.s.* EXCEPT (a.b)) AS s",
            ],
        }

        for sql, expected in cases.items():
            with self.subTest(sql):
                qualified = qualify(
                    parse_one(sql, dialect="databricks"),
                    dialect="databricks",
                    quote_identifiers=False,
                )
                self.assertEqual(
                    [selection.sql(dialect="databricks") for selection in qualified.selects],
                    expected,
                )
```

These assertions isolate the changed projections from unrelated `VALUES` formatting. The projections must contain the exact native struct exclusions shown above.

- [ ] **Step 2: Run the nested-field test and verify failure**

Run:

```bash
python -m unittest tests.test_optimizer.TestOptimizer.test_qualify_star_except_struct_fields
```

Expected: FAIL because current expansion either retains the full struct or treats the final field name as a top-level exclusion.

- [ ] **Step 3: Add public source-column type lookup to `Resolver`**

Add:

```python
    def get_source_column_type(
        self, source_name: str, column_name: str
    ) -> exp.DataType | None:
        """Resolve a source column's known type without exposing scope traversal internals."""
        source = self.scope.sources.get(source_name)
        if not source:
            return None
        return self._get_column_type_from_scope(
            source,
            exp.column(column_name, table=source_name),
        )
```

Before editing this public method in implementation, use LSP references on `_get_column_type_from_scope` to confirm no existing public wrapper should be reused.

- [ ] **Step 4: Resolve relative nested paths without flattening identifiers**

```python
path = _build_except_path(remaining_parts)
```

`_build_except_path` produces an `exp.Column` for one segment and an `exp.Dot` rooted in that column for longer paths, so both shapes expose the complete `.parts` sequence. Do not call `exp.column("a.b")`, which creates a single quoted identifier rather than path `a.b`.

- [ ] **Step 5: Validate known struct paths**

Use `resolver.get_source_column_type(source, column)` and walk each relative path through `DataType.expressions`:

```python
def _validate_except_field_path(
    data_type: exp.DataType | None,
    path: exp.Expr,
    exclusion: exp.Expr,
) -> None:
    if not data_type or data_type.is_type(exp.DType.UNKNOWN):
        return

    current = data_type
    for identifier in path.parts:
        if not current.is_type(exp.DType.STRUCT):
            raise OptimizeError(f"Unknown field in star exclusion: {exclusion.sql()}")
        field = next((field for field in current.expressions if field.name == identifier.name), None)
        if field is None:
            raise OptimizeError(f"Unknown field in star exclusion: {exclusion.sql()}")
        current = field.kind
```

Honor dialect identifier normalization and quoted matching using the same comparison convention as source-column resolution; do not compare raw SQL strings.

- [ ] **Step 6: Allow non-overlapping nested siblings and reject overlaps**

Extend `_add_resolved_except`:

```python
    existing = source_exclusions.get(column)
    if existing is None:
        source_exclusions[column] = [] if path is None else [path]
        return
    if not existing or path is None:
        raise OptimizeError(f"Duplicate or overlapping star exclusion: {exclusion.sql()}")

    new_parts = tuple(identifier.name for identifier in path.parts)
    for existing_path in existing:
        existing_parts = tuple(identifier.name for identifier in existing_path.parts)
        prefix = min(len(new_parts), len(existing_parts))
        if new_parts[:prefix] == existing_parts[:prefix]:
            raise OptimizeError(f"Duplicate or overlapping star exclusion: {exclusion.sql()}")
    existing.append(path)
```

Use identifier-aware normalized comparison in the implementation; the tuple-of-name snippet shows the prefix invariant, not permission to discard quotedness.

- [ ] **Step 7: Build the native struct reconstruction AST**

Add:

```python
def _struct_star_except(source: str, column: str, paths: list[exp.Expr]) -> exp.Struct:
    return exp.Struct(
        expressions=[
            exp.Column(
                this=exp.Star(except_=[path.copy() for path in paths]),
                table=exp.to_identifier(column),
                db=exp.to_identifier(source),
            )
        ]
    )
```

In the source-column loop:

```python
                if nested_paths:
                    if not dialect.SUPPORTS_STRUCT_STAR_EXCEPT:
                        raise OptimizeError(
                            f"Nested star exclusions are unsupported for {dialect.__class__.__name__}"
                        )
                    new_selections.append(
                        alias(_struct_star_except(table, name, nested_paths), name, copy=False)
                    )
                    continue
```

Do not add a generator transform.

- [ ] **Step 8: Add known-invalid and overlap tests**

Extend `test_qualify_star_except_errors` with schema-backed inputs:

```python
        struct_invalid = (
            "SELECT * EXCEPT (one.missing) FROM structs",
            "SELECT * EXCEPT (one, one.a_1) FROM structs",
            "SELECT * EXCEPT (nested_0.nested_1, nested_0.nested_1.a_2) FROM structs",
        )
        for sql in struct_invalid:
            with self.subTest(sql):
                with self.assertRaises(OptimizeError):
                    qualify(
                        parse_one(sql, dialect="databricks"),
                        schema=self.schema,
                        dialect="databricks",
                    )
```

- [ ] **Step 9: Run all focused tests**

Run:

```bash
python -m unittest \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_qualified_column \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_errors \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_struct_fields
```

Expected: 3 tests PASS, including every subtest.

- [ ] **Step 10: Commit Task 3**

```bash
git add sqlglot/optimizer/resolver.py sqlglot/optimizer/qualify_columns.py tests/test_optimizer.py
git commit -m "fix(spark): preserve nested star exclusions [CODEX]"
```

### Task 4: Cover modifier isolation, quoting, and optimizer regressions

**Files:**
- Modify: `tests/test_optimizer.py` in `TestOptimizer`

**Interfaces:**
- Consumes all capabilities and helpers from Tasks 1-3
- Produces no new public API

- [ ] **Step 1: Add repeated-star isolation and combined-exclusion tests**

Add:

```python
    def test_qualify_star_except_isolation(self):
        cases = {
            "WITH r AS (SELECT 1 AS pk, 2 AS x) SELECT r.* EXCEPT (r.pk), r.* FROM r":
                "WITH r AS (SELECT 1 AS pk, 2 AS x) SELECT r.x AS x, r.pk AS pk, r.x AS x FROM r AS r",
            "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y) "
            "SELECT * EXCEPT (r.pk, d.pk) FROM r JOIN d ON r.pk = d.pk":
                "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y) "
                "SELECT r.x AS x, d.y AS y FROM r AS r JOIN d AS d ON r.pk = d.pk",
            "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y) "
            "SELECT r.* EXCEPT (r.pk), d.* FROM r JOIN d ON r.pk = d.pk":
                "WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y) "
                "SELECT r.x AS x, d.pk AS pk, d.y AS y FROM r AS r JOIN d AS d ON r.pk = d.pk",
        }

        for sql, expected in cases.items():
            with self.subTest(sql):
                self.assertEqual(
                    qualify(
                        parse_one(sql, dialect="databricks"),
                        dialect="databricks",
                        quote_identifiers=False,
                    ).sql(dialect="databricks"),
                    expected,
                )
```

- [ ] **Step 2: Add a quoted-identifier control**

Add a case that preserves source, top-level column, and nested field quoting:

```python
        sql = """
            SELECT * EXCEPT (`T`.`S`.`Drop Me`)
            FROM VALUES(named_struct('Drop Me', 1, 'Keep Me', 2)) AS `T`(`S`)
        """
        qualified = qualify(
            parse_one(sql, dialect="databricks"),
            dialect="databricks",
            quote_identifiers=False,
        )
        self.assertIn(
            "STRUCT(`T`.`S`.* EXCEPT (`Drop Me`)) AS `S`",
            qualified.sql(dialect="databricks"),
        )
```

This assertion targets the changed projection while avoiding unrelated fixture formatting.

- [ ] **Step 3: Run the new isolation tests**

Run:

```bash
python -m unittest tests.test_optimizer.TestOptimizer.test_qualify_star_except_isolation
```

Expected: PASS. A failure means one of Tasks 1-3 is incomplete; return to the owning task rather than adding a boundary-specific workaround.

- [ ] **Step 4: Run the complete optimizer regression file**

Run:

```bash
python -m unittest tests.test_optimizer
```

Expected: all tests PASS with zero errors and failures.

- [ ] **Step 5: Run style checks on changed Python files**

Run:

```bash
make style
```

Expected: Ruff formatting, Ruff lint, and configured type checks pass. If formatting changes files, rerun the focused star-exclusion tests from Task 3.

- [ ] **Step 6: Commit Task 4**

```bash
git add tests/test_optimizer.py
git commit -m "test(spark): cover qualified star exclusion boundaries [CODEX]"
```

### Task 5: Verify the original behavior end to end

**Files:**
- No planned source changes
- Verification only

**Interfaces:**
- Consumes the completed optimizer behavior
- Produces final behavioral evidence

- [ ] **Step 1: Run the exact SQLGlot reproduction**

Run:

```bash
python - <<'PY'
from sqlglot.optimizer import optimize

sql = """
WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y),
     j AS (SELECT * EXCEPT (r.pk) FROM r JOIN d ON r.pk = d.pk)
SELECT j.pk FROM j
"""
print(optimize(sql, dialect="databricks").sql(dialect="databricks"))
PY
```

Expected: command exits successfully; `j` projects `r.x`, `d.pk`, and `d.y`, and the outer query projects `j.pk`.

- [ ] **Step 2: Run the focused contract tests once more**

Run:

```bash
python -m unittest \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_qualified_column \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_errors \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_struct_fields \
  tests.test_optimizer.TestOptimizer.test_qualify_star_except_isolation
```

Expected: 4 tests PASS with all subtests.

- [ ] **Step 3: Compare native and optimized SQL in Spark 4.0.1**

Run:

```bash
SPARK_LOCAL_IP=127.0.0.1 uv run --no-project --with pyspark==4.0.1 python - <<'PY'
from pyspark.sql import SparkSession

from sqlglot.optimizer import optimize

spark = (
    SparkSession.builder.master("local[1]")
    .appName("qualified-star-except-verification")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

queries = (
    """
    WITH r AS (SELECT 1 AS pk, 2 AS x), d AS (SELECT 1 AS pk, 3 AS y),
         j AS (SELECT * EXCEPT (r.pk) FROM r JOIN d ON r.pk = d.pk)
    SELECT j.pk FROM j
    """,
    """
    SELECT * EXCEPT (s.pk)
    FROM VALUES(CAST(NULL AS STRUCT<pk: INT, z: INT>), 3) AS t(s, y)
    """,
)

for sql in queries:
    optimized_sql = optimize(sql, dialect="databricks").sql(dialect="databricks")
    native = spark.sql(sql)
    optimized = spark.sql(optimized_sql)
    assert optimized.schema.simpleString() == native.schema.simpleString()
    assert optimized.collect() == native.collect()
    print(optimized_sql)

spark.stop()
PY
```

Expected: both queries execute; optimized and native schemas and rows are identical. The first returns `pk=1`; the second retains `s` as `STRUCT<z: INT>` with `z=NULL` and retains `y=3`.

- [ ] **Step 4: Review changed call sites and generated SQL**

Confirm:

- `_add_except_columns` has been removed or fully replaced; no name-only qualified-exclusion path remains for enabled dialects.
- Every `_expand_stars` branch passes the current expression's fresh plan.
- No parser, expression-class, or generator transform changed.
- Spark owns both capability assignments and Databricks inherits them.
- Unknown nested types retain native nested paths rather than being flattened or discarded.

