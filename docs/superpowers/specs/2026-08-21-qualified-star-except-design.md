# Qualified Star `EXCEPT` Resolution Design

## Problem

SQLGlot parses a Spark or Databricks exclusion such as `* EXCEPT (r.pk)` as an `exp.Column` with `table="r"` and `name="pk"`. During optimizer star expansion, `_add_except_columns` reduces every exclusion to `e.name` and applies the resulting set to every source expanded by the star. In a join containing both `r.pk` and `d.pk`, this removes both columns. A later reference to the remaining `d.pk` therefore fails qualification.

The same representation also cannot distinguish a relation-qualified column from a struct field path. Spark resolves `s.pk` as relation-qualified `pk` when source `s` exposes that column; otherwise it can resolve it as field `pk` of a struct column named `s`.

## Contract

For Spark and Databricks star exclusions:

- `* EXCEPT (r.pk)` removes only `r.pk`.
- `* EXCEPT (pk)` is ambiguous when multiple selected sources expose `pk`.
- `* EXCEPT (z.pk)` is unresolved when neither source qualification nor struct-path resolution succeeds.
- `* EXCEPT (s.pk)` removes field `pk` from struct column `s` when no relation-qualified top-level candidate exists.
- Relation-qualified top-level resolution takes precedence over struct-path resolution when both interpretations are possible.
- Whole-column exclusions supersede exclusions of fields within the same column.
- Duplicate or overlapping exclusions are errors rather than silently collapsed.
- Output column order, struct field order, types, values, and NULL behavior must match the engine.

This behavior is established by current Databricks documentation and live Spark 4.0.1 execution.

## Ownership

The semantic owner is optimizer star expansion in `sqlglot/optimizer/qualify_columns.py`. The parser, expression model, copy behavior, and Databricks generator already preserve the complete exclusion path. No parser or generator rewrite is required.

Resolution should be shared optimizer functionality. Dialect capabilities control whether nested struct-star exclusion can be reconstructed after top-level star expansion.

## Approaches

### Resolver-backed exclusion plan

Resolve each exclusion against the star's selected sources before expanding columns. Store whole-column and nested-field exclusions separately for each source and top-level column. This is the selected approach because it preserves the original AST contract, centralizes name resolution, and covers ambiguity, invalid qualifiers, and nested paths.

### Pre-qualify exclusion expressions

Passing exclusions through ordinary column qualification can fix simple `r.pk` cases but does not represent nested-field pruning or relation-versus-field precedence. Rejected as incomplete.

### Parser rewrite into qualified stars

Rewriting `* EXCEPT (r.pk)` into source-specific stars would require scope and schema information that parsing does not have. Rejected as the wrong layer.

## Exclusion Plan

Replace the name-only structure:

```python
except_columns: dict[int, set[str]]
```

with a per-source, per-column plan conceptually equivalent to:

```python
except_columns: dict[str, dict[str, list[exp.Expr]]]
```

The outer key is the normalized selected-source name. The inner key is a top-level output column. Presence with an empty list means remove the whole column. A non-empty list contains relative nested paths to remove while retaining the top-level struct column.

Examples:

```text
* EXCEPT (r.pk)
{
    "r": {"pk": []},
    "d": {},
}
```

```text
* EXCEPT (s.a.b, t.y)
{
    "t": {
        "s": [Dot("a", "b")],
        "y": [],
    }
}
```

Source names should be used directly instead of `id(table)`, and each select expression should receive a fresh plan so modifiers cannot leak between repeated stars over the same source. This removes reliance on Python string object identity and makes resolution results explicit.

## Resolution

Add a focused helper near `_add_except_columns`. It receives the star expression, the sources selected by that star, and the current `Resolver`, then returns a fresh plan for that select expression.

For each exclusion:

1. Preserve its ordered `Identifier` parts, including quoting metadata.
2. Try relation-qualified resolution first. The longest valid selected-source prefix must expose the following identifier as a top-level column. Remaining identifiers form the nested field path.
3. If relation-qualified resolution does not succeed, treat the first identifier as a top-level column and resolve its owning source through `Resolver`. Remaining identifiers form the nested field path.
4. Raise `OptimizeError` if no source/column candidate exists.
5. Propagate ambiguity when an unqualified root column belongs to multiple selected sources.
6. Reject duplicate paths and overlaps such as `s` with `s.pk`, or `s.a` with `s.a.b`.
7. Record a whole-column exclusion only when the column has no existing exclusion entry. Record a nested path only when no whole-column exclusion exists. Any collision is an error.

For `t.s.a.b`, successful relation resolution produces source `t`, top-level column `s`, and relative path `exp.Dot.build([a, b])`. Relative paths remain expression trees rather than dotted strings, preserving quoting and arbitrary nesting.

When struct type information is available, validate every nested field segment. When it is unavailable, preserve the native nested exclusion in the reconstructed AST so the target engine performs validation. Unknown type information must never cause a nested path to be treated as a top-level name exclusion.

## Expansion

For each source column during `_expand_stars`:

1. No plan entry: use existing expansion.
2. Whole-column entry: skip the column.
3. Nested-path entries: emit a struct reconstruction under the original output name.

For Spark-family dialects, reconstruction uses the native nested star operator:

```sql
STRUCT(t.s.* EXCEPT (a.b)) AS s
```

Its AST is an `exp.Struct` containing a qualified column-star whose `Star.except_` contains relative paths. This preserves top-level output naming while leaving the engine-native struct transformation intact.

Manual enumeration of struct fields is prohibited. It requires complete type information, can reorder fields, and increases maintenance cost. Spark 4.0.1 does not expose `DROP_FIELDS` as a SQL routine. Live execution confirms that `STRUCT(t.s.* EXCEPT (...))` preserves the native exclusion's shape and NULL behavior.

## Dialect Capabilities

Add two independent capabilities, both disabled by default:

```python
STAR_EXCEPT_REQUIRES_RESOLUTION = False
SUPPORTS_STRUCT_STAR_EXCEPT = False
```

`Spark` enables both; `Databricks` inherits them. The first capability selects the shared resolver-backed exclusion plan. The second permits nested reconstruction as `STRUCT(t.s.* EXCEPT (...))`.

Other dialects retain their current behavior until their qualified-exclusion contracts are established. If a future dialect enables resolver-backed exclusion without native nested reconstruction, encountering a nested path raises explicit unsupported behavior. It must never fall back to name-only filtering or leave a partially expanded projection.

## Errors

Use existing SQLGlot `OptimizeError` conventions. Do not reproduce engine-specific error-class strings.

Required error cases:

- Ambiguous unqualified exclusion across selected sources.
- Unknown source qualifier or root column.
- Duplicate exclusion.
- Whole-column and nested-path overlap.
- Ancestor and descendant nested-path overlap.
- Known invalid nested field.

## Verification

Add optimizer coverage for:

1. The complete reported CTE query, including outer `SELECT j.pk`, proving that `d.pk` remains.
2. The symmetric `d.pk` exclusion.
3. Ambiguous unqualified `pk`.
4. Unknown qualifier `z.pk`.
5. A qualified star, `r.* EXCEPT (r.pk), d.*`.
6. Both qualified exclusions, `* EXCEPT (r.pk, d.pk)`.
7. A single nested field, `* EXCEPT (s.pk)`.
8. A multi-level nested field, `* EXCEPT (s.a.b)`.
9. Relation-versus-struct name collision precedence.
10. Duplicate and overlapping paths.
11. Quoted and case-sensitive source, column, and field identifiers.
12. Unknown schemas, ensuring nested exclusions are preserved rather than miscompiled.

Engine-backed expectations must assert output names and order, values, types, struct shape, and NULL behavior. SQL-only fixtures are insufficient for the core semantic cases.

## Non-goals

- Changing parser AST shape.
- Rewriting all star modifiers.
- Adding backward-compatibility aliases or preserving the name-only behavior.
- Matching Spark or Databricks error strings exactly.
- Refactoring unrelated qualification logic.
