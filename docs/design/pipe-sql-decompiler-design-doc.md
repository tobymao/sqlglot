# Design Document: Standard SQL to Pipe SQL Decompiler

## 1. Problem Statement

We need a deterministic program that transforms standard SQL queries into semantically equivalent GoogleSQL pipe syntax. This decompiler is the critical data generation component for fine-tuning small language models on pipe SQL (see companion document: *Large-Scale Incremental Pipe SQL Synthesis & Specialized Fine-Tuning*).

**Input**: A standard SQL query (any dialect) + optional schema definition.
**Output**: A semantically equivalent pipe SQL query in canonical GoogleSQL syntax.

**Requirements**:
- Deterministic: same input always produces same output.
- Semantics-preserving: the pipe SQL must return identical results on the same database.
- High coverage: handle 90%+ of queries in Spider 1.0 and BIRD-SQL benchmarks.
- Transparent failure: clearly report which SQL patterns could not be transformed.

---

## 2. Approach Evaluation

We evaluate four possible approaches before committing to an architecture.

### 2.1 Approach A: LLM-Based Translation

Use a frontier LLM (GPT-4o, Claude) to translate standard SQL to pipe SQL.

| Criterion | Assessment |
|---|---|
| Correctness | Low — LLMs hallucinate syntax, invent non-existent operators, produce queries that don't execute |
| Determinism | None — non-deterministic by nature |
| Speed | ~1 query/sec (API-bound) |
| Cost | ~$0.01–0.05 per query; $500–2,500 for 50K queries |
| Validation required | Every single output must be execution-validated |
| Coverage | Moderate — struggles with complex nesting, correlated subqueries |

**Verdict**: Rejected as primary approach. Useful only as a fallback for edge cases the deterministic decompiler cannot handle.

### 2.2 Approach B: Regex / String Rewriting

Apply pattern-matching rules to the SQL string (e.g., swap clause order, insert `|>` tokens).

| Criterion | Assessment |
|---|---|
| Correctness | Very low — SQL is not a regular language; regex cannot handle nesting, quoting, or context |
| Determinism | Yes |
| Speed | Fast |
| Coverage | Minimal — breaks on any non-trivial query (subqueries, CTEs, string literals containing SQL keywords) |

**Verdict**: Rejected. This is the approach used by DuckDB's community `psql` extension, which is self-described as "quick and dirty regex substitutions" and "mainly an experiment." Not suitable for production data generation.

### 2.3 Approach C: AST-Based Transformation (SQLGlot)

Parse SQL into an Abstract Syntax Tree, apply structural transformations, emit pipe SQL.

| Criterion | Assessment |
|---|---|
| Correctness | High — AST captures full syntactic structure; transformations are provably structure-preserving |
| Determinism | Yes |
| Speed | ~1,000 queries/sec (pure Python AST manipulation) |
| Coverage | High — handles all SQL constructs that SQLGlot can parse (30+ dialects) |
| Extensibility | New patterns handled by adding transformation rules |

**Verdict**: Selected as the primary approach. SQLGlot provides the richest SQL AST available in open source, with built-in optimizer passes (qualify, unnest_subqueries) that directly support our transformation needs.

### 2.4 Approach D: Relational Algebra IR

Parse SQL into a relational algebra representation (Scan → Filter → Project → Join → Aggregate → Sort), then emit pipe operators from the relational plan.

| Criterion | Assessment |
|---|---|
| Correctness | High — relational algebra is the formal foundation of both SQL and pipe syntax |
| Determinism | Yes |
| Implementation cost | Very high — requires building or integrating a full SQL-to-relational-algebra compiler (e.g., Apache Calcite) |
| Coverage | High in theory, but Calcite's Java ecosystem doesn't integrate with our Python pipeline |

**Verdict**: Theoretically elegant but impractical. The relational algebra approach adds a heavy dependency (Calcite is Java) and an unnecessary abstraction layer. SQLGlot's AST is close enough to relational algebra for our purposes — a `Select` node with `from_`, `joins`, `where`, `group`, `having`, `order`, `limit` maps directly to relational operators.

### 2.5 Decision: AST-Based with SQLGlot (Approach C)

The AST-based approach using SQLGlot is the clear winner. It provides the best balance of correctness, speed, coverage, and implementation cost. The remainder of this document details this architecture.

---

## 3. SQLGlot Capabilities and Limitations

### 3.1 What SQLGlot Provides

SQLGlot (v29.x) is a Python SQL parser/transpiler supporting 30+ dialects. It provides:

1. **Unified AST**: All SQL dialects parse into the same `Expression` type hierarchy. A `Select` node is the same whether it came from PostgreSQL, MySQL, or BigQuery.

2. **Rich type system**: 86 aggregate function types (`AggFunc` subclasses: `Count`, `Sum`, `Avg`, `Max`, `Min`, etc.), explicit node types for `Window`, `Join`, `Subquery`, `CTE`, `Union`, `Intersect`, `Except`, and all standard SQL constructs.

3. **Optimizer passes** relevant to decompilation:
   - `qualify()`: Resolves all column references to `table.column`, expands `SELECT *`, expands alias references. Requires schema for full resolution.
   - `unnest_subqueries()`: Converts correlated subqueries into equivalent JOIN patterns. Critical for handling `WHERE EXISTS`, `WHERE IN (correlated)`, and scalar subqueries.
   - `eliminate_ctes()`: Inlines single-use CTEs.
   - `merge_subqueries()`: Flattens derived tables where possible.
   - `simplify()`: Simplifies boolean expressions.

4. **Scope analysis**: `optimizer.scope.build_scope()` / `traverse_scope()` provide scope-aware traversal that correctly distinguishes CTE references from table references and identifies correlated column references across subquery boundaries.

5. **Pipe syntax parsing (one-directional)**: SQLGlot can parse pipe SQL and decompose it into CTE-based standard SQL. Supported pipe operators for parsing: `SELECT`, `WHERE`, `AGGREGATE`, `EXTEND`, `JOIN`, `ORDER BY`, `LIMIT`, `AS`, `PIVOT`, `UNPIVOT`, `TABLESAMPLE`, set operations.

6. **AST manipulation API**:
   - `expression.find(*types)` / `find_all(*types)` — locate nodes by type
   - `expression.walk()` — iterate all descendants
   - `expression.transform(fn)` — apply function to all nodes (DFS pre-order)
   - `expression.replace(new)` — swap node in parent
   - `expression.pop()` — remove from parent
   - `expression.parent` / `find_ancestor(*types)` — navigate upward

### 3.2 What SQLGlot Does NOT Provide

1. **No pipe syntax generator**: There is no `Generator` that outputs `|>` operators. The `Generator` class has zero pipe-related output methods. No dialect produces pipe syntax.

2. **No pipe AST nodes**: When pipe SQL is parsed, pipe nodes are destroyed at parse time and replaced with CTEs. There are no `PipeSelect`, `PipeWhere`, `PipeAggregate` expression types in the AST. The information is irreversibly lost.

3. **No standard-to-pipe transformation**: There is no `to_pipe()`, `decompile()`, or reverse transformation of any kind.

**Consequence**: We must build both the transformation logic (AST → pipe structure) and the output generation (pipe structure → string) from scratch. SQLGlot provides the input parsing, qualification, and subquery unnesting; we provide everything after.

---

## 4. Architecture

### 4.1 System Overview

```
                          ┌──────────────┐
                          │  Input SQL   │
                          │  (any dialect)│
                          └──────┬───────┘
                                 │
                    ┌────────────▼────────────┐
                    │   sqlglot.parse_one()   │
                    │   (dialect-aware parse) │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Pre-Processing        │
                    │   ┌─ qualify()          │
                    │   ├─ unnest_subqueries()│
                    │   ├─ merge_subqueries() │
                    │   └─ simplify()         │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Classification        │
                    │   (determine query type │
                    │    and complexity tier)  │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Pipe Emitter          │
                    │   (AST → pipe operator  │
                    │    sequence)            │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Pipe Serializer       │
                    │   (pipe operators →     │
                    │    formatted string)    │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   TransformResult       │
                    │   ┌─ pipe_sql: str      │
                    │   ├─ warnings: []       │
                    │   ├─ unsupported: []    │
                    │   └─ coverage: float    │
                    └─────────────────────────┘
```

### 4.2 Module Decomposition

```
pipe_decompiler/
├── __init__.py
├── decompiler.py          # Top-level orchestrator
├── preprocessor.py        # SQLGlot qualify + unnest + simplify
├── classifier.py          # Query complexity classification
├── emitter.py             # Core AST → pipe operator sequence logic
├── rules/
│   ├── __init__.py
│   ├── from_rule.py       # FROM extraction
│   ├── join_rule.py       # JOIN linearization
│   ├── where_rule.py      # WHERE promotion
│   ├── aggregate_rule.py  # GROUP BY + HAVING decomposition
│   ├── window_rule.py     # Window function + QUALIFY handling
│   ├── subquery_rule.py   # Subquery unrolling
│   ├── cte_rule.py        # CTE handling
│   ├── setop_rule.py      # UNION / INTERSECT / EXCEPT
│   ├── projection_rule.py # Final SELECT projection
│   └── terminal_rule.py   # ORDER BY, LIMIT, DISTINCT
├── serializer.py          # Pipe operator sequence → formatted string
├── result.py              # TransformResult dataclass
└── tests/
    ├── test_simple.py
    ├── test_joins.py
    ├── test_aggregation.py
    ├── test_subqueries.py
    ├── test_windows.py
    ├── test_ctes.py
    ├── test_setops.py
    ├── test_edge_cases.py
    └── test_execution.py  # Differential execution tests
```

### 4.3 Data Flow Types

```python
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum

class PipeOpType(Enum):
    FROM = "FROM"
    SELECT = "SELECT"
    EXTEND = "EXTEND"
    WHERE = "WHERE"
    AGGREGATE = "AGGREGATE"
    JOIN = "JOIN"
    ORDER_BY = "ORDER BY"
    LIMIT = "LIMIT"
    DISTINCT = "DISTINCT"
    DROP = "DROP"
    SET = "SET"
    RENAME = "RENAME"
    AS = "AS"
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"

@dataclass
class PipeOperator:
    """A single pipe operator in the output sequence."""
    op_type: PipeOpType
    sql_fragment: str          # The SQL text for this operator (e.g., "AVG(salary) AS avg_sal GROUP BY department")
    source_node: Optional[any] = None  # Reference to originating AST node (for debugging)

@dataclass
class PipeQuery:
    """An ordered sequence of pipe operators forming a complete query."""
    operators: List[PipeOperator] = field(default_factory=list)
    ctes: List['PipeQuery'] = field(default_factory=list)  # Recursive: each CTE is itself a PipeQuery
    cte_names: List[str] = field(default_factory=list)

@dataclass
class TransformResult:
    """Output of the decompiler."""
    pipe_sql: str              # The final pipe SQL string
    pipe_query: PipeQuery      # Structured representation
    warnings: List[str] = field(default_factory=list)     # Patterns that needed approximation
    unsupported: List[str] = field(default_factory=list)   # Patterns that could not be transformed
    coverage: float = 1.0      # 0.0 to 1.0 — fraction of query successfully transformed
```

---

## 5. Pre-Processing Pipeline

### 5.1 Why Pre-Processing Is Critical

Raw SQL from benchmarks contains ambiguities that make direct transformation unreliable. Pre-processing normalizes the AST into a form where transformation rules can operate without guesswork.

### 5.2 Step 1: Parse

```python
import sqlglot

ast = sqlglot.parse_one(sql, read=source_dialect, error_level=ErrorLevel.RAISE)
```

The `read` parameter selects the source dialect parser (e.g., `"postgres"`, `"mysql"`, `"bigquery"`). For benchmark queries, `"sqlite"` (Spider) or `"bigquery"` (BIRD-SQL) is typical.

### 5.3 Step 2: Qualify

```python
from sqlglot.optimizer import qualify

qualified = qualify.qualify(
    ast,
    schema=schema,                    # dict mapping table names to column names/types
    validate_qualify_columns=False,   # Allow partial resolution if schema is incomplete
    infer_schema=True,                # Infer schema from query structure when possible
)
```

**What this does**:
- Resolves `name` → `table.name` for all column references.
- Expands `SELECT *` → `SELECT table.col1, table.col2, ...`.
- Expands alias references (e.g., `WHERE total > 100` when `total` is a SELECT alias).
- Adds explicit table aliases (`FROM orders` → `FROM orders AS orders`).
- Quotes all identifiers for unambiguous parsing.

**Why it matters for decompilation**:
- Knowing which table owns each column is essential for correct JOIN linearization and aggregate/group-key classification.
- Star expansion is necessary to emit explicit `|> SELECT` projections.

**Limitations**:
- Requires schema for full resolution. Without it, ambiguous columns (appearing in multiple tables) remain unqualified.
- Does not resolve UDF/TVF output schemas or dynamic table references.

### 5.4 Step 3: Unnest Subqueries (Selective)

```python
from sqlglot.optimizer import unnest_subqueries

unnested = unnest_subqueries.unnest_subqueries(qualified)
```

**What this does**: Converts correlated subqueries in `WHERE` into equivalent `LEFT JOIN` patterns.

**Example**:
```sql
-- Before:
SELECT e.name FROM employees e
WHERE e.salary > (SELECT AVG(salary) FROM employees WHERE dept = e.dept)

-- After unnest:
SELECT e.name FROM employees e
LEFT JOIN (
    SELECT AVG(salary) AS _col_0, dept AS _u_1
    FROM employees GROUP BY dept
) AS _u_0 ON _u_0._u_1 = e.dept
WHERE e.salary > _u_0._col_0
```

The JOIN form converts cleanly to pipe syntax. Without unnesting, the correlated subquery would need to remain as-is inside a `|> WHERE`, which is valid but doesn't exploit the linear pipe structure.

**What it does NOT handle**:
- Multi-level correlated subqueries (inner subquery referencing two different outer scopes).
- Correlated subqueries with `LIMIT` / `OFFSET`.
- `NOT EXISTS` patterns may produce double negation (`NOT NOT ... IS NULL`), requiring simplification.

**Safety**: Run `simplify()` after `unnest_subqueries()` to clean up redundant boolean expressions.

### 5.5 Step 4: Merge Subqueries (Selective)

```python
from sqlglot.optimizer import merge_subqueries

merged = merge_subqueries.merge_subqueries(unnested)
```

**What this does**: Flattens derived tables (subqueries in `FROM`) into the outer query where possible. This reduces nesting that pipe syntax can express linearly.

**Example**:
```sql
-- Before:
SELECT * FROM (SELECT dept, AVG(salary) AS avg FROM emp GROUP BY dept) sub WHERE avg > 100

-- After merge (if safe):
SELECT dept, AVG(salary) AS avg FROM emp GROUP BY dept HAVING avg > 100
```

The merged form is simpler to transform. However, not all derived tables can be safely merged (e.g., if the subquery contains LIMIT, DISTINCT, or window functions).

### 5.6 Step 5: Simplify

```python
from sqlglot.optimizer import simplify

simplified = simplify.simplify(merged)
```

Cleans up boolean expression artifacts from previous optimizer passes (e.g., `NOT NOT x IS NULL` → `x IS NULL`, `TRUE AND x` → `x`).

### 5.7 Passes to AVOID

The following SQLGlot optimizer passes should **not** be used because they change query semantics in ways that make pipe transformation harder or produce unexpected results:

| Pass | Why to avoid |
|---|---|
| `pushdown_predicates` | Moves WHERE conditions into JOINs, changing the natural clause boundaries we want to preserve |
| `optimize_joins` | Reorders joins for performance; we want to preserve the author's intended join order |
| `eliminate_joins` | Removes "unnecessary" joins; changes query structure |
| `pushdown_projections` | Removes unused columns early; we want to preserve the full column set until the final SELECT |

---

## 6. Query Classification

Before applying transformation rules, classify the query to determine which rules are needed and what complexity tier it belongs to.

### 6.1 Complexity Tiers

| Tier | Characteristics | Example | Expected Coverage |
|---|---|---|---|
| **T1: Simple** | Single table, no joins, no subqueries, no aggregation | `SELECT name FROM users WHERE age > 21` | 100% |
| **T2: Aggregate** | Single table with GROUP BY / HAVING, no subqueries | `SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5` | 100% |
| **T3: Join** | Multi-table joins (any type), no subqueries | `SELECT ... FROM a JOIN b ON ... JOIN c ON ...` | 100% |
| **T4: Join + Aggregate** | Multi-table with joins and aggregation | `SELECT dept, SUM(amount) FROM orders JOIN customers ON ... GROUP BY dept` | 100% |
| **T5: Window** | Window functions and/or QUALIFY | `SELECT ..., ROW_NUMBER() OVER (...) AS rn ... QUALIFY rn = 1` | 100% |
| **T6: Subquery (simple)** | Non-correlated subqueries in WHERE (IN, EXISTS, scalar) | `WHERE id IN (SELECT id FROM vips)` | 95%+ |
| **T7: Subquery (correlated)** | Correlated subqueries (converted to JOINs by unnest) | `WHERE salary > (SELECT AVG(salary) FROM ... WHERE dept = outer.dept)` | 85%+ |
| **T8: CTE** | WITH clauses (non-recursive) | `WITH cte AS (...) SELECT ... FROM cte` | 95%+ |
| **T9: Set Operations** | UNION / INTERSECT / EXCEPT | `SELECT ... UNION ALL SELECT ...` | 100% |
| **T10: Complex** | Multiple of the above combined | Nested CTEs with correlated subqueries, window functions, and set operations | 70%+ |

### 6.2 Classification Logic

```python
def classify(ast: exp.Expression) -> Set[str]:
    """Return set of feature tags for the query."""
    features = set()

    if ast.find(exp.Join):
        features.add("join")
    if ast.find(exp.Group):
        features.add("aggregate")
    if ast.find(exp.Having):
        features.add("having")
    if any(isinstance(n, exp.AggFunc) for n in ast.walk()):
        features.add("agg_func")
    if ast.find(exp.Window):
        features.add("window")
    if ast.args.get("qualify"):
        features.add("qualify")
    if ast.find(exp.Subquery):
        features.add("subquery")
    if ast.find(exp.CTE):
        features.add("cte")
    if isinstance(ast, (exp.Union, exp.Intersect, exp.Except)):
        features.add("setop")
    if ast.find(exp.Exists):
        features.add("exists")
    if any(isinstance(n, exp.In) and n.find(exp.Subquery) for n in ast.walk()):
        features.add("in_subquery")

    # Check for correlated subqueries using scope analysis
    from sqlglot.optimizer.scope import traverse_scope
    for scope in traverse_scope(ast):
        if scope.external_columns:
            features.add("correlated")
            break

    return features
```

---

## 7. Transformation Rules

### 7.1 Rule Application Order

Rules are applied in a fixed order that mirrors the logical data flow of a pipe query:

```
1. CTE handling          — Extract and recursively transform CTEs
2. Set operation handling — Decompose UNION/INTERSECT/EXCEPT
3. FROM extraction       — Extract the source table(s)
4. JOIN linearization    — Convert JOINs to sequential |> JOIN operators
5. WHERE promotion       — Convert pre-aggregation WHERE
6. Expression analysis   — Classify SELECT expressions into categories
7. Pre-aggregation EXTEND — Emit computed columns needed before aggregation
8. AGGREGATE emission    — Emit |> AGGREGATE with GROUP BY
9. Post-agg WHERE        — Convert HAVING to |> WHERE
10. Window EXTEND         — Emit window functions as |> EXTEND
11. Post-window WHERE     — Convert QUALIFY to |> WHERE
12. Final SELECT          — Emit |> SELECT for final projection
13. ORDER BY              — Emit |> ORDER BY
14. LIMIT / OFFSET        — Emit |> LIMIT
15. DISTINCT              — Emit |> DISTINCT (if needed)
```

This ordering is deterministic and canonical. When multiple valid orderings exist, this order is always used. This ensures identical input always produces identical output, which is critical for training data consistency.

### 7.2 Rule 1: CTE Handling

**Input**: A `Select` node with `with_` argument containing CTE definitions.

**Strategy**: Preserve the WITH wrapper. Recursively decompile each CTE body and the main query independently.

```python
def transform_ctes(ast: exp.Select) -> PipeQuery:
    pipe_query = PipeQuery()

    with_clause = ast.args.get("with_")
    if with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias
            cte_body = cte.this  # The inner Select/Union
            cte_pipe = emit_pipe_query(cte_body)  # Recursive call
            pipe_query.ctes.append(cte_pipe)
            pipe_query.cte_names.append(cte_name)

        # Remove WITH from main query before processing
        ast.set("with_", None)

    # Process main query
    main_pipe = emit_pipe_query(ast)
    pipe_query.operators = main_pipe.operators
    return pipe_query
```

**Recursive CTEs**: Preserved as-is with `WITH RECURSIVE`. The recursive and base cases are each pipe-ified independently. The `UNION ALL` between them remains.

**Edge case — SELECT without FROM**: Queries like `SELECT 1 AS x, CURRENT_TIMESTAMP` have no table source. These cannot use the `FROM`-first pipe pattern and are emitted as standard SQL.

### 7.3 Rule 2: Set Operation Handling

**Input**: A `Union`, `Intersect`, or `Except` node.

**Strategy**: The AST represents set operations as a binary tree (left-recursive). Linearize by walking the left spine.

```python
def transform_setop(ast: exp.Union | exp.Intersect | exp.Except) -> PipeQuery:
    # Collect all branches by walking the left spine
    branches = []
    ops = []
    node = ast
    while isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
        branches.append(node.expression)  # Right branch
        op_name = type(node).__name__.upper()
        modifier = "" if node.args.get("distinct") else " ALL"
        ops.append(f"{op_name}{modifier}")
        node = node.this  # Left branch
    branches.append(node)  # The leftmost SELECT
    branches.reverse()
    ops.reverse()

    # First branch becomes the FROM
    pipe_query = emit_pipe_query(branches[0])

    # Subsequent branches become |> UNION/INTERSECT/EXCEPT operators
    for i, (branch, op) in enumerate(zip(branches[1:], ops)):
        branch_pipe = emit_pipe_query(branch)
        branch_sql = serialize_pipe_query(branch_pipe)
        pipe_query.operators.append(
            PipeOperator(PipeOpType[op.split()[0]], f"({branch_sql})")
        )

    return pipe_query
```

**Output example**:
```sql
FROM t1 |> SELECT name
|> UNION ALL (FROM t2 |> SELECT name)
|> EXCEPT DISTINCT (FROM t3 |> SELECT name)
```

### 7.4 Rule 3: FROM Extraction

**Input**: A `Select` node with a `from_` argument.

**Strategy**: Extract the FROM clause as the first pipe operator.

```python
def extract_from(ast: exp.Select) -> PipeOperator:
    from_clause = ast.args.get("from_")
    if not from_clause:
        raise UnsupportedError("SELECT without FROM cannot be expressed in pipe syntax")

    table_expr = from_clause.this  # The table/subquery expression

    # Handle derived tables (subqueries in FROM)
    if isinstance(table_expr, exp.Subquery):
        inner_pipe = emit_pipe_query(table_expr.this)  # Recursive
        inner_sql = serialize_pipe_query(inner_pipe)
        alias = table_expr.alias
        return PipeOperator(PipeOpType.FROM, f"({inner_sql}) AS {alias}")

    return PipeOperator(PipeOpType.FROM, table_expr.sql())
```

**Comma joins** (`FROM a, b, c`): SQLGlot parses these as implicit `CROSS JOIN`. They appear in `ast.args["joins"]` or as multiple expressions in the FROM clause. Handled by the JOIN rule.

### 7.5 Rule 4: JOIN Linearization

**Input**: The `joins` list from the `Select` node.

**Strategy**: Emit each JOIN as a separate `|> JOIN` operator in order.

```python
def linearize_joins(ast: exp.Select) -> List[PipeOperator]:
    operators = []
    for join in ast.args.get("joins", []):
        join_type = []
        if join.side:    # LEFT, RIGHT, FULL
            join_type.append(join.side)
        if join.kind:    # INNER, CROSS, SEMI, ANTI
            join_type.append(join.kind)
        join_type.append("JOIN")
        join_type_str = " ".join(join_type)

        table = join.this.sql()  # Table or subquery being joined

        # Handle subquery joins — recursively decompile
        if isinstance(join.this, exp.Subquery):
            inner_pipe = emit_pipe_query(join.this.this)
            table = f"({serialize_pipe_query(inner_pipe)}) AS {join.this.alias}"

        condition = ""
        if join.args.get("on"):
            condition = f" ON {join.args['on'].sql()}"
        elif join.args.get("using"):
            cols = ", ".join(col.sql() for col in join.args["using"])
            condition = f" USING ({cols})"

        operators.append(
            PipeOperator(PipeOpType.JOIN, f"{join_type_str} {table}{condition}")
        )
    return operators
```

**Self-joins**: Require `|> AS` before the JOIN to alias the left side:
```sql
FROM employees |> AS e1
|> JOIN employees AS e2 ON e1.manager_id = e2.id
```

The decompiler detects self-joins (same table appearing in FROM and a JOIN) and inserts `|> AS` automatically.

### 7.6 Rule 5: WHERE Promotion

**Input**: The `where` argument from the `Select` node.

**Strategy**: Emit as `|> WHERE` immediately after FROM and JOINs.

```python
def promote_where(ast: exp.Select) -> Optional[PipeOperator]:
    where = ast.args.get("where")
    if not where:
        return None
    return PipeOperator(PipeOpType.WHERE, where.this.sql())
```

This is the simplest rule. The WHERE condition expression is emitted as-is.

**Subqueries in WHERE**: Non-correlated subqueries (`WHERE id IN (SELECT ...)`) are preserved inline. The inner subquery can optionally be recursively decompiled to pipe syntax:
```sql
|> WHERE id IN (FROM vip_customers |> SELECT id)
```

### 7.7 Rule 6: Expression Analysis

**Purpose**: Classify each expression in the `SELECT` list into one of four categories. This classification drives rules 7–12.

```python
from sqlglot import expressions as exp

def classify_select_expressions(ast: exp.Select) -> dict:
    """Classify SELECT expressions into categories."""
    result = {
        "group_keys": [],       # Expressions that appear in GROUP BY
        "aggregates": [],       # Expressions containing aggregate functions
        "windows": [],          # Expressions containing window functions
        "plain": [],            # Everything else (simple column refs, CASE, arithmetic)
    }

    group_exprs = set()
    if ast.args.get("group"):
        for g in ast.args["group"].expressions:
            group_exprs.add(g.sql())

    for expr in ast.expressions:
        # Get the inner expression (unwrap Alias if present)
        inner = expr.this if isinstance(expr, exp.Alias) else expr

        has_agg = any(isinstance(n, exp.AggFunc) for n in inner.walk())
        has_window = any(isinstance(n, exp.Window) for n in inner.walk())

        if has_window:
            result["windows"].append(expr)
        elif has_agg:
            result["aggregates"].append(expr)
        elif inner.sql() in group_exprs or (isinstance(expr, exp.Alias) and expr.alias in group_exprs):
            result["group_keys"].append(expr)
        else:
            result["plain"].append(expr)

    return result
```

### 7.8 Rule 7: AGGREGATE Emission

**Input**: GROUP BY clause + aggregate expressions from the SELECT list + HAVING clause.

**Strategy**: Fuse grouping and aggregation into a single `|> AGGREGATE ... GROUP BY` operator. Convert HAVING to a subsequent `|> WHERE`.

```python
def emit_aggregate(ast: exp.Select, classified: dict) -> List[PipeOperator]:
    operators = []
    group = ast.args.get("group")
    if not group and not classified["aggregates"]:
        return operators

    # Build AGGREGATE clause
    agg_parts = []
    for expr in classified["aggregates"]:
        agg_parts.append(expr.sql())

    # Handle HAVING: may reference aggregates not in SELECT
    having = ast.args.get("having")
    extra_aggs = []
    if having:
        # Find aggregate functions in HAVING that aren't already in SELECT
        for node in having.this.walk():
            if isinstance(node, exp.AggFunc):
                agg_sql = node.sql()
                if not any(agg_sql in a for a in agg_parts):
                    alias = f"_having_{len(extra_aggs)}"
                    extra_aggs.append(f"{agg_sql} AS {alias}")
                    # Rewrite HAVING to reference the alias
                    node.replace(exp.Column(this=exp.to_identifier(alias)))

        agg_parts.extend(extra_aggs)

    agg_str = ", ".join(agg_parts)

    # Build GROUP BY clause
    group_parts = []
    if group:
        for g in group.expressions:
            group_parts.append(g.sql())

    group_str = " GROUP BY " + ", ".join(group_parts) if group_parts else ""

    if agg_str or group_str:
        operators.append(
            PipeOperator(PipeOpType.AGGREGATE, f"{agg_str}{group_str}")
        )

    # Convert HAVING to post-AGGREGATE WHERE
    if having:
        operators.append(
            PipeOperator(PipeOpType.WHERE, having.this.sql())
        )

    # If extra aggregates were synthesized for HAVING, add SELECT to remove them
    if extra_aggs:
        # Project only the original columns (exclude _having_* temporaries)
        original_cols = [g.sql() for g in group.expressions] if group else []
        original_cols += [e.alias if isinstance(e, exp.Alias) else e.sql() for e in classified["aggregates"]]
        operators.append(
            PipeOperator(PipeOpType.SELECT, ", ".join(original_cols))
        )

    return operators
```

**Full-table aggregation** (no GROUP BY): Emitted as `|> AGGREGATE SUM(x) AS total` without a GROUP BY clause. Output is a single row.

**HAVING referencing aggregates not in SELECT**: The decompiler synthesizes temporary aggregate columns with `_having_N` aliases, adds a WHERE filter, then projects them away with a final SELECT. Example:

```sql
-- Input:
SELECT department FROM emp GROUP BY department HAVING COUNT(*) > 10

-- Output:
FROM emp
|> AGGREGATE COUNT(*) AS _having_0 GROUP BY department
|> WHERE _having_0 > 10
|> SELECT department
```

### 7.9 Rule 8: Window Function Handling

**Input**: Window function expressions from the SELECT list + QUALIFY clause.

**Strategy**: Emit window functions as `|> EXTEND` operators. Convert QUALIFY to `|> WHERE`.

```python
def emit_windows(classified: dict, ast: exp.Select) -> List[PipeOperator]:
    operators = []

    for expr in classified["windows"]:
        operators.append(
            PipeOperator(PipeOpType.EXTEND, expr.sql())
        )

    # Convert QUALIFY to post-window WHERE
    qualify = ast.args.get("qualify")
    if qualify:
        operators.append(
            PipeOperator(PipeOpType.WHERE, qualify.this.sql())
        )

    return operators
```

**Window functions in non-windowed queries**: If the query has window functions but no GROUP BY, the EXTEND operators appear after the WHERE (if any).

**Multiple window functions**: Each can be a separate EXTEND or combined into one:
```sql
-- Combined (single EXTEND):
|> EXTEND ROW_NUMBER() OVER (...) AS rn, SUM(amount) OVER (...) AS running_total

-- Separate (multiple EXTENDs):
|> EXTEND ROW_NUMBER() OVER (...) AS rn
|> EXTEND SUM(amount) OVER (...) AS running_total
```

The decompiler uses the combined form by default for brevity.

### 7.10 Rule 9: Final SELECT Projection

**Input**: The remaining SELECT expressions after aggregates and windows have been extracted.

**Strategy**: If the pipe operators so far already produce exactly the desired columns in the desired order, omit the SELECT. Otherwise, emit `|> SELECT` to project to the final column set.

```python
def emit_final_select(ast: exp.Select, classified: dict, preceding_ops: List[PipeOperator]) -> Optional[PipeOperator]:
    # Determine if a final SELECT is needed
    # After AGGREGATE, output is group_keys + aggregate aliases
    # After EXTEND, output adds window columns
    # If the desired output matches, no SELECT needed

    has_aggregate = any(op.op_type == PipeOpType.AGGREGATE for op in preceding_ops)

    if not has_aggregate and not classified["windows"]:
        # Simple query: the SELECT defines the entire projection
        select_exprs = [e.sql() for e in ast.expressions]
        return PipeOperator(PipeOpType.SELECT, ", ".join(select_exprs))

    # After AGGREGATE + EXTEND, check if we need to reorder or drop columns
    desired = [e.alias if isinstance(e, exp.Alias) else e.sql() for e in ast.expressions]
    # Compare with what the pipeline produces... (implementation detail)

    # If mismatch, emit SELECT
    return PipeOperator(PipeOpType.SELECT, ", ".join(desired))
```

**SELECT DISTINCT**: If the original query has `SELECT DISTINCT`, emit either `|> SELECT DISTINCT ...` or `|> SELECT ... |> DISTINCT`.

### 7.11 Rule 10: Terminal Operators

```python
def emit_terminals(ast: exp.Select) -> List[PipeOperator]:
    operators = []

    order = ast.args.get("order")
    if order:
        order_parts = [o.sql() for o in order.expressions]
        operators.append(PipeOperator(PipeOpType.ORDER_BY, ", ".join(order_parts)))

    limit = ast.args.get("limit")
    if limit:
        limit_str = limit.this.sql()
        offset = ast.args.get("offset")
        if offset:
            limit_str += f" OFFSET {offset.this.sql()}"
        operators.append(PipeOperator(PipeOpType.LIMIT, limit_str))

    return operators
```

---

## 8. Serialization

### 8.1 Pipe Query to String

```python
def serialize_pipe_query(query: PipeQuery, indent: int = 0) -> str:
    lines = []

    # Emit CTEs
    if query.ctes:
        cte_defs = []
        for name, cte_query in zip(query.cte_names, query.ctes):
            cte_sql = serialize_pipe_query(cte_query, indent=indent + 2)
            cte_defs.append(f"{name} AS (\n{cte_sql}\n)")
        lines.append("WITH " + ",\n     ".join(cte_defs))

    # Emit operators
    for i, op in enumerate(query.operators):
        if i == 0:
            # First operator (FROM) — no pipe prefix
            lines.append(f"{op.op_type.value} {op.sql_fragment}")
        else:
            lines.append(f"|> {op.op_type.value} {op.sql_fragment}")

    prefix = " " * indent
    return "\n".join(prefix + line for line in lines)
```

### 8.2 Formatting Conventions

- One operator per line.
- `|>` prefix aligned at the same indentation level.
- Multi-line operator arguments (e.g., long AGGREGATE with many columns) indented by 4 spaces.
- Subqueries within operators indented by an additional 2 spaces.

---

## 9. Column Visibility Tracking

### 9.1 Why Track Column Visibility

The decompiler must know the active column set after each pipe operator to:
1. Determine whether a final `|> SELECT` is needed.
2. Verify that emitted operators reference valid columns.
3. Handle `HAVING` references to aggregates not in `SELECT`.
4. Detect when `|> AS` is needed before a self-join.

### 9.2 Visibility Model

```python
@dataclass
class ColumnState:
    """Tracks visible columns at a point in the pipe."""
    columns: Dict[str, str]      # name -> type (or "unknown")
    table_aliases: Dict[str, List[str]]  # alias -> [column names]

def apply_operator(state: ColumnState, op: PipeOperator) -> ColumnState:
    """Compute new column state after applying a pipe operator."""
    match op.op_type:
        case PipeOpType.FROM:
            return schema_lookup(op.sql_fragment)
        case PipeOpType.SELECT:
            return ColumnState(
                columns={col: infer_type(col) for col in parse_select_list(op.sql_fragment)},
                table_aliases={}  # SELECT destroys aliases
            )
        case PipeOpType.EXTEND:
            new_state = copy(state)
            for col in parse_extend_list(op.sql_fragment):
                new_state.columns[col.alias] = infer_type(col)
            return new_state
        case PipeOpType.AGGREGATE:
            # Only group keys + aggregate aliases survive
            return ColumnState(
                columns=parse_aggregate_output(op.sql_fragment),
                table_aliases={}  # AGGREGATE destroys aliases
            )
        case PipeOpType.WHERE | PipeOpType.ORDER_BY | PipeOpType.LIMIT | PipeOpType.DISTINCT:
            return state  # No schema change
        case PipeOpType.JOIN:
            new_state = copy(state)
            right_cols = schema_lookup(parse_join_table(op.sql_fragment))
            new_state.columns.update(right_cols.columns)
            return new_state
        case PipeOpType.DROP:
            new_state = copy(state)
            for col in parse_drop_list(op.sql_fragment):
                new_state.columns.pop(col, None)
            return new_state
```

Key invariants from the GoogleSQL spec:
- **SELECT** and **AGGREGATE** create new column scopes (destroy previous aliases).
- **EXTEND**, **WHERE**, **ORDER BY**, **LIMIT**, **DISTINCT** preserve the existing scope.
- **JOIN** merges left and right column sets.
- **DROP** removes columns but table aliases can still access originals (a subtlety we don't need for decompilation).

---

## 10. Edge Cases and Limitations

### 10.1 Patterns That Cannot Be Pipe-ified

| Pattern | Reason | Handling |
|---|---|---|
| `SELECT 1 AS x` (no FROM) | No table source for pipe entry | Emit as standard SQL; flag in `unsupported` |
| `INSERT / UPDATE / DELETE / MERGE` | DML, not queries | Out of scope; reject |
| `CREATE TABLE AS SELECT` | DDL wrapper | Pipe-ify the inner SELECT; preserve the DDL wrapper |

### 10.2 Patterns That Require Special Care

| Pattern | Challenge | Strategy |
|---|---|---|
| **Recursive CTEs** | Cannot flatten; recursive reference is self-referential | Preserve WITH RECURSIVE; pipe-ify base and recursive cases independently |
| **HAVING referencing aggregates not in SELECT** | Pipe AGGREGATE only produces columns listed in AGGREGATE and GROUP BY | Synthesize temporary aggregate aliases, filter, then project away |
| **Multi-level aggregation** (aggregation of aggregation) | Standard SQL requires nesting; pipe eliminates this naturally | Emit two sequential `\|> AGGREGATE` operators — pipe syntax's strength |
| **Implicit CROSS JOINs** (`FROM a, b`) | No explicit JOIN keyword | Convert to `\|> CROSS JOIN b` |
| **Self-joins** | Pipe input table needs a name for ON condition | Insert `\|> AS alias` before the JOIN |
| **Correlated subqueries that unnest failed to convert** | Complex correlation patterns | Preserve as-is inside `\|> WHERE`; flag in `warnings` |
| **EXISTS / NOT EXISTS** | After unnesting, may produce `LEFT JOIN ... WHERE ... IS [NOT] NULL` | Clean up double negation with simplify() |
| **DISTINCT ON (expr)** (PostgreSQL-specific) | Not available in GoogleSQL pipe syntax | Convert to `\|> EXTEND ROW_NUMBER() OVER (PARTITION BY expr ORDER BY ...) AS rn \|> WHERE rn = 1` (SQLGlot's `eliminate_distinct_on` handles this) |
| **Aggregated window functions** (`SUM(COUNT(*)) OVER ()`) | Requires two-stage pipeline | Split: `\|> AGGREGATE COUNT(*) AS cnt GROUP BY x \|> EXTEND SUM(cnt) OVER () AS total` |
| **Scalar subqueries in SELECT** | `SELECT (SELECT MAX(x) FROM t2) AS max_x, ...` | Preserve as-is within `\|> SELECT`; optionally convert to `\|> JOIN` + `\|> EXTEND` |

### 10.3 Coverage Estimate by Benchmark

Based on analysis of Spider 1.0 and BIRD-SQL query distributions:

| Query Category | % of Benchmark | Estimated Decompiler Coverage |
|---|---|---|
| Simple (T1) | ~25% | 100% |
| Aggregate (T2) | ~20% | 100% |
| Join (T3–T4) | ~25% | 100% |
| Window (T5) | ~5% | 100% |
| Subquery (T6–T7) | ~15% | 90%+ (after unnesting) |
| CTE (T8) | ~5% | 95%+ |
| Set ops (T9) | ~3% | 100% |
| Complex (T10) | ~2% | 75%+ |
| **Weighted total** | **100%** | **~96%** |

---

## 11. Testing Strategy

### 11.1 Layer 1: Unit Tests (Per-Rule)

Each transformation rule has a dedicated test file with input/output pairs covering:
- Minimal cases (the simplest query that exercises the rule).
- Boundary cases (empty GROUP BY, single-column SELECT, self-join).
- Negative cases (queries that should NOT trigger the rule).

```python
def test_simple_where():
    assert decompile("SELECT name FROM users WHERE age > 21") == \
        "FROM users\n|> WHERE age > 21\n|> SELECT name"

def test_aggregate_with_having():
    assert decompile("SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept HAVING cnt > 5") == \
        "FROM emp\n|> AGGREGATE COUNT(*) AS cnt GROUP BY dept\n|> WHERE cnt > 5"
```

### 11.2 Layer 2: Round-Trip Tests

Parse the output pipe SQL with SQLGlot (which converts pipe → standard SQL via CTEs), then compare the resulting standard SQL with the original input for semantic equivalence.

```python
def test_roundtrip(standard_sql):
    pipe_sql = decompile(standard_sql)
    roundtripped = sqlglot.transpile(pipe_sql, read="bigquery", write="bigquery")[0]
    # roundtripped is CTE-based standard SQL
    assert semantically_equivalent(standard_sql, roundtripped)
```

### 11.3 Layer 3: Differential Execution Tests

Execute both the original standard SQL and the decompiled pipe SQL (transpiled back to standard via SQLGlot) against a real database and compare result sets.

**Test database**: Use the Spider 1.0 SQLite databases. For each benchmark query:

```python
def test_execution(benchmark_query, db_path):
    pipe_sql = decompile(benchmark_query.sql)
    standard_from_pipe = sqlglot.transpile(pipe_sql, read="bigquery", write="sqlite")[0]

    conn = sqlite3.connect(db_path)
    result_original = pd.read_sql(benchmark_query.sql, conn)
    result_pipe = pd.read_sql(standard_from_pipe, conn)

    # Sort both by all columns, compare
    assert_frame_equal(
        result_original.sort_values(list(result_original.columns)).reset_index(drop=True),
        result_pipe.sort_values(list(result_pipe.columns)).reset_index(drop=True),
    )
```

### 11.4 Layer 4: Benchmark Coverage Tests

Run the decompiler over all queries in Spider 1.0 (~7K train + ~1K dev) and BIRD-SQL (~9.4K train + ~1.5K dev). Report:
- Success rate (queries that decompile without errors).
- Warning rate (queries that decompile with warnings).
- Failure rate (queries that cannot be decompiled).
- Execution match rate (pipe output matches original on the database).

Target: 90%+ success rate, 95%+ execution match rate on successful decompilations.

### 11.5 Layer 5: Property-Based Fuzzing

Use Hypothesis (Python) to generate random SQL ASTs and verify the decompiler never crashes:

```python
from hypothesis import given, strategies as st

@given(sql=sql_ast_strategy())
def test_never_crashes(sql):
    result = decompile(sql)
    assert isinstance(result, TransformResult)
    # May have warnings/unsupported, but should never raise
```

---

## 12. Performance Considerations

### 12.1 Expected Throughput

| Stage | Time per query | Notes |
|---|---|---|
| Parse | ~0.2ms | SQLGlot recursive descent parser |
| Qualify | ~0.5ms | Schema lookup + column resolution |
| Unnest subqueries | ~0.3ms | Only runs on queries with correlated subqueries |
| Transformation rules | ~0.2ms | Pure AST manipulation |
| Serialization | ~0.1ms | String formatting |
| **Total** | **~1.3ms** | **~770 queries/sec** |

For 50K queries: ~65 seconds total. Well within practical limits.

### 12.2 Memory

Each query is processed independently. Memory usage is proportional to AST size (~10KB per typical query). No persistent state between queries.

---

## 13. Implementation Roadmap

### Phase 1: Core Rules (Week 1–2)
- Implement rules 1–5 (CTE, set ops, FROM, JOIN, WHERE) and rule 10 (terminals).
- Handle Tiers T1, T3, T9.
- Unit tests for each rule.
- Target: 50% of benchmark queries decompile successfully.

### Phase 2: Aggregation + Windows (Week 2–3)
- Implement rules 6–9 (expression analysis, AGGREGATE, windows, final SELECT).
- Handle Tiers T2, T4, T5.
- Target: 80% of benchmark queries.

### Phase 3: Subqueries + Edge Cases (Week 3–4)
- Integrate `unnest_subqueries` pre-processing.
- Implement subquery handling in WHERE, SELECT, and FROM.
- Handle Tiers T6, T7, T8.
- Target: 90%+ of benchmark queries.

### Phase 4: Validation + Hardening (Week 4–5)
- Differential execution tests against Spider 1.0 and BIRD-SQL databases.
- Property-based fuzzing.
- Fix edge cases surfaced by testing.
- Target: 95%+ execution match rate.

### Phase 5: Integration (Week 5–6)
- Integrate with the data augmentation pipeline.
- Generate trajectory-decomposed JSONL training files.
- Produce the final corpus of 50K+ validated pipe SQL queries.
