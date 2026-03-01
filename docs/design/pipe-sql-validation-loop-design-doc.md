# Design Document: SQLite₁ → Pipe SQL → SQLite₂ Validation & Feedback Loop

## 1. Problem Statement

The decompiler transforms standard SQL into pipe SQL. We must prove that this transformation preserves semantics — that the pipe SQL, when executed, returns exactly the same results as the original.

This document designs a closed-loop validation system using a **tiered benchmark strategy**: Spider 1.0 as the primary lightweight dataset (~1 GB), with BIRD Mini-Dev as a secondary stress test.

```
SQLite₁ (gold SQL) ──execute──► Result Set A
        │
        ▼
   [Decompiler]
        │
        ▼
   Pipe SQL (synthesized)
        │
        ▼
   [SQLGlot transpile pipe→sqlite]
        │
        ▼
SQLite₂ (round-tripped) ──execute──► Result Set B

    Result Set A == Result Set B ?
```

**Goal**: For every query in the benchmark, `Result Set A == Result Set B`. Any mismatch triggers a diagnostic feedback loop that identifies the root cause and feeds corrections back into the decompiler.

---

## 2. Data Source: Tiered Benchmark Strategy

### 2.1 Why Not BIRD-SQL Directly?

The full BIRD-SQL benchmark is **33.4 GB** — most of that bulk comes from a few massive databases (financial, geographic datasets). This creates unnecessary friction for iterative decompiler development where fast feedback cycles matter most.

### 2.2 Tier 1 (Primary): Spider 1.0

| Property | Value |
|---|---|
| Total queries | 10,181 (8,659 train + 1,034 dev + 2,147 test) |
| Database engine | SQLite |
| Number of databases | 200 (across 138 domains) |
| Database size | **~1 GB total** |
| Difficulty levels | easy, medium, hard, extra hard |
| Ground truth | Execution-verified SQL with known-correct result sets |
| Download | yale-lily.github.io/spider |

Spider 1.0 is ideal as the primary validation dataset because:
1. **Lightweight** — ~1 GB total, 200 small SQLite databases. Fast to download, fast to iterate.
2. All databases are **SQLite** — no external database setup required.
3. Ground truth SQL is **execution-verified** — we know the gold SQL produces correct results.
4. Queries cover JOINs, aggregations, subqueries, GROUP BY, ORDER BY, HAVING, nested queries, and set operations.
5. **Well-studied** — extensive prior work means known failure modes and edge cases are documented.
6. 200 databases across 138 domains provide broad schema diversity.

### 2.3 Tier 2 (Stress Test): BIRD Mini-Dev

| Property | Value |
|---|---|
| Total queries | 500 (curated high-quality subset) |
| Database engine | SQLite |
| Number of databases | 11 |
| Database size | ~few GB (much smaller than full BIRD's 33.4 GB) |
| Difficulty levels | simple, moderate, challenging |
| Download | HuggingFace `birdsql/bird_mini_dev` |

BIRD Mini-Dev is used as a secondary stress test because:
1. Queries are harder than Spider — more complex JOINs, domain-specific reasoning, challenging expressions.
2. 500 curated queries is manageable but tests edge cases Spider may miss.
3. Uses the same 11 dev databases as full BIRD but without the massive train databases.

### 2.4 Tier 3 (Production Scale-Up): Full BIRD Train

Once the decompiler passes Tier 1 and Tier 2, apply it to the full BIRD train set (9,428 queries, 33.4 GB) for large-scale golden corpus generation. This is a one-time batch job — the 30+ GB download is justified only at this stage.

### 2.5 Data Format

**Spider 1.0:**
```
spider/
├── dev.json                         # Question-SQL pairs
│   [
│     {
│       "db_id": "concert_singer",
│       "query": "SELECT count(*) FROM singer",
│       "query_toks": ["SELECT", "count", "(", "*", ")", ...],
│       "question": "How many singers do we have?",
│       "hardness": "easy"
│     },
│     ...
│   ]
├── database/
│   ├── concert_singer/
│   │   ├── concert_singer.sqlite
│   │   └── schema.sql
│   ├── pets_1/
│   │   ├── pets_1.sqlite
│   │   └── schema.sql
│   └── ... (200 databases)
└── dev_gold.sql                     # Gold SQL per line
```

**BIRD Mini-Dev:**
```
bird_mini_dev/
├── mini_dev_sqlite.json             # Question-SQL pairs
│   [
│     {
│       "question_id": 7,
│       "db_id": "california_schools",
│       "question": "What is the phone number of ...",
│       "evidence": "",
│       "SQL": "SELECT T2.Phone FROM satscores AS T1 INNER JOIN ...",
│       "difficulty": "simple"
│     },
│     ...
│   ]
├── mini_dev_databases/
│   ├── california_schools/
│   │   └── california_schools.sqlite
│   └── ... (11 databases)
└── mini_dev_gold.sql
```

### 2.6 Working Set

| Phase | Dataset | Queries | Purpose |
|---|---|---|---|
| Development & iteration | Spider 1.0 dev | 1,034 | Fast feedback loop (~1 GB, seconds to run) |
| Stress testing | BIRD Mini-Dev | 500 | Harder queries, edge case discovery |
| Production corpus | Spider 1.0 train | 8,659 | Scale-up validated pipe SQL pairs |
| Production corpus | BIRD train | 9,428 | Maximum training data (download 33.4 GB only at this stage) |

---

## 3. Validation Pipeline Architecture

### 3.1 End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        For each (question_id, db_id, gold_sql) │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────────┐  │
│  │  gold_sql     │────►│  Execute on  │────►│  Result Set A  │  │
│  │  (SQLite)     │     │  SQLite DB   │     │  (gold result) │  │
│  └──────┬───────┘     └──────────────┘     └───────┬────────┘  │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐                                  │            │
│  │  Parse with   │                                  │            │
│  │  SQLGlot      │                                  │            │
│  │  (read=sqlite)│                                  │            │
│  └──────┬───────┘                                  │            │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐                                  │            │
│  │  Pre-process  │                                  │            │
│  │  (qualify,    │                                  │            │
│  │   unnest,     │                                  │            │
│  │   simplify)   │                                  │            │
│  └──────┬───────┘                                  │            │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐                                  │            │
│  │  Pipe Emitter │                                  │            │
│  │  (AST → pipe  │                                  │            │
│  │   operators)  │                                  │            │
│  └──────┬───────┘                                  │            │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐                                  │            │
│  │  pipe_sql     │                                  │            │
│  │  (canonical   │                                  │            │
│  │   pipe syntax)│                                  │            │
│  └──────┬───────┘                                  │            │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐                                  │            │
│  │  SQLGlot      │                                  │            │
│  │  transpile    │                                  │            │
│  │  pipe→sqlite  │                                  │            │
│  └──────┬───────┘                                  │            │
│         │                                          │            │
│         ▼                                          │            │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────────┐  │
│  │  sqlite2_sql  │────►│  Execute on  │────►│  Result Set B  │  │
│  │  (round-trip) │     │  same DB     │     │  (pipe result) │  │
│  └──────────────┘     └──────────────┘     └───────┬────────┘  │
│                                                    │            │
│                                          ┌─────────▼─────────┐ │
│                                          │   Compare A == B  │ │
│                                          └─────────┬─────────┘ │
│                                                    │            │
│                            ┌───────────────────────┼──────┐    │
│                            │                       │      │    │
│                            ▼                       ▼      ▼    │
│                         MATCH              MISMATCH   ERROR    │
│                     (log success)      (enter feedback) (triage)│
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 The Three Outcomes

| Outcome | Meaning | Action |
|---|---|---|
| **MATCH** | `set(Result A) == set(Result B)` | Query is validated. Add to golden corpus. |
| **MISMATCH** | Both execute, but result sets differ | Enter diagnostic feedback loop (Section 5). |
| **ERROR** | SQLite₂ fails to execute (syntax error, runtime error) | Enter error triage (Section 6). |

An additional sub-outcome exists:

| Sub-outcome | Meaning | Action |
|---|---|---|
| **DECOMPILE_FAIL** | The decompiler could not transform the query | Log with classification. Attempt fallback strategies. |
| **TIMEOUT** | Execution exceeds 30-second limit | Use BIRD's standard 30s timeout. Score as ERROR. |

---

## 4. Result Comparison Logic

### 4.1 Set-Based Comparison

Following standard text-to-SQL evaluation methodology (used by both Spider and BIRD), comparison is **set-based** — row order does not matter:

```python
def compare_results(result_a: List[Tuple], result_b: List[Tuple]) -> bool:
    """Compare two result sets using set equality."""
    return set(result_a) == set(result_b)
```

This is intentionally strict: every row must match exactly. No fuzzy matching, no type coercion.

### 4.2 Enhanced Comparison (For Diagnostic Purposes)

When set comparison fails, we compute additional metrics to diagnose the mismatch:

```python
@dataclass
class ComparisonResult:
    match: bool                    # set(A) == set(B)
    result_a_rows: int
    result_b_rows: int
    result_a_cols: int
    result_b_cols: int

    # Diagnostic fields (only computed on mismatch)
    row_count_match: bool          # len(A) == len(B)
    col_count_match: bool          # same number of columns
    col_types_match: bool          # column types compatible
    sorted_match: bool             # match after sorting both
    subset_a_in_b: bool            # A ⊆ B
    subset_b_in_a: bool            # B ⊆ A
    symmetric_difference: int      # |A △ B|
    sample_diff_rows: List[Tuple]  # Up to 5 rows in A but not B
    f1_score: float                # Cell-level F1 (soft metric)

    # Root cause classification
    mismatch_type: str             # See Section 5.2
```

### 4.3 Floating-Point Tolerance

SQLite may return slightly different floating-point results depending on expression evaluation order. We apply a tolerance layer:

```python
def normalize_row(row: Tuple, tolerance: float = 1e-6) -> Tuple:
    """Normalize a row for comparison."""
    normalized = []
    for val in row:
        if isinstance(val, float):
            normalized.append(round(val, 6))
        elif isinstance(val, str):
            normalized.append(val.strip())
        elif val is None:
            normalized.append(None)
        else:
            normalized.append(val)
    return tuple(normalized)

def compare_with_tolerance(result_a, result_b, tolerance=1e-6):
    set_a = set(normalize_row(r, tolerance) for r in result_a)
    set_b = set(normalize_row(r, tolerance) for r in result_b)
    return set_a == set_b
```

### 4.4 Column Order Handling

Standard evaluation compares result sets as sets of tuples, which means column order matters (a row `(1, 'Alice')` ≠ `('Alice', 1)`). Since pipe SQL may reorder columns (e.g., `|> AGGREGATE` outputs grouping columns first, then aggregates), the decompiler must ensure the final `|> SELECT` matches the original column order.

If column reordering is the sole cause of mismatch, it is classified as a **REORDER** mismatch (non-semantic, fixable by adjusting the final SELECT).

---

## 5. Diagnostic Feedback Loop (Mismatch Handling)

### 5.1 Feedback Loop Flow

```
            MISMATCH detected
                 │
                 ▼
        ┌────────────────┐
        │  Classify       │
        │  mismatch type  │
        └────────┬───────┘
                 │
    ┌────────────┼────────────┬──────────────┬───────────────┐
    ▼            ▼            ▼              ▼               ▼
 REORDER    EXTRA_ROWS   MISSING_ROWS   WRONG_VALUES    NULL_DIFF
    │            │            │              │               │
    ▼            ▼            ▼              ▼               ▼
 Fix final   Diagnose     Diagnose       Diagnose        Diagnose
 SELECT      filter/join  filter/join    expression      NULL handling
 projection  logic        logic          rewriting       differences
    │            │            │              │               │
    ▼            ▼            ▼              ▼               ▼
        ┌────────────────────────────────────────────────┐
        │           Generate Fix Hypothesis              │
        │  (which transformation rule is at fault?)      │
        └────────────────────┬───────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────────────┐
        │           Apply Fix to Decompiler Rule         │
        │           (update rule, add edge case)         │
        └────────────────────┬───────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────────────┐
        │           Re-run Validation on Affected Queries│
        │           (regression test)                    │
        └────────────────────────────────────────────────┘
```

### 5.2 Mismatch Classification

| Type | Symptom | Likely Root Cause | Fix Strategy |
|---|---|---|---|
| **REORDER** | Same rows, different column order | Final `\|> SELECT` doesn't match original column order | Adjust projection rule to preserve original SELECT order |
| **EXTRA_ROWS** | B has rows not in A | WHERE filter was dropped or weakened during transformation | Check WHERE promotion rule; verify HAVING→WHERE conversion |
| **MISSING_ROWS** | A has rows not in B | WHERE filter is too aggressive, or JOIN type changed | Check JOIN linearization (INNER vs LEFT); verify subquery unnesting |
| **WRONG_VALUES** | Same row count, different values | Expression rewriting error (e.g., aggregate alias mismatch, CASE transformation) | Diff the two SQLs at the expression level; identify which column differs |
| **NULL_DIFF** | Mismatch only in NULL-containing rows | NULL ordering difference, or LEFT JOIN → INNER JOIN conversion | Check JOIN types and NULL handling in WHERE conditions |
| **TYPE_DIFF** | Values are "equal" but different types (e.g., `1` vs `1.0`, `"1"` vs `1`) | Type coercion difference between original and CTE-based round-trip | Add type normalization in comparison or fix CAST in decompiler |
| **DUPLICATE_DIFF** | Set sizes differ but distinct values match | DISTINCT was added or dropped during transformation | Check DISTINCT handling in decompiler |

### 5.3 Automated Root Cause Analysis

For each mismatch, the system performs automated analysis:

```python
def diagnose_mismatch(
    gold_sql: str,
    pipe_sql: str,
    sqlite2_sql: str,
    result_a: List[Tuple],
    result_b: List[Tuple],
    db_path: str
) -> DiagnosticReport:
    report = DiagnosticReport()

    # 1. Column count check
    if len(result_a[0]) != len(result_b[0]):
        report.mismatch_type = "REORDER" if sorted(result_a[0]) == sorted(result_b[0]) else "COL_COUNT"
        report.fix_hint = "Adjust final |> SELECT projection"
        return report

    # 2. Row count check
    if len(result_a) != len(result_b):
        extra = set(result_b) - set(result_a)
        missing = set(result_a) - set(result_b)
        if extra and not missing:
            report.mismatch_type = "EXTRA_ROWS"
            report.fix_hint = "WHERE filter dropped or weakened"
        elif missing and not extra:
            report.mismatch_type = "MISSING_ROWS"
            report.fix_hint = "WHERE filter too aggressive or JOIN type changed"
        else:
            report.mismatch_type = "ROW_SWAP"
            report.fix_hint = "Both extra and missing rows — logic error in transformation"
        report.sample_extra = list(extra)[:5]
        report.sample_missing = list(missing)[:5]
        return report

    # 3. Value-level diff (same row count)
    # Sort both and compare position by position
    sorted_a = sorted(result_a)
    sorted_b = sorted(result_b)
    diff_positions = []
    for i, (ra, rb) in enumerate(zip(sorted_a, sorted_b)):
        if ra != rb:
            diff_positions.append((i, ra, rb))
    if diff_positions:
        # Check if it's a NULL issue
        null_diffs = [(i, a, b) for i, a, b in diff_positions
                      if any(v is None for v in a + b)]
        if len(null_diffs) == len(diff_positions):
            report.mismatch_type = "NULL_DIFF"
        else:
            report.mismatch_type = "WRONG_VALUES"
        report.sample_diffs = diff_positions[:5]

    # 4. AST diff between gold_sql and sqlite2_sql
    report.ast_diff = compute_ast_diff(gold_sql, sqlite2_sql)

    # 5. Identify which transformation rule likely caused the issue
    report.suspected_rule = identify_suspected_rule(report.ast_diff, report.mismatch_type)

    return report
```

### 5.4 AST Diff for Root Cause Identification

Compare the AST of the original `gold_sql` with the AST of `sqlite2_sql` (the round-tripped version) to pinpoint structural differences:

```python
def compute_ast_diff(sql_a: str, sql_b: str) -> List[str]:
    """Compute structural differences between two SQL ASTs."""
    import sqlglot

    ast_a = sqlglot.parse_one(sql_a, read="sqlite")
    ast_b = sqlglot.parse_one(sql_b, read="sqlite")

    diffs = []

    # Compare FROM clauses
    if ast_a.find(exp.From) and ast_b.find(exp.From):
        if ast_a.find(exp.From).sql() != ast_b.find(exp.From).sql():
            diffs.append(f"FROM changed: {ast_a.find(exp.From).sql()} → {ast_b.find(exp.From).sql()}")

    # Compare JOIN count and types
    joins_a = list(ast_a.find_all(exp.Join))
    joins_b = list(ast_b.find_all(exp.Join))
    if len(joins_a) != len(joins_b):
        diffs.append(f"JOIN count changed: {len(joins_a)} → {len(joins_b)}")
    for i, (ja, jb) in enumerate(zip(joins_a, joins_b)):
        if ja.side != jb.side or ja.kind != jb.kind:
            diffs.append(f"JOIN[{i}] type changed: {ja.side} {ja.kind} → {jb.side} {jb.kind}")

    # Compare WHERE conditions
    where_a = ast_a.find(exp.Where)
    where_b = ast_b.find(exp.Where)
    if (where_a is None) != (where_b is None):
        diffs.append(f"WHERE {'added' if where_b else 'dropped'}")
    elif where_a and where_b and where_a.sql() != where_b.sql():
        diffs.append(f"WHERE changed: {where_a.sql()} → {where_b.sql()}")

    # Compare GROUP BY
    group_a = ast_a.find(exp.Group)
    group_b = ast_b.find(exp.Group)
    if (group_a is None) != (group_b is None):
        diffs.append(f"GROUP BY {'added' if group_b else 'dropped'}")

    # Compare aggregate functions
    aggs_a = sorted(n.sql() for n in ast_a.find_all(exp.AggFunc))
    aggs_b = sorted(n.sql() for n in ast_b.find_all(exp.AggFunc))
    if aggs_a != aggs_b:
        diffs.append(f"Aggregates changed: {aggs_a} → {aggs_b}")

    # Compare SELECT expressions
    if isinstance(ast_a, exp.Select) and isinstance(ast_b, exp.Select):
        sels_a = [e.sql() for e in ast_a.expressions]
        sels_b = [e.sql() for e in ast_b.expressions]
        if sels_a != sels_b:
            diffs.append(f"SELECT changed: {sels_a} → {sels_b}")

    return diffs
```

### 5.5 Suspected Rule Identification

Map mismatch types to likely decompiler rules:

```python
RULE_SUSPECTS = {
    "REORDER":       ["projection_rule"],
    "EXTRA_ROWS":    ["where_rule", "aggregate_rule"],
    "MISSING_ROWS":  ["where_rule", "join_rule", "subquery_rule"],
    "WRONG_VALUES":  ["aggregate_rule", "window_rule", "projection_rule"],
    "NULL_DIFF":     ["join_rule", "where_rule"],
    "TYPE_DIFF":     ["projection_rule"],
    "DUPLICATE_DIFF": ["terminal_rule"],  # DISTINCT handling
    "COL_COUNT":     ["projection_rule", "aggregate_rule"],
}

def identify_suspected_rule(ast_diffs: List[str], mismatch_type: str) -> List[str]:
    suspects = list(RULE_SUSPECTS.get(mismatch_type, []))

    # Refine based on AST diffs
    for diff in ast_diffs:
        if "JOIN" in diff:
            suspects.insert(0, "join_rule")
        if "WHERE" in diff:
            suspects.insert(0, "where_rule")
        if "GROUP BY" in diff:
            suspects.insert(0, "aggregate_rule")
        if "Aggregates" in diff:
            suspects.insert(0, "aggregate_rule")

    return list(dict.fromkeys(suspects))  # deduplicate, preserve order
```

---

## 6. Error Triage (Execution Failures)

### 6.1 Error Categories

When `sqlite2_sql` fails to execute, classify the error:

| Error Category | Example Error Message | Root Cause | Fix Strategy |
|---|---|---|---|
| **SYNTAX** | `near "|>": syntax error` | Pipe syntax leaked into SQLite output (SQLGlot transpile failure) | Fix SQLGlot read/write dialect params |
| **NO_SUCH_TABLE** | `no such table: __tmp1` | CTE chain broken during transpilation | Ensure all CTEs are properly defined |
| **NO_SUCH_COLUMN** | `no such column: t1.name` | Column qualification error after pipe transformation | Fix qualify step or alias propagation |
| **NO_SUCH_FUNCTION** | `no such function: ARRAY_AGG` | BigQuery function leaked into SQLite output | Add function mapping or flag as unsupported |
| **AMBIGUOUS_COLUMN** | `ambiguous column name: id` | Self-join or multi-table query lost its aliases | Fix AS insertion and alias propagation |
| **TYPE_ERROR** | `cannot use aggregate in this context` | Aggregate/non-aggregate mixing in wrong position | Fix expression classification in aggregate rule |
| **TIMEOUT** | Execution exceeded 30s | Query produces cartesian product or infinite recursion | Flag as decompiler bug (likely missing JOIN condition). Use standard 30s timeout. |
| **PARSE_FAIL** | SQLGlot cannot parse gold SQL | Source SQL uses SQLite-specific syntax SQLGlot doesn't support | Log and skip; count toward coverage metric |

### 6.2 Error Handling Flow

```python
def execute_with_error_handling(sql: str, db_path: str, timeout: int = 30) -> ExecutionResult:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout = 30000")
        cursor = conn.cursor()

        # Execute with timeout
        result = cursor.execute(sql).fetchall()
        col_names = [desc[0] for desc in cursor.description] if cursor.description else []
        return ExecutionResult(success=True, rows=result, columns=col_names)

    except sqlite3.OperationalError as e:
        error_msg = str(e)
        if "no such table" in error_msg:
            return ExecutionResult(success=False, error_category="NO_SUCH_TABLE", error_msg=error_msg)
        elif "no such column" in error_msg:
            return ExecutionResult(success=False, error_category="NO_SUCH_COLUMN", error_msg=error_msg)
        elif "no such function" in error_msg:
            return ExecutionResult(success=False, error_category="NO_SUCH_FUNCTION", error_msg=error_msg)
        elif "ambiguous column" in error_msg:
            return ExecutionResult(success=False, error_category="AMBIGUOUS_COLUMN", error_msg=error_msg)
        elif "near" in error_msg:
            return ExecutionResult(success=False, error_category="SYNTAX", error_msg=error_msg)
        else:
            return ExecutionResult(success=False, error_category="OTHER", error_msg=error_msg)

    except Exception as e:
        return ExecutionResult(success=False, error_category="UNEXPECTED", error_msg=str(e))

    finally:
        conn.close()
```

---

## 7. Known SQLGlot Round-Trip Issues

These are confirmed issues in SQLGlot v29.x that will cause false mismatches. The validation loop must account for them.

### 7.1 Issues That Cause Silent Wrong Answers

| Issue | Description | Impact | Mitigation |
|---|---|---|---|
| `LEAST(a, b)` bug | 2-argument `LEAST(a, b)` drops the second argument, outputs just `a` | Wrong values in result | Patch SQLGlot or post-process: detect LEAST/GREATEST with 2 args and rewrite to `MIN(a, b)` / `MAX(a, b)` |
| `IGNORE NULLS` dropped | SQLGlot silently drops `IGNORE NULLS` from window functions | Wrong NULL handling | Flag queries using IGNORE NULLS as KNOWN_ISSUE |
| `SAFE_CAST` → `CAST` | Safety semantics lost; runtime errors instead of NULL | Execution error or wrong values | Flag SAFE_CAST queries as KNOWN_ISSUE |
| `GROUP_CONCAT ... ORDER BY` dropped | ORDER BY inside GROUP_CONCAT is silently removed | Different string concatenation order | Flag or rewrite to subquery-based ordering |

### 7.2 Issues That Cause Execution Errors

| Issue | Description | Impact | Mitigation |
|---|---|---|---|
| Pipe operators SET/DROP/RENAME/DISTINCT/CALL/WITH | Crash with TypeError in v29.x | Cannot use these operators in pipe output | Avoid these operators in decompiler output; use SELECT/WHERE alternatives |
| `EXCEPT ALL` / `INTERSECT ALL` | SQLite does not support `ALL` modifier | Runtime error | Convert to EXCEPT/INTERSECT (drop ALL if source has no duplicates) |
| `TABLESAMPLE` | Not supported in SQLite | Runtime error | Avoid TABLESAMPLE in pipe output for SQLite targets |

### 7.3 Issues That Cause Cosmetic Differences (Not Semantic)

| Issue | Description | Impact |
|---|---|---|
| `IFNULL` → `COALESCE` | Function renamed during round-trip | None (semantically identical in SQLite) |
| `SUBSTR` → `SUBSTRING` | Function renamed | None (SQLite accepts both) |
| Identifier quoting: `[col]` → `"col"` | Quote style normalized | None |

### 7.4 Pre-Validation Filters

Before comparing results, apply these filters to exclude known-problematic queries:

```python
KNOWN_ISSUE_PATTERNS = [
    (r'\bLEAST\s*\([^,]+,[^,]+\)', "LEAST_2ARG_BUG"),
    (r'\bGREATEST\s*\([^,]+,[^,]+\)', "GREATEST_2ARG_BUG"),
    (r'IGNORE\s+NULLS', "IGNORE_NULLS_DROPPED"),
    (r'SAFE_CAST', "SAFE_CAST_LOSSY"),
    (r'GROUP_CONCAT\s*\([^)]*ORDER\s+BY', "GROUP_CONCAT_ORDER_DROPPED"),
    (r'EXCEPT\s+ALL|INTERSECT\s+ALL', "SET_OP_ALL_UNSUPPORTED"),
    (r'TABLESAMPLE', "TABLESAMPLE_UNSUPPORTED"),
]

def check_known_issues(sql: str) -> List[str]:
    """Return list of known issue tags for a query."""
    import re
    issues = []
    for pattern, tag in KNOWN_ISSUE_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            issues.append(tag)
    return issues
```

Queries with known issues are tracked separately. They still go through the pipeline, but mismatches attributed to known issues are not counted against the decompiler's success rate.

---

## 8. Feedback Cycle: From Mismatch to Fix

### 8.1 The Iteration Loop

```
┌──────────────────────────────────────────────────────────────┐
│  ITERATION N                                                  │
│                                                               │
│  1. Run validation pipeline on all dev queries                │
│                                                               │
│  2. Collect results:                                          │
│     ├── MATCH:           1,200 queries ✓                      │
│     ├── MISMATCH:          150 queries ✗                      │
│     ├── ERROR:              80 queries ✗                      │
│     ├── DECOMPILE_FAIL:     84 queries ⊘                     │
│     └── KNOWN_ISSUE:        20 queries ~                      │
│                                                               │
│  3. Analyze MISMATCHes by type:                               │
│     ├── REORDER:            45 (fix: projection_rule)         │
│     ├── EXTRA_ROWS:         30 (fix: where_rule)              │
│     ├── MISSING_ROWS:       25 (fix: join_rule)               │
│     ├── WRONG_VALUES:       35 (fix: aggregate_rule)          │
│     └── NULL_DIFF:          15 (fix: join_rule)               │
│                                                               │
│  4. Prioritize fixes by impact (most affected queries first)  │
│                                                               │
│  5. Fix decompiler rule(s)                                    │
│                                                               │
│  6. Regression test: re-run on ALL dev queries                │
│     ├── Verify: previous MATCHes still MATCH                  │
│     └── Verify: targeted MISMATCHes now MATCH                 │
│                                                               │
│  7. If success rate < 95%: goto ITERATION N+1                 │
│     Else: proceed to train set production                     │
└──────────────────────────────────────────────────────────────┘
```

### 8.2 Fix Priority Ranking

Fixes are prioritized by:
1. **Blast radius**: How many queries are affected? Fix the rule that causes the most mismatches first.
2. **Severity**: WRONG_VALUES and MISSING_ROWS are more severe than REORDER.
3. **Fixability**: Some mismatches are caused by SQLGlot limitations (known issues) and cannot be fixed in the decompiler. Deprioritize these.

### 8.3 Regression Guard

Every decompiler change must pass the regression test:

```python
def regression_test(
    previous_results: Dict[int, str],  # question_id -> "MATCH"/"MISMATCH"/"ERROR"
    current_results: Dict[int, str],
) -> RegressionReport:
    regressions = []
    improvements = []

    for qid in previous_results:
        prev = previous_results[qid]
        curr = current_results[qid]

        if prev == "MATCH" and curr != "MATCH":
            regressions.append(qid)  # Previously passing, now failing
        elif prev != "MATCH" and curr == "MATCH":
            improvements.append(qid)  # Previously failing, now passing

    return RegressionReport(
        regressions=regressions,
        improvements=improvements,
        net_change=len(improvements) - len(regressions),
        accept=len(regressions) == 0,  # ZERO regressions policy
    )
```

**Policy**: A decompiler change is accepted only if it causes **zero regressions**. If a fix for one mismatch type causes regressions elsewhere, the fix must be refined.

---

## 9. Execution Harness

### 9.1 Main Entry Point

```python
import json
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
import sqlglot

@dataclass
class ValidationRecord:
    question_id: int
    db_id: str
    difficulty: str
    gold_sql: str
    pipe_sql: Optional[str] = None
    sqlite2_sql: Optional[str] = None
    outcome: str = ""                    # MATCH, MISMATCH, ERROR, DECOMPILE_FAIL, KNOWN_ISSUE
    mismatch_type: Optional[str] = None
    error_category: Optional[str] = None
    error_msg: Optional[str] = None
    known_issues: List[str] = field(default_factory=list)
    diagnostics: Optional[dict] = None
    result_a_rows: int = 0
    result_b_rows: int = 0

def normalize_entry(entry: dict, source: str = "spider") -> dict:
    """Normalize Spider or BIRD JSON entry to a common format."""
    if source == "spider":
        return {
            "question_id": entry.get("question_id", id(entry)),
            "db_id": entry["db_id"],
            "difficulty": entry.get("hardness", "unknown"),  # Spider uses "hardness"
            "gold_sql": entry["query"],                       # Spider uses "query"
        }
    else:  # bird
        return {
            "question_id": entry["question_id"],
            "db_id": entry["db_id"],
            "difficulty": entry.get("difficulty", "unknown"), # BIRD uses "difficulty"
            "gold_sql": entry["SQL"],                         # BIRD uses "SQL"
        }

def run_validation(
    dev_json_path: str,
    db_dir: str,
    decompiler,                          # The decompiler instance
    output_path: str,
    source: str = "spider",             # "spider" or "bird"
) -> Dict[str, int]:
    """Run the full validation pipeline on the benchmark dev set.

    Args:
        source: "spider" for Spider 1.0 format, "bird" for BIRD/BIRD Mini-Dev format.
    """

    with open(dev_json_path) as f:
        queries = json.load(f)

    records = []
    counters = {"MATCH": 0, "MISMATCH": 0, "ERROR": 0,
                "DECOMPILE_FAIL": 0, "KNOWN_ISSUE": 0, "TIMEOUT": 0}

    for raw_entry in queries:
        entry = normalize_entry(raw_entry, source)
        record = ValidationRecord(
            question_id=entry["question_id"],
            db_id=entry["db_id"],
            difficulty=entry["difficulty"],
            gold_sql=entry["gold_sql"],
        )

        db_path = Path(db_dir) / entry["db_id"] / f"{entry['db_id']}.sqlite"

        # Step 1: Check for known SQLGlot issues
        record.known_issues = check_known_issues(entry["gold_sql"])

        # Step 2: Execute gold SQL → Result Set A
        exec_a = execute_with_error_handling(entry["gold_sql"], str(db_path))
        if not exec_a.success:
            record.outcome = "ERROR"
            record.error_category = "GOLD_SQL_FAIL"
            record.error_msg = exec_a.error_msg
            records.append(record)
            counters["ERROR"] += 1
            continue
        record.result_a_rows = len(exec_a.rows)

        # Step 3: Decompile to pipe SQL
        try:
            result = decompiler.transform(entry["gold_sql"], dialect="sqlite")
            record.pipe_sql = result.pipe_sql
        except Exception as e:
            record.outcome = "DECOMPILE_FAIL"
            record.error_msg = str(e)
            records.append(record)
            counters["DECOMPILE_FAIL"] += 1
            continue

        # Step 4: Transpile pipe SQL back to SQLite
        try:
            sqlite2 = sqlglot.transpile(
                record.pipe_sql, read="bigquery", write="sqlite"
            )[0]
            record.sqlite2_sql = sqlite2
        except Exception as e:
            record.outcome = "ERROR"
            record.error_category = "TRANSPILE_FAIL"
            record.error_msg = str(e)
            records.append(record)
            counters["ERROR"] += 1
            continue

        # Step 5: Execute SQLite₂ → Result Set B
        exec_b = execute_with_error_handling(sqlite2, str(db_path))
        if not exec_b.success:
            record.outcome = "ERROR"
            record.error_category = exec_b.error_category
            record.error_msg = exec_b.error_msg
            records.append(record)
            counters["ERROR"] += 1
            continue
        record.result_b_rows = len(exec_b.rows)

        # Step 6: Compare results
        if compare_with_tolerance(exec_a.rows, exec_b.rows):
            record.outcome = "MATCH"
            counters["MATCH"] += 1
        else:
            if record.known_issues:
                record.outcome = "KNOWN_ISSUE"
                counters["KNOWN_ISSUE"] += 1
            else:
                record.outcome = "MISMATCH"
                record.diagnostics = asdict(diagnose_mismatch(
                    entry["SQL"], record.pipe_sql, sqlite2,
                    exec_a.rows, exec_b.rows, str(db_path)
                ))
                record.mismatch_type = record.diagnostics.get("mismatch_type")
                counters["MISMATCH"] += 1

        records.append(record)

    # Write detailed results
    with open(output_path, "w") as f:
        json.dump([asdict(r) for r in records], f, indent=2)

    # Print summary
    total = len(records)
    print(f"\n{'='*60}")
    print(f"Validation Results: {total} queries")
    print(f"{'='*60}")
    for outcome, count in sorted(counters.items()):
        pct = count / total * 100
        print(f"  {outcome:20s}: {count:5d} ({pct:5.1f}%)")
    match_rate = counters["MATCH"] / total * 100
    print(f"{'='*60}")
    print(f"  Match rate: {match_rate:.1f}%")
    effective = (counters["MATCH"] + counters["KNOWN_ISSUE"]) / total * 100
    print(f"  Effective rate (excl. known issues): {effective:.1f}%")

    return counters
```

### 9.2 Difficulty-Stratified Reporting

Report results stratified by difficulty:

```python
def print_stratified_report(records: List[ValidationRecord], source: str = "spider"):
    """Print difficulty-stratified results."""
    by_difficulty = defaultdict(lambda: {"total": 0, "match": 0})

    for r in records:
        by_difficulty[r.difficulty]["total"] += 1
        if r.outcome == "MATCH":
            by_difficulty[r.difficulty]["match"] += 1

    # Spider uses: easy, medium, hard, extra
    # BIRD uses: simple, moderate, challenging
    if source == "spider":
        levels = ["easy", "medium", "hard", "extra"]
    else:
        levels = ["simple", "moderate", "challenging"]

    header = f"{'':15s}"
    for d in levels:
        header += f" {d:>12s}"
    header += f" {'total':>10s}"
    print(f"\n{header}")
    print("-" * (15 + 12 * len(levels) + 10))

    totals = {"total": 0, "match": 0}
    row = f"{'count':15s}"
    for d in levels:
        row += f" {by_difficulty[d]['total']:12d}"
        totals["total"] += by_difficulty[d]["total"]
        totals["match"] += by_difficulty[d]["match"]
    row += f" {totals['total']:10d}"
    print(row)

    row = f"{'match rate':15s}"
    for d in levels:
        rate = by_difficulty[d]["match"] / max(by_difficulty[d]["total"], 1) * 100
        row += f" {rate:11.1f}%"
    overall = totals["match"] / max(totals["total"], 1) * 100
    row += f" {overall:9.1f}%"
    print(row)
```

---

## 10. Intermediate Validation: Pipe SQL Syntax Check

Before transpiling pipe SQL back to SQLite, verify the pipe SQL is syntactically valid:

```python
def validate_pipe_syntax(pipe_sql: str) -> Tuple[bool, Optional[str]]:
    """Verify pipe SQL parses without error."""
    try:
        ast = sqlglot.parse_one(pipe_sql, read="bigquery")
        # Verify it round-trips to valid SQL
        standard = ast.sql(dialect="bigquery")
        return True, None
    except sqlglot.errors.ParseError as e:
        return False, str(e)
```

This catches decompiler bugs that produce syntactically invalid pipe SQL before they reach the execution stage. Syntax errors are cheaper to diagnose than execution mismatches.

### 10.1 Prefix Validation

Exploit the Prefix Property: every prefix of a valid pipe query (up to a `|>` boundary) is itself a valid query. Validate each prefix independently:

```python
def validate_pipe_prefixes(pipe_sql: str) -> List[Tuple[int, bool, Optional[str]]]:
    """Validate each prefix of the pipe query."""
    lines = pipe_sql.strip().split("\n")
    results = []

    prefix = ""
    for i, line in enumerate(lines):
        if i == 0:
            prefix = line
        else:
            prefix += "\n" + line

        valid, error = validate_pipe_syntax(prefix)
        results.append((i, valid, error))

        if not valid:
            break  # First invalid prefix identifies the broken operator

    return results
```

If prefix N is valid but prefix N+1 is not, the bug is in the Nth pipe operator — providing precise localization for debugging.

---

## 11. Output Artifacts

### 11.1 Per-Iteration Output

Each validation run produces:

```
validation_output/
├── iteration_001/
│   ├── results.json          # Full per-query results (ValidationRecord array)
│   ├── summary.txt           # Stratified match rates
│   ├── mismatches.json       # Only MISMATCH records with diagnostics
│   ├── errors.json           # Only ERROR records with error details
│   ├── decompile_fails.json  # Only DECOMPILE_FAIL records
│   └── golden_pairs.jsonl    # Successfully validated (question, pipe_sql) pairs
├── iteration_002/
│   ├── ...
│   └── regression_report.txt # Comparison with iteration_001
└── ...
```

### 11.2 Golden Corpus Output

Queries that achieve MATCH are exported as training data:

```json
{
  "question_id": 7,
  "db_id": "california_schools",
  "question": "What is the phone number of the school that has the highest number of test takers with an SAT score of over 1500?",
  "evidence": "",
  "gold_sql": "SELECT T2.Phone FROM satscores AS T1 INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode ORDER BY T1.NumGE1500 DESC LIMIT 1",
  "pipe_sql": "FROM satscores AS T1\n|> JOIN schools AS T2 ON T1.cds = T2.CDSCode\n|> ORDER BY T1.NumGE1500 DESC\n|> LIMIT 1\n|> SELECT T2.Phone",
  "difficulty": "simple",
  "validation": "MATCH"
}
```

---

## 12. Success Criteria

| Metric | Target | Measured On |
|---|---|---|
| **Match rate** (strict) | ≥ 95% of decompilable queries | Spider dev (1,034 queries) |
| **Decompile success rate** | ≥ 90% of all queries | Spider dev |
| **Effective rate** (excl. known issues + decompile fails) | ≥ 97% | Successfully decompiled queries |
| **Zero regressions per iteration** | 100% | Previous MATCH queries remain MATCH |
| **Error rate** (execution failures) | ≤ 3% of decompilable queries | Spider dev |
| **Match rate by difficulty** | easy ≥ 99%, medium ≥ 97%, hard ≥ 93%, extra hard ≥ 88% | Spider dev |
| **Tier 2 stress test** | ≥ 90% match rate | BIRD Mini-Dev (500 queries) |

### 12.1 Graduation Criteria

**Tier 1 graduation** (Spider) — the validation loop advances to Tier 2 when:
1. Match rate ≥ 95% on Spider dev set (1,034 queries).
2. All MISMATCH records have been either fixed or classified as KNOWN_ISSUE.
3. The decompiler has been stable (no regressions) for 2 consecutive iterations.
4. The golden corpus contains ≥ 900 validated pipe SQL queries from Spider dev.

**Tier 2 graduation** (BIRD Mini-Dev) — production readiness when:
1. Match rate ≥ 90% on BIRD Mini-Dev (500 queries).
2. No new categories of MISMATCH discovered (all mismatch types already seen in Tier 1).
3. Any BIRD-specific edge cases have been fixed without regressing Spider results.

At Tier 2 graduation, the decompiler is applied to Spider train (8,659 queries) and then full BIRD train (9,428 queries, 33.4 GB download) for production corpus generation.

---

## 13. Implementation Roadmap

### Week 1: Harness Setup
- Download Spider 1.0 (~1 GB: dev.json + database/)
- Implement execution harness (`run_validation`)
- Implement result comparison logic (set-based + tolerance)
- Run baseline: gold SQL → parse → generate SQLite (no pipe) → execute → compare
- This baseline measures SQLGlot's round-trip fidelity before the decompiler is involved

### Week 2: Decompiler Integration
- Connect the decompiler (from companion design doc) to the validation harness
- Run first full iteration on Spider dev (1,034 queries)
- Implement diagnostic feedback (mismatch classification, AST diff)
- Triage all errors and mismatches from iteration 1

### Week 3–4: Fix Cycle (Tier 1)
- Fix decompiler rules based on mismatch diagnostics
- Run iterations 2–N with regression testing
- Target: 80% → 90% → 95% match rate on Spider dev

### Week 5: Tier 2 Stress Test
- Download BIRD Mini-Dev (500 queries, 11 databases)
- Run decompiler on BIRD Mini-Dev
- Fix any new edge cases discovered
- Target: ≥ 90% match rate on BIRD Mini-Dev

### Week 6: Production
- Run decompiler on Spider train set (8,659 queries) — generate first golden corpus
- Download full BIRD train set (9,428 queries, 33.4 GB) — generate extended corpus
- Feed into trajectory decomposition pipeline (from fine-tuning design doc, Section 7)
