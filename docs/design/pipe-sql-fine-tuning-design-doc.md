# Design Document: Large-Scale Incremental Pipe SQL Synthesis & Specialized Fine-Tuning for Small Language Models (1.5B–7B)

## 1. Executive Summary

Pipe SQL syntax — where queries are written as linear chains of `|>` operators rather than nested, inside-out clause blocks — is now production-ready in BigQuery (GA February 2025), Apache Spark 4.0+, and Databricks Runtime 16.2+. This linear structure maps naturally onto autoregressive token generation, making it a compelling target for specialized Text-to-SQL models.

This document presents a complete system for:

1. **Synthesizing a large corpus of semantically validated pipe SQL queries** from existing standard SQL benchmarks via a custom AST-based decompiler.
2. **Fine-tuning small language models (1.5B–7B)** using an incremental, pipe-by-pipe training strategy that teaches models to build queries step-by-step rather than emit them in one shot.

The core thesis: by converting the Text-to-SQL problem from "generate a complex nested structure" into "append the next correct pipe operator," we can train models at a fraction of frontier cost while achieving competitive execution accuracy.

---

## 2. Why Pipe SQL?

### 2.1 The Problem with Standard SQL Generation

Standard SQL requires the model to produce clauses in an order that is the inverse of logical execution:

```sql
SELECT department, AVG(salary) AS avg_sal    -- Step 4: project
FROM employees                                -- Step 1: scan
WHERE hire_date > '2020-01-01'                -- Step 2: filter
GROUP BY department                           -- Step 3: aggregate
HAVING AVG(salary) > 80000                    -- Step 5: post-agg filter
ORDER BY avg_sal DESC                         -- Step 6: sort
```

An LLM generating this left-to-right must write `SELECT department, AVG(salary)` before it has committed to the `WHERE` filter or `GROUP BY` clause. This inversion causes three well-documented failure modes:

- **Schema hallucination**: referencing columns eliminated by an earlier aggregation or join that the model hasn't generated yet.
- **Alias invisibility**: standard SQL forbids referencing a `SELECT` alias in the `WHERE` clause of the same block, leading to invalid queries.
- **Nesting complexity**: correlated subqueries and multi-level CTEs require the model to maintain deep structural state across its entire generation window.

### 2.2 How Pipe Syntax Solves This

Pipe syntax linearizes the query into a sequence of transformations, each consuming the output of the previous step:

```sql
FROM employees
|> WHERE hire_date > '2020-01-01'
|> AGGREGATE AVG(salary) AS avg_sal GROUP BY department
|> WHERE avg_sal > 80000
|> ORDER BY avg_sal DESC
```

This provides three structural guarantees that directly benefit autoregressive generation:

1. **The Prefix Property**: every prefix up to any `|>` boundary is a valid, independently executable query. The model never needs to look ahead.
2. **Local scope**: each operator only sees columns produced by its immediate predecessor. The active schema is always well-defined and narrow.
3. **Linear data flow**: no nesting, no back-references. The generation order matches the logical execution order.

### 2.3 Supported Pipe Operators

The following operators are supported across BigQuery and Spark, which are the primary target engines:

| Operator | Purpose | Replaces (Standard SQL) |
|---|---|---|
| `FROM table` | Entry point | `FROM` clause |
| `\|> SELECT` | Column projection | `SELECT` |
| `\|> EXTEND expr AS alias` | Add computed column (preserves existing) | Inline expression in `SELECT` |
| `\|> WHERE` | Row filtering at any point in the pipeline | `WHERE`, `HAVING`, `QUALIFY` |
| `\|> AGGREGATE expr GROUP BY cols` | Aggregation | `SELECT ... GROUP BY` |
| `\|> JOIN table ON cond` | Join (all types: INNER, LEFT, etc.) | `JOIN` clause |
| `\|> ORDER BY` | Sorting | `ORDER BY` |
| `\|> LIMIT n` | Row limiting | `LIMIT` |
| `\|> SET col = expr` | Replace column values | N/A (new) |
| `\|> DROP col` | Remove columns | N/A (new) |
| `\|> RENAME old AS new` | Rename columns | `AS` alias |
| `\|> DISTINCT` | Deduplicate | `SELECT DISTINCT` |

---

## 3. Pipe Query Syntax Landscape

Multiple pipe-like query systems exist. This section surveys the landscape to justify our choice of GoogleSQL pipe syntax as the canonical training target.

### 3.1 The Same Query in Every Syntax

> "From employees, filter Chicago office, average salary by department, keep departments with avg > 80K, sort descending."

**Standard SQL:**
```sql
SELECT department, AVG(salary) AS avg_salary
FROM employees
WHERE office = 'Chicago'
GROUP BY department
HAVING AVG(salary) > 80000
ORDER BY avg_salary DESC;
```

**GoogleSQL Pipe (BigQuery / Spark):**
```sql
FROM employees
|> WHERE office = 'Chicago'
|> AGGREGATE AVG(salary) AS avg_salary GROUP BY department
|> WHERE avg_salary > 80000
|> ORDER BY avg_salary DESC;
```

**Snowflake Flow (`->>`):**
```sql
SELECT * FROM employees WHERE office = 'Chicago'
->> SELECT department, AVG(salary) AS avg_salary FROM $1 GROUP BY department
->> SELECT * FROM $2 WHERE avg_salary > 80000
->> SELECT * FROM $3 ORDER BY avg_salary DESC;
```

**PRQL:**
```prql
from employees
filter office == 'Chicago'
group {department} (
  aggregate { avg_salary = average salary }
)
filter avg_salary > 80000
sort {-avg_salary}
```

**KQL (Kusto):**
```kql
employees
| where office == 'Chicago'
| summarize avg_salary = avg(salary) by department
| where avg_salary > 80000
| sort by avg_salary desc
```

**Malloy:**
```malloy
run: duckdb.table('employees') -> {
  where: office = 'Chicago'
  group_by: department
  aggregate: avg_salary is avg(salary)
} -> {
  where: avg_salary > 80000
  order_by: avg_salary desc
}
```

**dplyr (R):**
```r
employees |>
  filter(office == "Chicago") |>
  group_by(department) |>
  summarize(avg_salary = mean(salary)) |>
  filter(avg_salary > 80000) |>
  arrange(desc(avg_salary))
```

**Polars (Python):**
```python
(
    pl.scan_csv("employees.csv")
    .filter(pl.col("office") == "Chicago")
    .group_by("department")
    .agg(pl.col("salary").mean().alias("avg_salary"))
    .filter(pl.col("avg_salary") > 80000)
    .sort("avg_salary", descending=True)
    .collect()
)
```

### 3.2 Comparison Matrix

| System | Pipe Symbol | SQL-Compatible? | Compiles to SQL? | Native Engine | Adoption |
|---|---|---|---|---|---|
| **GoogleSQL Pipe** | `\|>` | Yes (extension) | N/A (IS SQL) | BigQuery, Spark, Databricks | Very High |
| **Snowflake Flow** | `->>` | Yes (chains full stmts) | N/A (IS SQL) | Snowflake | High |
| **KQL (Kusto)** | `\|` | No (separate language) | No | Azure Data Explorer | Very High |
| **PRQL** | Newlines | No (separate language) | Yes | None (compiler only) | Medium |
| **Malloy** | `->` | No (separate language) | Yes | None (compiles to SQL) | Low-Medium |
| **dplyr (R)** | `%>%` / `\|>` | No (R code) | Yes (via dbplyr) | R in-memory | Very High |
| **Polars** | `.method()` | No (Python) | No | Rust engine | High |
| **Logica** | N/A (predicates) | No (logic language) | Yes | None (compiler only) | Low |

### 3.3 Key Distinctions

**GoogleSQL Pipe vs. Snowflake Flow**: Snowflake's `->>` chains *entire SQL statements* together, referencing prior results via positional `$1`, `$2` parameters. GoogleSQL pipes *individual operators* within a single query. Snowflake's approach is more like Unix pipes between programs; GoogleSQL's is like method chaining within a single program.

**GoogleSQL Pipe vs. KQL**: KQL (Microsoft Azure Data Explorer) is the spiritual predecessor — its `| where`, `| summarize`, `| extend` operators clearly inspired GoogleSQL pipe syntax. However, KQL is an entirely separate language that only runs on Kusto-engine databases. GoogleSQL pipe syntax stays within SQL itself.

**GoogleSQL Pipe vs. PRQL / Malloy**: Both are full language replacements that compile *down to* SQL. They offer cleaner syntax but require a compilation step and have no native engine support (except ClickHouse's experimental PRQL support). GoogleSQL pipe syntax is SQL — it runs natively without compilation.

**GoogleSQL Pipe vs. dplyr / Polars**: These are DataFrame API approaches in R and Python respectively. They solve the same readability problem at the programming language level. The Databricks team explicitly cited DataFrame APIs as inspiration for pipe SQL.

### 3.4 BigQuery vs. Spark: Detailed Differences

While BigQuery and Spark share the same `|>` symbol and core operators, they diverge in several ways:

**Operators only in BigQuery:**

| Operator | Purpose |
|---|---|
| `\|> RENAME` | Rename columns directly |
| `\|> CALL` | Invoke table-valued functions in the pipe chain |
| `\|> WITH` | Inline CTEs within the pipe |
| `\|> WINDOW` | Standalone window function operator (deprecated in favor of EXTEND) |
| `\|> MATCH_RECOGNIZE` | Pattern matching on row sequences |
| `GROUP AND ORDER BY` | Shorthand inside AGGREGATE that also orders the output |

**Features only in Spark:**

| Feature | Purpose |
|---|---|
| `SEMI JOIN` / `ANTI JOIN` | Explicit semi/anti join keywords (BigQuery requires WHERE EXISTS) |
| `NATURAL JOIN` / `LATERAL JOIN` | Additional join types |
| Standalone `OFFSET` | OFFSET without requiring LIMIT (BigQuery requires LIMIT before OFFSET) |

**Behavioral differences:**

| Behavior | BigQuery | Spark |
|---|---|---|
| Lateral references in EXTEND | Allowed (later column can reference earlier alias in same EXTEND) | Not allowed (each projection is independent) |
| Default NULL ordering (ASC) | NULLs first | NULLs last |
| Default NULL ordering (DESC) | NULLs last | NULLs first |

### 3.5 Why GoogleSQL Pipe Syntax Is the Training Target

1. **It IS SQL.** Unlike PRQL, KQL, or Malloy, GoogleSQL pipe syntax requires no compilation step. The generated output is directly executable.
2. **Multi-engine support.** BigQuery, Spark, and Databricks all support it natively. No other pipe syntax has this breadth.
3. **BigQuery is a superset.** Training on BigQuery's dialect covers all Spark operators. The only gap is Spark's `SEMI`/`ANTI`/`NATURAL`/`LATERAL` join types, which can be expressed differently in BigQuery syntax.
4. **SQLGlot round-trip.** SQLGlot can parse GoogleSQL pipe syntax back to standard SQL for any of its 30+ supported dialects. This enables the dual-execution validation loop (Section 6) and deployment-time transpilation to any target database (Section 12.2).
5. **Pretraining signal.** BigQuery has been GA since February 2025. Frontier LLMs trained after this date will have GoogleSQL pipe syntax in their training data, providing a foundation for fine-tuning.

---

## 4. The Data Problem: Why a Decompiler Is Essential

### 4.1 The Fine-Tuning Data Bottleneck

Fine-tuning a language model to generate pipe SQL requires a large corpus of (natural language question, database schema, pipe SQL query) triples that are **semantically correct** — meaning they return the right answer on the target database. The challenge:

- **No pipe SQL training data exists.** All major Text-to-SQL benchmarks (Spider, BIRD-SQL, WikiSQL, KaggleDBQA) contain exclusively standard SQL. Combined, Spider 1.0 (~7K train) and BIRD-SQL (~9.4K train) provide roughly 16K standard SQL queries — zero in pipe syntax.
- **LLM-based generation is unreliable.** Using a frontier model (GPT-4o, Claude) to generate pipe SQL has three problems: (a) high cost at scale, (b) the model has limited pipe SQL in its training data, and (c) there is no efficient way to validate correctness without executing every generated query.
- **Manual annotation is infeasible.** Writing thousands of pipe SQL queries by hand is prohibitively expensive and error-prone.

### 4.2 The Decompiler Solution

A **decompiler** is a deterministic program that transforms standard SQL (which we have in abundance and with verified correctness) into semantically equivalent pipe SQL. This is the only approach that simultaneously satisfies all three requirements for training data:

| Requirement | LLM Generation | Manual Writing | Decompiler |
|---|---|---|---|
| Scale (50K+ queries) | Expensive ($$$) | Infeasible | Free (compute only) |
| Correctness guarantee | No (needs validation) | Error-prone | Deterministic (provable) |
| Reproducibility | Non-deterministic | N/A | Fully reproducible |
| Speed | ~1 query/sec | ~5 min/query | ~1000 queries/sec |

The decompiler takes a known-correct standard SQL query from an established benchmark, parses it into an AST, and mechanically transforms it into an equivalent pipe SQL query. Because the transformation is rule-based and structure-preserving, the output is guaranteed to be semantically equivalent to the input — no execution-based validation is strictly required (though we perform it anyway as a safety net).

### 4.3 Why Not Just Use Standard SQL for Training?

One might ask: why not fine-tune on standard SQL and transpile at inference time? Because the entire point is to exploit the structural advantages of pipe syntax during generation. A model trained on standard SQL still suffers from the nesting and inversion problems described in Section 2.1. The model must learn to *think* in pipes — to decompose a question into a linear sequence of transformations — and this requires training on pipe SQL directly.

### 4.4 Data Augmentation Strategy

Starting from the ~16K seed queries in Spider 1.0 + BIRD-SQL:

1. **Decompile all seed queries** → ~16K pipe SQL equivalents
2. **Schema-aware augmentation**: for each query, generate variants by substituting table/column names from the same schema family (e.g., swap `employees.salary` for `staff.compensation`)
3. **Complexity augmentation**: compose simple queries into multi-step pipes (e.g., combine a filter query and an aggregation query into a single pipeline)
4. **Synthetic NL paraphrasing**: use a language model to rephrase the natural language question while keeping the SQL unchanged

Target: **50K–100K** validated pipe SQL training pairs.

---

## 5. Decompiler Architecture

### 5.1 Why SQLGlot (and Its Limitations)

SQLGlot is the best available open-source SQL parser/transpiler, supporting 30+ dialects and providing a rich AST. However, its pipe syntax support is **one-directional only**:

- **Pipe → Standard**: SQLGlot can parse pipe syntax and decompose it into CTE-based standard SQL. This works.
- **Standard → Pipe**: SQLGlot has **no generator** for pipe syntax output. Pipe nodes are destroyed at parse time and replaced with CTEs. There is no `to_pipe()` method, no pipe expression nodes in the AST, and no reverse transformation.

Therefore, we must build a custom decompiler on top of SQLGlot's parser and AST infrastructure. SQLGlot provides the parsing, qualification, and optimization layers; we add the pipe emission layer.

### 5.2 Transformation Pipeline

```
Standard SQL (string)
    │
    ▼
[SQLGlot Parse] ──► AST (language-agnostic)
    │
    ▼
[SQLGlot Qualify] ──► Fully qualified AST
    │                  (all columns resolved to table.column,
    │                   all aliases expanded, star expressions resolved)
    │
    ▼
[Custom Pipe Emitter] ──► Pipe SQL (string)
```

### 5.3 Pipe Emitter Transformation Rules

The custom emitter walks the qualified AST and applies the following rules in order:

**Rule 1: FROM extraction**
```
SELECT ... FROM table_expr → FROM table_expr
```
Extract the `FROM` clause as the pipe entry point. If the `FROM` contains joins, emit them as separate `|> JOIN` operators.

**Rule 2: JOIN linearization**
```
FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
→
FROM a
|> JOIN b ON a.id = b.id
|> JOIN c ON b.id = c.id
```

**Rule 3: WHERE promotion**
```
WHERE condition → |> WHERE condition
```
Placed immediately after all `FROM`/`JOIN` operators.

**Rule 4: GROUP BY + aggregation fusion**
```
SELECT col, AGG(expr) ... GROUP BY col
→
|> AGGREGATE AGG(expr) AS alias GROUP BY col
```
The `SELECT` list is decomposed: aggregate expressions go into `|> AGGREGATE`, non-aggregate computed expressions become `|> EXTEND` operators placed before the aggregation.

**Rule 5: HAVING → post-aggregation WHERE**
```
HAVING AGG(x) > threshold
→
|> WHERE agg_alias > threshold
```
Since `|> AGGREGATE` produces named output columns, the `HAVING` condition is rewritten to reference those aliases.

**Rule 6: Window function extraction**
```
SELECT ..., ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn
→
|> EXTEND ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn
```
Window functions are emitted as `|> EXTEND` operators after all filtering and aggregation.

**Rule 7: QUALIFY → post-window WHERE**
```
QUALIFY rn = 1 → |> WHERE rn = 1
```

**Rule 8: ORDER BY / LIMIT passthrough**
```
ORDER BY col DESC LIMIT 10
→
|> ORDER BY col DESC
|> LIMIT 10
```

**Rule 9: Subquery unrolling**
Correlated and non-correlated subqueries in `WHERE` or `SELECT` are "unrolled" into preceding pipe segments. Scalar subqueries become `|> JOIN` + `|> EXTEND` patterns. `EXISTS`/`IN` subqueries become `|> JOIN` (semi-join) patterns.

**Rule 10: CTE inlining**
CTEs are unrolled into the main pipeline. Each CTE becomes a named sub-pipeline that feeds into the final query via `|> JOIN` or direct substitution.

### 5.4 Handling Ambiguity and Edge Cases

Not every standard SQL query maps cleanly to a single pipe representation. The emitter follows a **canonical ordering** convention:

```
FROM → JOINs → WHERE (pre-agg) → EXTEND (computed cols) →
AGGREGATE → WHERE (post-agg) → EXTEND (windows) →
WHERE (post-window) → SELECT (final projection) →
ORDER BY → LIMIT
```

When multiple valid orderings exist, the canonical order ensures deterministic output, which is critical for training data consistency.

---

## 6. Semantic Validation: The Dual-Execution Loop

Even though the decompiler is deterministic, bugs in transformation rules can silently produce semantically incorrect pipe SQL. We validate every synthesized query through dual execution:

```
                    ┌──────────────────────┐
                    │  Standard SQL (gold) │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼───────────┐
                    │   Execute on DB     │──► Result Set A
                    └─────────────────────┘

                    ┌──────────────────────┐
                    │   Pipe SQL (synth)   │
                    └─────────┬────────────┘
                              │
               ┌──────────────▼───────────────┐
               │  Transpile back to standard  │
               │  SQL via SQLGlot             │
               │  (pipe→standard is supported)│
               └──────────────┬───────────────┘
                              │
                    ┌─────────▼───────────┐
                    │   Execute on DB     │──► Result Set B
                    └─────────────────────┘

                    Result Set A == Result Set B?
```

**Comparison method**:
- Both result sets are loaded into DataFrames.
- Rows are sorted deterministically (by all columns) to eliminate non-deterministic ordering.
- Column types are coerced to common types (e.g., `DECIMAL` vs. `FLOAT` tolerance).
- For large results (>100K rows), row-level SHA-256 hashes are aggregated for constant-time comparison.

Queries that fail validation are quarantined with diagnostic metadata (AST diff, execution error) for manual review and decompiler rule refinement.

---

## 7. Incremental Training Strategy

### 7.1 Why Incremental?

Standard fine-tuning teaches the model to emit the entire query in one shot. This wastes the structural advantage of pipe syntax. Instead, we train the model to generate one pipe operator at a time, conditioned on the growing prefix. This mirrors how a human analyst would build a query: start with the data source, filter, transform, aggregate, filter again.

### 7.2 Trajectory Decomposition

Each N-operator pipe query is decomposed into N training samples:

**Example**: "Which departments in the Chicago office have average salaries above $80K?"

| Step | Input (prompt) | Output (completion) |
|---|---|---|
| 1 | Question + Schema | `FROM employees` |
| 2 | Question + Schema + `FROM employees` | `\|> WHERE office = 'Chicago'` |
| 3 | Question + Schema + prefix(1–2) | `\|> AGGREGATE AVG(salary) AS avg_sal GROUP BY department` |
| 4 | Question + Schema + prefix(1–3) | `\|> WHERE avg_sal > 80000` |
| 5 | Question + Schema + prefix(1–4) | `\|> SELECT department, avg_sal` |

This produces a 5:1 amplification of training data: a single pipe query yields 5 supervised examples. For 50K pipe queries with an average of 4 operators each, this produces **~200K training samples**.

### 7.3 Dataset Format (JSONL)

```json
{
  "messages": [
    {
      "role": "system",
      "content": "You are a SQL assistant that builds pipe SQL queries incrementally. Given a question, schema, and the query built so far, emit only the next pipe operator."
    },
    {
      "role": "user",
      "content": "Question: Which departments have avg salary > 80K?\nSchema: employees(id INT, name TEXT, department TEXT, salary DECIMAL, office TEXT)\nQuery so far: FROM employees\n|> WHERE office = 'Chicago'"
    },
    {
      "role": "assistant",
      "content": "|> AGGREGATE AVG(salary) AS avg_sal GROUP BY department"
    }
  ]
}
```

### 7.4 Prefix Executability as a Training Signal

Because every prefix is independently executable (the Prefix Property), we can verify each intermediate step during data generation. If step K's prefix doesn't execute successfully against the database, the entire trajectory is flagged. This catches decompiler errors that might only manifest mid-pipeline.

---

## 8. Fine-Tuning Configuration

### 8.1 Base Model Selection

| Model | Parameters | Why |
|---|---|---|
| **Qwen-2.5-Coder-7B** (primary) | 7.6B | 82.0 on Spider (standard SQL); strong code/SQL pretraining; 128K context |
| **Llama-3.2-3B** (lightweight) | 3.2B | Cost-efficient inference; suitable for simpler schemas; 128K context |
| **Qwen-2.5-Coder-1.5B** (edge) | 1.5B | Speculative decoding draft model; edge deployment |

Qwen-2.5-Coder-7B is the primary target because it already has strong SQL capabilities from pretraining, meaning fewer training steps are needed to adapt it to pipe syntax.

### 8.2 Training Parameters

| Parameter | Value | Rationale |
|---|---|---|
| Method | QLoRA (4-bit quantization) | 7B model fits in ~11GB VRAM on RTX 4090 (24GB) |
| LoRA rank | 64–128 | Pipe SQL is a novel syntax underrepresented in pretraining; higher rank captures structural patterns better |
| LoRA alpha | 2× rank | Standard scaling rule |
| LoRA target modules | `q_proj`, `k_proj`, `v_proj`, `o_proj`, `gate_proj`, `up_proj`, `down_proj` | Full attention + MLP adaptation |
| Learning rate | 1e-4 (with cosine decay) | Standard for QLoRA |
| Batch size | 8 (effective, with gradient accumulation) | Balance between stability and speed |
| Context window | 4096 tokens | Accommodates schema (1–3K tokens) + prefix + next operator |
| Epochs | 3–5 | Monitor validation loss; early stop on plateau |
| Warmup | 5% of total steps | Prevent early divergence |

### 8.3 Loss Masking

Only the assistant completion (the next pipe operator) contributes to the loss. The system prompt, user message (question + schema + prefix), and any padding tokens are masked with label ID `-100`.

---

## 9. Agentic Inference: Tool Calls Between Pipes

At inference time, the model operates within an agent loop. After generating each pipe operator, external tools are invoked to ground the next generation step in reality.

### 9.1 Tool 1: Schema Propagation (Dry Run)

**When**: After every operator.
**How**: Execute `DESCRIBE` or a dry-run API call on the current prefix.
**Returns**: The list of columns and their types available for the next operator.
**Why**: Prevents the model from referencing columns that were dropped by an earlier `|> AGGREGATE` or `|> SELECT`. This is the single most impactful tool for preventing schema hallucination.

### 9.2 Tool 2: Sample Rows

**When**: After `|> WHERE` and `|> JOIN` operators (where data content matters).
**How**: Execute `[prefix] |> LIMIT 5` against the database.
**Returns**: 5 rows of actual data.
**Why**: Lets the model verify literal values (e.g., is it `'Furniture'` or `'FURNITURE'`?), date formats, and NULL patterns. Eliminates constant hallucination.

### 9.3 Tool 3: Syntax Validation

**When**: After generating any operator candidate.
**How**: Parse `[prefix] |> [candidate]` with SQLGlot.
**Returns**: Success or error with location.
**Why**: Catches syntax errors (missing commas, invalid keywords) before the model proceeds, avoiding cascading errors in subsequent operators.

### 9.4 Agent Loop

```
Input: (question, schema)
prefix = ""

while not DONE:
    candidate = model.generate(question, schema, active_columns, prefix)

    if candidate == "<END>":
        break

    if not syntax_valid(prefix + candidate):
        candidate = model.retry(question, schema, prefix, error_msg)

    prefix = prefix + "\n" + candidate
    active_columns = dry_run(prefix)

    if needs_data_grounding(candidate):
        sample = execute(prefix + " |> LIMIT 5")
        # Feed sample into next generation context

return prefix
```

---

## 10. Post-SFT Reinforcement: Group Relative Policy Optimization (GRPO)

After supervised fine-tuning, we apply GRPO to improve reasoning quality. GRPO (introduced in DeepSeek-Math) eliminates the need for a separate critic model by using group-level baselines, making it practical for small-scale training.

### 10.1 Reward Signals

For each generated pipe query, compute a composite reward:

| Signal | Type | Weight | Description |
|---|---|---|---|
| **Execution** | Binary (0/1) | 0.3 | Does the complete query execute without error? |
| **Result correctness** | Continuous (0–1) | 0.5 | Does the result match the gold standard? (F1 over result set rows) |
| **Schema adherence** | Binary (0/1) | 0.1 | Does every referenced column exist in the active schema at that pipe stage? |
| **Operator structure** | Continuous (0–1) | 0.1 | Does the query use the expected operator types? (e.g., question implies aggregation → model uses `\|> AGGREGATE`) |

### 10.2 GRPO Procedure

1. For each training prompt (question + schema), generate K=8 complete pipe queries using the SFT model.
2. Score each query using the reward signals above.
3. Compute the group mean and standard deviation of rewards.
4. For each query, compute advantage = (reward - group_mean) / group_std.
5. Update the policy to increase the probability of above-average completions and decrease below-average ones.

This is particularly effective for Text-to-SQL because the reward signal (execution correctness) is cheap to compute — just run the query.

---

## 11. Performance Targets

### 11.1 Realistic Benchmarks

Performance targets are calibrated against published results as of early 2026:

| Benchmark | Base 7B (standard SQL) | Specialized Pipe 7B (target) | Current 7B SOTA (standard SQL) | Frontier model reference |
|---|---|---|---|---|
| **BIRD-SQL** (dev) | ~35% EX | **65–70% EX** | 70.4% (Arctic-R1-7B w/ GRPO) | ~78% (GPT-4o pipeline) |
| **Spider 1.0** (test) | ~70% EX | **80–85% EX** | ~82% (Qwen-2.5-Coder-7B) | ~86% (GPT-4o) |

**Note on Spider 2.0**: This benchmark involves enterprise-scale databases with 1000+ columns and complex multi-tool workflows. Even GPT-4o scores only ~13% on Spider 2.0-Lite. We do not set a target here — Spider 2.0 requires agentic capabilities beyond single-query generation.

### 11.2 Why These Targets Are Achievable

- **BIRD-SQL 65–70%**: Arctic-Text2SQL-R1-7B already achieves 70.4% with standard SQL + GRPO. Pipe syntax should provide a comparable or slightly improved structural advantage, as the model doesn't need to manage nesting.
- **Spider 1.0 80–85%**: Qwen-2.5-Coder-7B already hits 82% with standard SQL. Fine-tuning on pipe syntax should at minimum match this, with incremental training providing an additional boost through better step-by-step reasoning.

---

## 12. Deployment

### 12.1 Serving Stack

- **vLLM** for inference with automatic prefix caching (APC) enabled.
- **Schema caching**: database schemas (often 1–3K tokens) are prepended to every request. With prefix caching, repeated queries against the same database save 70–90% on schema tokens.
- **Speculative decoding** (optional): use the Qwen-2.5-Coder-1.5B model as a draft for the 7B model. Expected speedup: 1.5–1.8x for standard draft-verify; 2–3x if using EAGLE-style speculator trained on the 7B model's hidden states.

### 12.2 Output Processing

The generated pipe SQL must be transpiled to the target engine's dialect before execution. Since SQLGlot can parse pipe syntax and emit standard SQL for any of its 30+ supported dialects, this is a single function call:

```python
import sqlglot

pipe_query = model.generate(question, schema)
executable = sqlglot.transpile(pipe_query, read="bigquery", write="postgres")[0]
cursor.execute(executable)
```

This means the model generates in a single canonical pipe syntax, and deployment supports any target database.

---

## 13. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Decompiler bugs produce incorrect pipe SQL | Corrupted training data | Dual-execution validation loop (Section 6) catches all semantic errors |
| 16K seed queries insufficient for fine-tuning | Underfitting on complex patterns | Data augmentation (Section 4.4) expands to 50–100K queries; trajectory decomposition (Section 7.2) amplifies to 200K+ samples |
| Pipe syntax unseen in pretraining | Model struggles with novel tokens | LoRA rank 64–128 provides sufficient capacity; `\|>` is a simple 2-token sequence, not a complex new grammar |
| Engine-specific pipe differences (BigQuery vs. Spark) | Portability issues | Train on canonical (BigQuery) syntax only; transpile at deployment via SQLGlot |
| Incremental generation is slower than one-shot | Latency at inference time | Speculative decoding + prefix caching offset the overhead; accuracy gains justify the tradeoff |

---

## 14. Implementation Roadmap

### Phase 1: Decompiler
- Build pipe emitter on top of SQLGlot's qualified AST
- Implement transformation rules (Section 5.3)
- Validate against Spider 1.0 + BIRD-SQL with dual-execution loop
- Target: 90%+ of seed queries successfully decompiled and validated

### Phase 2: Data Pipeline
- Run decompiler over all seed queries
- Apply augmentation strategies (schema substitution, complexity composition, NL paraphrasing)
- Generate trajectory-decomposed JSONL training files
- Target: 50K+ pipe queries → 200K+ training samples

### Phase 3: Supervised Fine-Tuning
- Fine-tune Qwen-2.5-Coder-7B with QLoRA
- Evaluate on held-out BIRD-SQL dev split
- Ablate: incremental vs. one-shot training, LoRA rank, context window size
- Target: match or exceed base model's standard SQL accuracy

### Phase 4: GRPO Reinforcement
- Implement execution-based reward function
- Run GRPO on the SFT checkpoint
- Evaluate on full BIRD-SQL dev and Spider 1.0 test
- Target: 5–10% EX improvement over SFT-only model

### Phase 5: Agentic Integration & Deployment
- Build agent loop with dry-run, sample rows, and syntax validation tools
- Integrate with vLLM serving
- End-to-end evaluation on production schemas
- Target: sub-2-second latency for 4-operator queries on RTX 4090

---

## Appendix A: Pipe SQL Engine Support Matrix

| Engine | Status | Pipe Symbol | Notes |
|---|---|---|---|
| **BigQuery (GoogleSQL)** | GA (Feb 2025) | `\|>` | 20+ operators; most complete implementation |
| **Apache Spark 4.0+** | GA | `\|>` | Full operator set; mirrors BigQuery |
| **Databricks Runtime 16.2+** | GA | `\|>` | Same as Spark |
| **Firebolt** | Supported | `\|>` | Subset of operators |
| **DuckDB** | Community extension only | `\|>` | Regex-based preprocessor; not production-grade |
| **Snowflake** | Different concept | `->>` | Chains full SQL statements, not operators |
| **PostgreSQL / MySQL / SQL Server** | Not supported | N/A | — |

## Appendix B: SQLGlot Pipe Syntax Capabilities

| Capability | Supported? |
|---|---|
| Parse pipe syntax → AST | Yes (12 operators) |
| Generate pipe syntax from AST | **No** |
| Pipe → standard SQL transpilation | Yes (via CTE decomposition at parse time) |
| Standard SQL → pipe transpilation | **No** (requires custom decompiler) |
| Optimizer/qualify on pipe input | Yes (operates on CTE-decomposed AST) |
| Supported pipe operators (parsing) | SELECT, WHERE, AGGREGATE, EXTEND, JOIN, ORDER BY, LIMIT, AS, PIVOT, UNPIVOT, TABLESAMPLE, set ops |
| Known broken operators (v29.x) | SET, DROP, RENAME, DISTINCT, CALL, WITH |
