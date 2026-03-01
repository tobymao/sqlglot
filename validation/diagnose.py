"""Mismatch diagnostics, AST diff analysis, and root cause identification.

Implements design doc Sections 5.2–5.5 and 6.1.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from .compare import ComparisonResult
from .known_issues import check_known_issues


@dataclass
class ASTDiff:
    category: str  # e.g. "join_type", "predicate", "aggregation", "ordering"
    description: str
    node_a: str = ""
    node_b: str = ""


@dataclass
class DiagnosticReport:
    question_id: str = ""
    gold_sql: str = ""
    round_tripped_sql: str = ""
    pipe_sql: str = ""
    mismatch_type: str = ""
    error_category: str = ""
    known_issue_tags: list[str] = field(default_factory=list)
    ast_diffs: list[ASTDiff] = field(default_factory=list)
    suspected_rules: list[str] = field(default_factory=list)
    detail: str = ""
    sample_extra: list = field(default_factory=list)
    sample_missing: list = field(default_factory=list)


# Section 5.5: Map mismatch types to suspected decompiler rules
RULE_SUSPECTS: dict[str, list[str]] = {
    "REORDER": ["projection_rule"],
    "EXTRA_ROWS": ["where_rule", "aggregate_rule"],
    "MISSING_ROWS": ["where_rule", "join_rule", "subquery_rule"],
    "WRONG_VALUES": ["aggregate_rule", "window_rule", "projection_rule"],
    "NULL_DIFF": ["join_rule", "where_rule"],
    "TYPE_DIFF": ["projection_rule"],
    "DUPLICATE_DIFF": ["terminal_rule"],
    "COL_COUNT": ["projection_rule", "aggregate_rule"],
    "ROW_SWAP": ["where_rule", "aggregate_rule"],
}


def classify_error(error_msg: str) -> str:
    """Classify SQLite error messages into categories (design doc Section 6.1)."""
    msg = error_msg.lower()
    if "no such table" in msg:
        return "NO_SUCH_TABLE"
    if "no such column" in msg:
        return "NO_SUCH_COLUMN"
    if "no such function" in msg:
        return "NO_SUCH_FUNCTION"
    if "ambiguous column" in msg:
        return "AMBIGUOUS_COLUMN"
    if "near" in msg and "syntax error" in msg:
        return "SYNTAX"
    if "near" in msg:
        return "SYNTAX"
    if "cannot use aggregate" in msg or "misuse of aggregate" in msg:
        return "TYPE_ERROR"
    if "timed out" in msg or "timeout" in msg:
        return "TIMEOUT"
    return "OTHER"


def compute_ast_diff(sql_a: str, sql_b: str, dialect: str = "sqlite") -> list[ASTDiff]:
    """Compare AST structures of two SQL statements (design doc Section 5.4)."""
    diffs = []

    try:
        tree_a = sqlglot.parse_one(sql_a, dialect=dialect)
        tree_b = sqlglot.parse_one(sql_b, dialect=dialect)
    except Exception as e:
        return [ASTDiff(category="parse_error", description=str(e))]

    # Compare FROM clauses
    from_a = tree_a.find(exp.From)
    from_b = tree_b.find(exp.From)
    if from_a and from_b:
        if from_a.sql(dialect=dialect) != from_b.sql(dialect=dialect):
            diffs.append(ASTDiff(
                category="from",
                description=f"FROM changed",
                node_a=from_a.sql(dialect=dialect)[:200],
                node_b=from_b.sql(dialect=dialect)[:200],
            ))

    # Compare SELECT expressions
    if isinstance(tree_a, exp.Select) and isinstance(tree_b, exp.Select):
        sels_a = [e.sql(dialect=dialect) for e in tree_a.expressions]
        sels_b = [e.sql(dialect=dialect) for e in tree_b.expressions]
        if sels_a != sels_b:
            diffs.append(ASTDiff(
                category="projection",
                description=f"SELECT expressions differ: {len(sels_a)} vs {len(sels_b)} columns",
                node_a=", ".join(sels_a)[:200],
                node_b=", ".join(sels_b)[:200],
            ))

    # Compare JOINs
    joins_a = list(tree_a.find_all(exp.Join))
    joins_b = list(tree_b.find_all(exp.Join))
    if len(joins_a) != len(joins_b):
        diffs.append(ASTDiff(
            category="join_type",
            description=f"JOIN count changed: {len(joins_a)} → {len(joins_b)}",
        ))
    for i, (ja, jb) in enumerate(zip(joins_a, joins_b)):
        ja_sql = ja.sql(dialect=dialect)
        jb_sql = jb.sql(dialect=dialect)
        if ja_sql != jb_sql:
            diffs.append(ASTDiff(
                category="join_type",
                description=f"JOIN[{i}] changed",
                node_a=ja_sql[:200],
                node_b=jb_sql[:200],
            ))

    # Compare WHERE predicates
    where_a = tree_a.find(exp.Where)
    where_b = tree_b.find(exp.Where)
    if (where_a is None) != (where_b is None):
        diffs.append(ASTDiff(
            category="predicate",
            description=f"WHERE {'added' if where_b else 'dropped'}",
        ))
    elif where_a and where_b:
        if where_a.sql(dialect=dialect) != where_b.sql(dialect=dialect):
            diffs.append(ASTDiff(
                category="predicate",
                description="WHERE condition changed",
                node_a=where_a.sql(dialect=dialect)[:200],
                node_b=where_b.sql(dialect=dialect)[:200],
            ))

    # Compare GROUP BY
    group_a = tree_a.find(exp.Group)
    group_b = tree_b.find(exp.Group)
    if (group_a is None) != (group_b is None):
        diffs.append(ASTDiff(
            category="grouping",
            description=f"GROUP BY {'added' if group_b else 'dropped'}",
        ))
    elif group_a and group_b:
        if group_a.sql(dialect=dialect) != group_b.sql(dialect=dialect):
            diffs.append(ASTDiff(
                category="grouping",
                description="GROUP BY changed",
                node_a=group_a.sql(dialect=dialect)[:200],
                node_b=group_b.sql(dialect=dialect)[:200],
            ))

    # Compare aggregate functions
    aggs_a = sorted(n.sql(dialect=dialect) for n in tree_a.find_all(exp.AggFunc))
    aggs_b = sorted(n.sql(dialect=dialect) for n in tree_b.find_all(exp.AggFunc))
    if aggs_a != aggs_b:
        diffs.append(ASTDiff(
            category="aggregation",
            description=f"Aggregates changed",
            node_a=str(aggs_a)[:200],
            node_b=str(aggs_b)[:200],
        ))

    # Compare ORDER BY
    orders_a = list(tree_a.find_all(exp.Order))
    orders_b = list(tree_b.find_all(exp.Order))
    if len(orders_a) != len(orders_b):
        diffs.append(ASTDiff(
            category="ordering",
            description=f"ORDER BY count changed: {len(orders_a)} → {len(orders_b)}",
        ))

    # Compare HAVING
    having_a = tree_a.find(exp.Having)
    having_b = tree_b.find(exp.Having)
    if (having_a is None) != (having_b is None):
        diffs.append(ASTDiff(
            category="having",
            description=f"HAVING {'added' if having_b else 'dropped'}",
        ))

    # If no structural diffs, do string comparison
    if not diffs:
        gen_a = tree_a.sql(dialect=dialect)
        gen_b = tree_b.sql(dialect=dialect)
        if gen_a != gen_b:
            diffs.append(ASTDiff(
                category="surface",
                description="SQL strings differ but no structural AST diff detected",
                node_a=gen_a[:200],
                node_b=gen_b[:200],
            ))

    return diffs


def identify_suspected_rule(ast_diffs: list[ASTDiff], mismatch_type: str) -> list[str]:
    """Map mismatch type + AST diffs to suspected decompiler rules (Section 5.5)."""
    suspects = list(RULE_SUSPECTS.get(mismatch_type, []))

    # Refine based on AST diffs
    for diff in ast_diffs:
        if diff.category == "join_type":
            if "join_rule" not in suspects:
                suspects.insert(0, "join_rule")
        if diff.category == "predicate":
            if "where_rule" not in suspects:
                suspects.insert(0, "where_rule")
        if diff.category in ("grouping", "aggregation"):
            if "aggregate_rule" not in suspects:
                suspects.insert(0, "aggregate_rule")
        if diff.category == "ordering":
            if "terminal_rule" not in suspects:
                suspects.insert(0, "terminal_rule")
        if diff.category == "projection":
            if "projection_rule" not in suspects:
                suspects.insert(0, "projection_rule")
        if diff.category == "from":
            if "from_rule" not in suspects:
                suspects.insert(0, "from_rule")
        if diff.category == "having":
            if "aggregate_rule" not in suspects:
                suspects.insert(0, "aggregate_rule")

    # Deduplicate preserving order
    seen = set()
    result = []
    for s in suspects:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


def diagnose_mismatch(
    question_id: str,
    gold_sql: str,
    round_tripped_sql: str,
    pipe_sql: str = "",
    comparison: ComparisonResult | None = None,
    error: str = "",
) -> DiagnosticReport:
    """Create a diagnostic report for a mismatched or errored query."""
    known_tags = check_known_issues(gold_sql)

    report = DiagnosticReport(
        question_id=question_id,
        gold_sql=gold_sql,
        round_tripped_sql=round_tripped_sql,
        pipe_sql=pipe_sql,
        known_issue_tags=known_tags,
    )

    if error:
        report.error_category = classify_error(error)
        report.mismatch_type = report.error_category
        report.detail = error
    elif comparison and not comparison.match:
        report.mismatch_type = comparison.mismatch_type or "result_diff"
        report.detail = comparison.detail
        report.sample_missing = comparison.missing_from_b[:5]
        report.sample_extra = comparison.extra_in_b[:5]
    else:
        report.mismatch_type = "unknown"

    # Compute AST diff if we have both SQLs
    if round_tripped_sql:
        report.ast_diffs = compute_ast_diff(gold_sql, round_tripped_sql)

    report.suspected_rules = identify_suspected_rule(report.ast_diffs, report.mismatch_type)

    return report
