"""Main execution harness for the validation loop.

Implements design doc Sections 3, 6, 9, 10.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field

import sqlglot

from .compare import ComparisonResult, compare_with_tolerance
from .diagnose import DiagnosticReport, classify_error, diagnose_mismatch
from .known_issues import check_known_issues
from .normalize import NormalizedEntry, load_entries
from .pipe_validator import validate_pipe_syntax


@dataclass
class ExecutionResult:
    rows: list[tuple] = field(default_factory=list)
    error: str = ""
    error_type: str = ""  # "timeout", "sqlite_error", "parse_error", "generate_error"
    error_category: str = ""  # Detailed: SYNTAX, NO_SUCH_TABLE, etc.
    elapsed_ms: float = 0.0


@dataclass
class ValidationRecord:
    question_id: str
    db_id: str
    difficulty: str
    gold_sql: str
    pipe_sql: str = ""
    round_tripped_sql: str = ""
    gold_result: ExecutionResult = field(default_factory=ExecutionResult)
    rt_result: ExecutionResult = field(default_factory=ExecutionResult)
    comparison: ComparisonResult = field(default_factory=lambda: ComparisonResult(match=False))
    diagnostic: DiagnosticReport | None = None
    known_issue_tags: list[str] = field(default_factory=list)
    status: str = ""  # "match", "mismatch", "gold_error", "rt_error", "parse_error", "known_issue"
    pipe_valid: bool = True


class _Timeout:
    """Context manager for execution timeout using SIGALRM."""

    def __init__(self, seconds: int):
        self.seconds = seconds

    def __enter__(self):
        if hasattr(signal, "SIGALRM"):
            signal.signal(signal.SIGALRM, self._handler)
            signal.alarm(self.seconds)
        return self

    def __exit__(self, *args):
        if hasattr(signal, "SIGALRM"):
            signal.alarm(0)

    @staticmethod
    def _handler(signum, frame):
        raise TimeoutError("SQL execution timed out")


def execute_with_error_handling(sql: str, db_path: str, timeout: int = 30) -> ExecutionResult:
    """Execute SQL against a SQLite database with error handling and categorization."""
    start = time.monotonic()
    try:
        with _Timeout(timeout):
            conn = sqlite3.connect(db_path)
            try:
                cursor = conn.execute(sql)
                rows = cursor.fetchall()
                elapsed = (time.monotonic() - start) * 1000
                return ExecutionResult(rows=rows, elapsed_ms=elapsed)
            finally:
                conn.close()
    except TimeoutError:
        return ExecutionResult(
            error=f"Timed out after {timeout}s",
            error_type="timeout",
            error_category="TIMEOUT",
            elapsed_ms=timeout * 1000,
        )
    except sqlite3.Error as e:
        elapsed = (time.monotonic() - start) * 1000
        error_msg = str(e)
        return ExecutionResult(
            error=error_msg,
            error_type="sqlite_error",
            error_category=classify_error(error_msg),
            elapsed_ms=elapsed,
        )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        error_msg = str(e)
        return ExecutionResult(
            error=error_msg,
            error_type="sqlite_error",
            error_category=classify_error(error_msg),
            elapsed_ms=elapsed,
        )


def _round_trip_sql(sql: str, dialect: str = "sqlite") -> tuple[str, str]:
    """Parse and regenerate SQL through SQLGlot. Returns (generated_sql, error)."""
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        generated = parsed.sql(dialect=dialect)
        return generated, ""
    except Exception as e:
        return "", str(e)


def run_validation(
    dev_json_path: str,
    db_dir: str,
    output_path: str,
    source: str = "spider",
    decompiler=None,
    baseline: bool = False,
    timeout: int = 30,
    limit: int = 0,
) -> list[ValidationRecord]:
    """Run the full validation pipeline.

    In baseline mode (no decompiler): gold SQL → SQLGlot parse → generate → execute → compare.
    With decompiler: gold SQL → decompile to pipe SQL → transpile to SQLite → execute → compare.
    """
    entries = load_entries(dev_json_path, source)
    if limit > 0:
        entries = entries[:limit]

    records = []
    for i, entry in enumerate(entries):
        record = _validate_entry(entry, db_dir, decompiler, baseline, timeout)
        records.append(record)

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(entries)} queries...")

    # Write output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    _write_outputs(records, output_path, source)

    return records


def _validate_entry(
    entry: NormalizedEntry,
    db_dir: str,
    decompiler,
    baseline: bool,
    timeout: int,
) -> ValidationRecord:
    """Validate a single entry."""
    db_path = os.path.join(db_dir, entry.db_id, f"{entry.db_id}.sqlite")

    record = ValidationRecord(
        question_id=entry.question_id,
        db_id=entry.db_id,
        difficulty=entry.difficulty,
        gold_sql=entry.gold_sql,
        known_issue_tags=check_known_issues(entry.gold_sql),
    )

    # Execute gold SQL
    record.gold_result = execute_with_error_handling(entry.gold_sql, db_path, timeout)
    if record.gold_result.error:
        record.status = "gold_error"
        return record

    # Round-trip through SQLGlot (baseline) or decompiler
    if baseline or decompiler is None:
        rt_sql, rt_error = _round_trip_sql(entry.gold_sql)
        record.round_tripped_sql = rt_sql
    else:
        try:
            pipe_sql = decompiler(entry.gold_sql)
            record.pipe_sql = pipe_sql

            # Intermediate validation: check pipe SQL syntax (Section 10)
            pipe_validation = validate_pipe_syntax(pipe_sql)
            record.pipe_valid = pipe_validation.valid

            if not pipe_validation.valid:
                rt_sql = ""
                rt_error = f"Invalid pipe SQL syntax: {pipe_validation.error}"
            else:
                rt_sql = sqlglot.transpile(pipe_sql, write="sqlite")[0]
                rt_error = ""

            record.round_tripped_sql = rt_sql
        except Exception as e:
            rt_sql, rt_error = "", str(e)
            record.round_tripped_sql = ""

    if rt_error:
        record.status = "parse_error"
        record.diagnostic = diagnose_mismatch(
            entry.question_id,
            entry.gold_sql,
            "",
            pipe_sql=record.pipe_sql,
            error=rt_error,
        )
        return record

    # Execute round-tripped SQL
    record.rt_result = execute_with_error_handling(rt_sql, db_path, timeout)
    if record.rt_result.error:
        record.status = "rt_error"
        record.diagnostic = diagnose_mismatch(
            entry.question_id,
            entry.gold_sql,
            rt_sql,
            pipe_sql=record.pipe_sql,
            error=record.rt_result.error,
        )
        return record

    # Compare results
    record.comparison = compare_with_tolerance(record.gold_result.rows, record.rt_result.rows)

    if record.comparison.match:
        record.status = "match"
    else:
        # Check if mismatch is attributable to known issues
        if record.known_issue_tags:
            record.status = "known_issue"
        else:
            record.status = "mismatch"
        record.diagnostic = diagnose_mismatch(
            entry.question_id,
            entry.gold_sql,
            rt_sql,
            pipe_sql=record.pipe_sql,
            comparison=record.comparison,
        )

    return record


def _write_outputs(records: list[ValidationRecord], output_path: str, source: str) -> None:
    """Write all output artifacts (design doc Section 11)."""
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    # 1. Full results.json
    with open(output_path, "w") as f:
        json.dump([_record_to_dict(r) for r in records], f, indent=2, default=str)

    # 2. Summary text
    summary_path = output_path.replace(".json", "_summary.txt")
    summary = format_stratified_report(records, source)
    with open(summary_path, "w") as f:
        f.write(summary)

    # 3. Mismatches only
    mismatches = [r for r in records if r.status in ("mismatch", "known_issue")]
    if mismatches:
        mismatch_path = os.path.join(output_dir, "mismatches.json")
        with open(mismatch_path, "w") as f:
            json.dump([_record_to_dict(r) for r in mismatches], f, indent=2, default=str)

    # 4. Errors only
    errors = [r for r in records if r.status in ("rt_error", "parse_error", "gold_error")]
    if errors:
        errors_path = os.path.join(output_dir, "errors.json")
        with open(errors_path, "w") as f:
            json.dump([_record_to_dict(r) for r in errors], f, indent=2, default=str)

    # 5. Golden pairs (MATCH records as training data)
    matches = [r for r in records if r.status == "match"]
    if matches:
        golden_path = os.path.join(output_dir, "golden_pairs.jsonl")
        with open(golden_path, "w") as f:
            for r in matches:
                pair = {
                    "question_id": r.question_id,
                    "db_id": r.db_id,
                    "difficulty": r.difficulty,
                    "gold_sql": r.gold_sql,
                    "pipe_sql": r.pipe_sql,
                    "round_tripped_sql": r.round_tripped_sql,
                    "validation": "MATCH",
                }
                f.write(json.dumps(pair, default=str) + "\n")


def _record_to_dict(record: ValidationRecord) -> dict:
    """Convert a ValidationRecord to a JSON-serializable dict."""
    d = {
        "question_id": record.question_id,
        "db_id": record.db_id,
        "difficulty": record.difficulty,
        "gold_sql": record.gold_sql,
        "pipe_sql": record.pipe_sql,
        "round_tripped_sql": record.round_tripped_sql,
        "status": record.status,
        "pipe_valid": record.pipe_valid,
        "known_issue_tags": record.known_issue_tags,
        "gold_result": {
            "row_count": len(record.gold_result.rows),
            "error": record.gold_result.error,
            "error_type": record.gold_result.error_type,
            "error_category": record.gold_result.error_category,
            "elapsed_ms": record.gold_result.elapsed_ms,
        },
        "rt_result": {
            "row_count": len(record.rt_result.rows),
            "error": record.rt_result.error,
            "error_type": record.rt_result.error_type,
            "error_category": record.rt_result.error_category,
            "elapsed_ms": record.rt_result.elapsed_ms,
        },
        "comparison": {
            "match": record.comparison.match,
            "match_type": record.comparison.match_type,
            "mismatch_type": record.comparison.mismatch_type,
            "row_count_a": record.comparison.row_count_a,
            "row_count_b": record.comparison.row_count_b,
            "col_count_a": record.comparison.col_count_a,
            "col_count_b": record.comparison.col_count_b,
            "row_count_match": record.comparison.row_count_match,
            "col_count_match": record.comparison.col_count_match,
            "sorted_match": record.comparison.sorted_match,
            "subset_a_in_b": record.comparison.subset_a_in_b,
            "subset_b_in_a": record.comparison.subset_b_in_a,
            "symmetric_difference": record.comparison.symmetric_difference,
            "f1_score": record.comparison.f1_score,
            "detail": record.comparison.detail,
        },
    }
    if record.diagnostic:
        d["diagnostic"] = {
            "mismatch_type": record.diagnostic.mismatch_type,
            "error_category": record.diagnostic.error_category,
            "known_issue_tags": record.diagnostic.known_issue_tags,
            "suspected_rules": record.diagnostic.suspected_rules,
            "detail": record.diagnostic.detail,
            "ast_diffs": [
                {
                    "category": ad.category,
                    "description": ad.description,
                    "node_a": ad.node_a,
                    "node_b": ad.node_b,
                }
                for ad in record.diagnostic.ast_diffs
            ],
        }
    return d


def format_stratified_report(records: list[ValidationRecord], source: str = "spider") -> str:
    """Format a difficulty-stratified summary report."""
    lines = []
    lines.append(f"=== Validation Report ({source.upper()}) ===")
    lines.append(f"Total queries: {len(records)}")
    lines.append("")

    # Overall stats
    by_status = defaultdict(int)
    for r in records:
        by_status[r.status] += 1

    lines.append("Overall:")
    for status in ["match", "mismatch", "known_issue", "gold_error", "rt_error", "parse_error"]:
        count = by_status.get(status, 0)
        pct = count / len(records) * 100 if records else 0
        lines.append(f"  {status:15s}: {count:5d}  ({pct:5.1f}%)")

    match_count = by_status.get("match", 0)
    ki_count = by_status.get("known_issue", 0)
    match_rate = match_count / len(records) * 100 if records else 0
    effective_rate = (match_count + ki_count) / len(records) * 100 if records else 0
    lines.append("")
    lines.append(f"  Match rate:     {match_rate:.1f}%")
    lines.append(f"  Effective rate: {effective_rate:.1f}% (excl. known issues)")
    lines.append("")

    # Stratified by difficulty
    by_diff = defaultdict(list)
    for r in records:
        by_diff[r.difficulty].append(r)

    difficulty_order = [
        "easy",
        "medium",
        "hard",
        "extra",
        "extra hard",
        "simple",
        "moderate",
        "challenging",
        "unknown",
    ]
    lines.append("By difficulty:")
    for diff in difficulty_order:
        if diff not in by_diff:
            continue
        subset = by_diff[diff]
        m = sum(1 for r in subset if r.status == "match")
        pct = m / len(subset) * 100 if subset else 0
        lines.append(f"  {diff:15s}: {m:4d}/{len(subset):4d} match ({pct:5.1f}%)")

    lines.append("")

    # Mismatch type breakdown
    mismatch_types = defaultdict(int)
    for r in records:
        if r.status in ("mismatch", "known_issue") and r.comparison.mismatch_type:
            mismatch_types[r.comparison.mismatch_type] += 1

    if mismatch_types:
        lines.append("Mismatch types:")
        for mt, count in sorted(mismatch_types.items(), key=lambda x: -x[1]):
            lines.append(f"  {mt:20s}: {count:4d}")
        lines.append("")

    # Error category breakdown
    error_cats = defaultdict(int)
    for r in records:
        if r.status in ("rt_error", "parse_error") and r.diagnostic:
            cat = r.diagnostic.error_category or r.diagnostic.mismatch_type
            error_cats[cat] += 1

    if error_cats:
        lines.append("Error categories:")
        for cat, count in sorted(error_cats.items(), key=lambda x: -x[1]):
            lines.append(f"  {cat:20s}: {count:4d}")
        lines.append("")

    # Suspected rules
    rule_counts = defaultdict(int)
    for r in records:
        if r.diagnostic and r.diagnostic.suspected_rules:
            for rule in r.diagnostic.suspected_rules[:1]:  # Primary suspect only
                rule_counts[rule] += 1

    if rule_counts:
        lines.append("Primary suspected rules:")
        for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {rule:25s}: {count:4d}")
        lines.append("")

    # Known issue correlation
    ki_counts = defaultdict(lambda: {"total": 0, "mismatch": 0})
    for r in records:
        for tag in r.known_issue_tags:
            ki_counts[tag]["total"] += 1
            if r.status != "match":
                ki_counts[tag]["mismatch"] += 1

    if ki_counts:
        lines.append("Known issue tag correlation:")
        for tag, counts in sorted(ki_counts.items()):
            lines.append(f"  {tag:30s}: {counts['mismatch']:3d}/{counts['total']:3d} mismatches")
        lines.append("")

    return "\n".join(lines)


def print_stratified_report(records: list[ValidationRecord], source: str = "spider") -> None:
    print(format_stratified_report(records, source))


def regression_test(
    previous_path: str, current_records: list[ValidationRecord]
) -> tuple[bool, list[str], list[str]]:
    """Check for regressions against previous results.

    Returns (passed, regressions, improvements).
    """
    with open(previous_path) as f:
        previous = json.load(f)

    prev_by_id = {r["question_id"]: r for r in previous}
    regressions = []
    improvements = []

    for record in current_records:
        prev = prev_by_id.get(record.question_id)
        if not prev:
            continue
        if prev["status"] == "match" and record.status != "match":
            regressions.append(f"{record.question_id}: was match, now {record.status}")
        elif prev["status"] != "match" and record.status == "match":
            improvements.append(f"{record.question_id}: was {prev['status']}, now match")

    return len(regressions) == 0, regressions, improvements
