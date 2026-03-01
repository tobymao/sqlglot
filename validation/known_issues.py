"""Known SQLGlot round-trip issue patterns from design doc Section 7."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class KnownIssue:
    tag: str
    pattern: re.Pattern
    description: str
    severity: str = "warning"  # "warning", "silent_wrong", "execution_error", "cosmetic"


# Section 7.1: Issues that cause silent wrong answers
SILENT_WRONG_ISSUES: list[KnownIssue] = [
    KnownIssue(
        tag="LEAST_2ARG_BUG",
        pattern=re.compile(r"\bLEAST\s*\([^,]+,[^,]+\)", re.IGNORECASE),
        description="2-argument LEAST(a, b) drops second argument, outputs just a",
        severity="silent_wrong",
    ),
    KnownIssue(
        tag="GREATEST_2ARG_BUG",
        pattern=re.compile(r"\bGREATEST\s*\([^,]+,[^,]+\)", re.IGNORECASE),
        description="2-argument GREATEST(a, b) drops second argument",
        severity="silent_wrong",
    ),
    KnownIssue(
        tag="IGNORE_NULLS_DROPPED",
        pattern=re.compile(r"IGNORE\s+NULLS", re.IGNORECASE),
        description="SQLGlot silently drops IGNORE NULLS from window functions",
        severity="silent_wrong",
    ),
    KnownIssue(
        tag="SAFE_CAST_LOSSY",
        pattern=re.compile(r"\bSAFE_CAST\b", re.IGNORECASE),
        description="SAFE_CAST → CAST loses safety semantics; runtime errors instead of NULL",
        severity="silent_wrong",
    ),
    KnownIssue(
        tag="GROUP_CONCAT_ORDER_DROPPED",
        pattern=re.compile(r"GROUP_CONCAT\s*\([^)]*ORDER\s+BY", re.IGNORECASE),
        description="ORDER BY inside GROUP_CONCAT is silently removed",
        severity="silent_wrong",
    ),
]

# Section 7.2: Issues that cause execution errors
EXECUTION_ERROR_ISSUES: list[KnownIssue] = [
    KnownIssue(
        tag="SET_OP_ALL_UNSUPPORTED",
        pattern=re.compile(r"\b(EXCEPT|INTERSECT)\s+ALL\b", re.IGNORECASE),
        description="SQLite does not support EXCEPT ALL / INTERSECT ALL",
        severity="execution_error",
    ),
    KnownIssue(
        tag="TABLESAMPLE_UNSUPPORTED",
        pattern=re.compile(r"\bTABLESAMPLE\b", re.IGNORECASE),
        description="TABLESAMPLE not supported in SQLite",
        severity="execution_error",
    ),
]

# General patterns that may cause issues (from original implementation)
GENERAL_ISSUES: list[KnownIssue] = [
    KnownIssue(
        tag="NESTED_SUBQUERY",
        pattern=re.compile(
            r"SELECT\s.*\(\s*SELECT\s.*\(\s*SELECT", re.IGNORECASE | re.DOTALL
        ),
        description="Deeply nested subqueries (3+ levels) may lose semantics",
        severity="warning",
    ),
    KnownIssue(
        tag="CASE_IN_AGG",
        pattern=re.compile(
            r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*CASE\b", re.IGNORECASE
        ),
        description="CASE inside aggregate may have round-trip issues",
        severity="warning",
    ),
    KnownIssue(
        tag="LIKE_ESCAPE",
        pattern=re.compile(r"\bLIKE\b.*\bESCAPE\b", re.IGNORECASE),
        description="LIKE with ESCAPE clause may not round-trip",
        severity="warning",
    ),
]

# Combined list for scanning
KNOWN_ISSUE_PATTERNS: list[KnownIssue] = (
    SILENT_WRONG_ISSUES + EXECUTION_ERROR_ISSUES + GENERAL_ISSUES
)


def check_known_issues(sql: str) -> list[str]:
    """Return list of known issue tags for a query."""
    return [ki.tag for ki in KNOWN_ISSUE_PATTERNS if ki.pattern.search(sql)]


def get_known_issue_details(sql: str) -> list[KnownIssue]:
    """Return full known issue objects for a query."""
    return [ki for ki in KNOWN_ISSUE_PATTERNS if ki.pattern.search(sql)]
