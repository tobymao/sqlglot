"""Normalize Spider/BIRD JSON entries to a common format."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass
class NormalizedEntry:
    question_id: str
    db_id: str
    difficulty: str
    gold_sql: str


def normalize_entry(entry: dict, source: str = "spider", index: int = 0) -> NormalizedEntry:
    if source == "spider":
        sql = _clean_sql(entry.get("query", entry.get("sql", "")))
        return NormalizedEntry(
            question_id=f"spider_{index}",
            db_id=entry["db_id"],
            difficulty=entry.get("difficulty") or _classify_difficulty(sql),
            gold_sql=sql,
        )
    elif source == "bird":
        return NormalizedEntry(
            question_id=entry.get("question_id", f"bird_{index}"),
            db_id=entry["db_id"],
            difficulty=entry.get("difficulty", "unknown"),
            gold_sql=_clean_sql(entry.get("SQL", entry.get("query", ""))),
        )
    else:
        raise ValueError(f"Unknown source: {source}")


def load_entries(path: str, source: str = "spider") -> list[NormalizedEntry]:
    with open(path) as f:
        raw = json.load(f)
    return [normalize_entry(entry, source, i) for i, entry in enumerate(raw)]


def _classify_difficulty(sql: str) -> str:
    """Classify SQL difficulty based on Spider's criteria (component counting)."""
    upper = sql.upper()
    components = 0
    # Count SQL components per Spider's difficulty scheme
    for kw in ["WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
        if kw in upper:
            components += 1
    # Count JOINs
    components += len(re.findall(r"\bJOIN\b", upper))
    # Count nested SELECTs (subqueries)
    nested = upper.count("SELECT") - 1
    components += nested
    # Count set operations
    for op in ["UNION", "INTERSECT", "EXCEPT"]:
        components += upper.count(op)
    # Count aggregations
    for agg in ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("]:
        components += min(upper.count(agg), 1)

    if components <= 1:
        return "easy"
    elif components <= 2:
        return "medium"
    elif components <= 3 and nested == 0:
        return "hard"
    else:
        return "extra"


def _clean_sql(sql: str) -> str:
    if isinstance(sql, dict):
        # Spider sometimes stores SQL as {"human_readable": ..., "sql": ...}
        sql = sql.get("sql", sql.get("human_readable", str(sql)))
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql
