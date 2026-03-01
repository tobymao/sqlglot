"""Result set comparison with tolerance support and enhanced diagnostics."""

from __future__ import annotations

import math
import typing as t
from dataclasses import dataclass, field


@dataclass
class ComparisonResult:
    match: bool
    match_type: str = ""  # "exact", "tolerant", "mismatch"
    row_count_a: int = 0
    row_count_b: int = 0
    col_count_a: int = 0
    col_count_b: int = 0

    # Diagnostic fields (computed on mismatch)
    row_count_match: bool = True
    col_count_match: bool = True
    sorted_match: bool = False
    subset_a_in_b: bool = False
    subset_b_in_a: bool = False
    symmetric_difference: int = 0
    missing_from_b: list = field(default_factory=list)
    extra_in_b: list = field(default_factory=list)
    sample_diff_rows: list = field(default_factory=list)
    f1_score: float = 0.0

    # Root cause classification
    mismatch_type: str = ""
    detail: str = ""


def normalize_row(row: tuple, tolerance: float = 1e-6) -> tuple:
    normalized = []
    for val in row:
        if isinstance(val, float):
            if math.isnan(val):
                normalized.append(None)
            else:
                normalized.append(round(val, int(-math.log10(tolerance))))
        elif isinstance(val, str):
            normalized.append(val.strip())
        elif val is None:
            normalized.append(None)
        else:
            normalized.append(val)
    return tuple(normalized)


def _to_sortable(val: t.Any) -> tuple:
    """Convert a value to a sortable representation for deterministic ordering."""
    if val is None:
        return (0, "")
    if isinstance(val, (int, float)):
        return (1, val)
    return (2, str(val))


def _row_sort_key(row: tuple) -> tuple:
    return tuple(_to_sortable(v) for v in row)


def _compute_f1(set_a: set, set_b: set) -> float:
    """Compute F1 score treating set_a as gold and set_b as predicted."""
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    true_positives = len(set_a & set_b)
    precision = true_positives / len(set_b) if set_b else 0.0
    recall = true_positives / len(set_a) if set_a else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def _classify_mismatch(
    set_a: set,
    set_b: set,
    result_a: list[tuple],
    result_b: list[tuple],
) -> str:
    """Classify the type of mismatch between two result sets.

    Types: REORDER, EXTRA_ROWS, MISSING_ROWS, WRONG_VALUES, NULL_DIFF,
           TYPE_DIFF, DUPLICATE_DIFF, ROW_SWAP
    """
    missing = set_a - set_b
    extra = set_b - set_a

    # Check column count mismatch first
    if result_a and result_b:
        if len(result_a[0]) != len(result_b[0]):
            # Check if same values just reordered columns
            if result_a and result_b:
                sorted_cols_a = sorted(
                    result_a[0], key=lambda v: (type(v).__name__, str(v) if v is not None else "")
                )
                sorted_cols_b = sorted(
                    result_b[0], key=lambda v: (type(v).__name__, str(v) if v is not None else "")
                )
                if sorted_cols_a == sorted_cols_b:
                    return "REORDER"
            return "COL_COUNT"

    # Check duplicate count difference (same distinct values, different counts)
    distinct_a = set(result_a)
    distinct_b = set(result_b)
    if distinct_a == distinct_b and len(result_a) != len(result_b):
        return "DUPLICATE_DIFF"

    # No missing or extra rows but multiset differs
    if not missing and not extra and len(result_a) != len(result_b):
        return "DUPLICATE_DIFF"

    # Check if it's only NULL differences
    if missing and extra and len(missing) == len(extra):
        null_diffs = 0
        for row in missing:
            if any(v is None for v in row):
                null_diffs += 1
        for row in extra:
            if any(v is None for v in row):
                null_diffs += 1
        if null_diffs == len(missing) + len(extra):
            return "NULL_DIFF"

    # Check for type differences (e.g., 1 vs 1.0, "1" vs 1)
    if missing and extra and len(missing) == len(extra):
        type_diffs = 0
        sorted_missing = sorted(missing, key=_row_sort_key)
        sorted_extra = sorted(extra, key=_row_sort_key)
        for m, e in zip(sorted_missing, sorted_extra):
            if len(m) == len(e):
                all_type_diff = True
                for vm, ve in zip(m, e):
                    if vm == ve:
                        continue
                    try:
                        if str(vm) == str(ve):
                            continue
                    except Exception:
                        pass
                    all_type_diff = False
                    break
                if all_type_diff:
                    type_diffs += 1
        if type_diffs == len(missing):
            return "TYPE_DIFF"

    if extra and not missing:
        return "EXTRA_ROWS"
    if missing and not extra:
        return "MISSING_ROWS"
    if missing and extra:
        # Same row count but different values
        if len(result_a) == len(result_b):
            return "WRONG_VALUES"
        return "ROW_SWAP"

    return "UNKNOWN"


def compare_results(result_a: list[tuple], result_b: list[tuple]) -> ComparisonResult:
    """Strict set equality (order-independent)."""
    set_a = set(result_a)
    set_b = set(result_b)

    col_count_a = len(result_a[0]) if result_a else 0
    col_count_b = len(result_b[0]) if result_b else 0

    if set_a == set_b:
        return ComparisonResult(
            match=True,
            match_type="exact",
            row_count_a=len(result_a),
            row_count_b=len(result_b),
            col_count_a=col_count_a,
            col_count_b=col_count_b,
            row_count_match=len(result_a) == len(result_b),
            col_count_match=col_count_a == col_count_b,
            f1_score=1.0,
        )

    missing = sorted(set_a - set_b, key=_row_sort_key)
    extra = sorted(set_b - set_a, key=_row_sort_key)

    # Sorted match check
    sorted_a = sorted(result_a, key=_row_sort_key)
    sorted_b = sorted(result_b, key=_row_sort_key)
    sorted_match = sorted_a == sorted_b

    mismatch_type = _classify_mismatch(set_a, set_b, result_a, result_b)

    return ComparisonResult(
        match=False,
        match_type="mismatch",
        row_count_a=len(result_a),
        row_count_b=len(result_b),
        col_count_a=col_count_a,
        col_count_b=col_count_b,
        row_count_match=len(result_a) == len(result_b),
        col_count_match=col_count_a == col_count_b,
        sorted_match=sorted_match,
        subset_a_in_b=set_a <= set_b,
        subset_b_in_a=set_b <= set_a,
        symmetric_difference=len(set_a ^ set_b),
        missing_from_b=missing[:10],
        extra_in_b=extra[:10],
        sample_diff_rows=missing[:5],
        f1_score=_compute_f1(set_a, set_b),
        mismatch_type=mismatch_type,
        detail=f"{len(missing)} rows missing, {len(extra)} extra rows",
    )


def compare_with_tolerance(
    result_a: list[tuple], result_b: list[tuple], tolerance: float = 1e-6
) -> ComparisonResult:
    """Set equality with float tolerance."""
    norm_a = [normalize_row(r, tolerance) for r in result_a]
    norm_b = [normalize_row(r, tolerance) for r in result_b]

    set_a = set(norm_a)
    set_b = set(norm_b)

    col_count_a = len(result_a[0]) if result_a else 0
    col_count_b = len(result_b[0]) if result_b else 0

    if set_a == set_b:
        return ComparisonResult(
            match=True,
            match_type="tolerant",
            row_count_a=len(result_a),
            row_count_b=len(result_b),
            col_count_a=col_count_a,
            col_count_b=col_count_b,
            row_count_match=len(result_a) == len(result_b),
            col_count_match=col_count_a == col_count_b,
            f1_score=1.0,
        )

    missing = sorted(set_a - set_b, key=_row_sort_key)
    extra = sorted(set_b - set_a, key=_row_sort_key)

    sorted_a = sorted(norm_a, key=_row_sort_key)
    sorted_b = sorted(norm_b, key=_row_sort_key)
    sorted_match = sorted_a == sorted_b

    mismatch_type = _classify_mismatch(set_a, set_b, norm_a, norm_b)

    return ComparisonResult(
        match=False,
        match_type="mismatch",
        row_count_a=len(result_a),
        row_count_b=len(result_b),
        col_count_a=col_count_a,
        col_count_b=col_count_b,
        row_count_match=len(result_a) == len(result_b),
        col_count_match=col_count_a == col_count_b,
        sorted_match=sorted_match,
        subset_a_in_b=set_a <= set_b,
        subset_b_in_a=set_b <= set_a,
        symmetric_difference=len(set_a ^ set_b),
        missing_from_b=missing[:10],
        extra_in_b=extra[:10],
        sample_diff_rows=missing[:5],
        f1_score=_compute_f1(set_a, set_b),
        mismatch_type=mismatch_type,
        detail=f"{len(missing)} rows missing, {len(extra)} extra rows (tolerance={tolerance})",
    )
