"""
Compares query results produced by different engines.

Drivers return the same logical value in different Python representations (e.g. a REAL as a
float64 vs. its float32 widening, a TIMESTAMPTZ in different zones, JSON as text with different
formatting), so values are first normalized into a canonical form and then compared with a small
set of tolerances. Anything that isn't a representation detail is reported as a difference.
"""

from __future__ import annotations

import json
import math
import struct
import typing as t
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, tzinfo
from decimal import ROUND_HALF_UP, Decimal, localcontext

from conformance.result import Result

FLOAT_REL_TOL = 1e-9
FLOAT_ABS_TOL = 1e-12

# Engines pick different scales for inexact decimal results (e.g. AVG and division), so two
# decimals are compared at the coarser of their scales, but only if that scale is at least this
# large. Below it, a scale difference is a semantic difference (e.g. ROUND(x) vs. x).
MIN_ROUNDING_SCALE = 6

# Bounds the quadratic fallback used to pair up rows that sorting failed to align
MAX_UNMATCHED_SCAN = 500
MAX_REPORTED_ROWS = 10

# Type family pairs that don't warrant a warning, because an engine has no closer equivalent
_EQUIVALENT_FAMILIES = {frozenset(("string", "json"))}


class _NaN:
    """NaN sentinel, so that NaN compares equal to itself and can be sorted and hashed."""

    def __repr__(self) -> str:
        return "NaN"


NAN = _NaN()


@dataclass
class Comparison:
    equal: bool
    reason: str = ""
    details: dict[str, t.Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def compare_results(
    expected: Result,
    actual: Result,
    *,
    ordered: bool,
    timezone_name: str = "UTC",
    rel_tol: float = FLOAT_REL_TOL,
) -> Comparison:
    """
    Compares two results positionally by column; column names are ignored because engines name
    unaliased expressions differently. Rows are compared as a multiset unless `ordered` is set.
    """
    warnings = type_warnings(expected, actual)

    if len(expected.columns) != len(actual.columns):
        return Comparison(
            False,
            f"column count differs: expected {len(expected.columns)}, got {len(actual.columns)}",
            {"expected_columns": expected.columns, "actual_columns": actual.columns},
            warnings,
        )

    zone = get_zone(timezone_name)
    families = [effective_family(e, a) for e, a in zip(expected.families, actual.families)]
    expected_rows = normalize_rows(expected.rows, families, zone)
    actual_rows = normalize_rows(actual.rows, families, zone)

    if ordered:
        comparison = _compare_ordered(expected_rows, actual_rows, rel_tol)
    else:
        comparison = _compare_unordered(expected_rows, actual_rows, rel_tol)

    comparison.warnings = warnings
    return comparison


def type_warnings(expected: Result, actual: Result) -> list[str]:
    warnings = []
    for i, (e, a) in enumerate(zip(expected.families, actual.families)):
        if e != a and "null" not in (e, a) and frozenset((e, a)) not in _EQUIVALENT_FAMILIES:
            warnings.append(
                f"column {i + 1} type: expected {expected.types[i]} ({e}), got {actual.types[i]} ({a})"
            )
    return warnings


def effective_family(*families: str) -> str:
    """The family that decides how a column is normalized when the two sides disagree."""
    for family in ("json", "map", "float32"):
        if family in families:
            return family
    return families[0] if families else ""


def get_zone(name: str) -> tzinfo:
    if name.upper() == "UTC":
        return timezone.utc

    from zoneinfo import ZoneInfo

    return ZoneInfo(name)


def normalize_rows(rows: t.Iterable[t.Sequence], families: list[str], zone: tzinfo) -> list[tuple]:
    return [
        tuple(normalize(value, family, zone) for value, family in zip(row, families))
        for row in rows
    ]


def normalize(value: t.Any, family: str = "", zone: tzinfo = timezone.utc) -> t.Any:
    """Converts a driver value into a canonical, hashable, sortable representation."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return NAN
        if family == "float32" and math.isfinite(value):
            try:
                value = struct.unpack("f", struct.pack("f", value))[0]
            except OverflowError:
                pass
        # Collapses -0.0 into 0.0
        return value + 0.0
    if isinstance(value, Decimal):
        return NAN if value.is_nan() else value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        # Instants are compared as wall-clock times in the session time zone, which is how the
        # engines render them. A naive vs. aware mismatch is surfaced as a type warning instead.
        try:
            value = value.astimezone(zone)
        except OverflowError:
            value = value.astimezone(timezone.utc)
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        if family == "json":
            try:
                return ("json", _freeze_json(json.loads(value)))
            except ValueError:
                pass
        return value
    if isinstance(value, dict):
        if family == "map":
            return _sorted_pairs(value.items(), zone)
        # Structs are compared positionally because engines name fields differently
        return tuple(normalize(v, zone=zone) for v in value.values())
    if isinstance(value, (list, tuple)):
        if family == "map":
            return _sorted_pairs(value, zone)
        return tuple(normalize(v, zone=zone) for v in value)
    return value


def values_equal(a: t.Any, b: t.Any, rel_tol: float = FLOAT_REL_TOL) -> bool:
    """Compares two normalized values."""
    if a is None or b is None or a is NAN or b is NAN:
        return a is b
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if _is_number(a) and _is_number(b):
        return _numbers_equal(a, b, rel_tol)
    if isinstance(a, tuple) and isinstance(b, tuple):
        return len(a) == len(b) and all(values_equal(x, y, rel_tol) for x, y in zip(a, b))
    if isinstance(a, date) and isinstance(b, date):
        # A DATE matches a TIMESTAMP at midnight; the type difference is reported separately
        return _as_datetime(a) == _as_datetime(b)
    return type(a) is type(b) and a == b


def rows_equal(a: tuple, b: tuple, rel_tol: float = FLOAT_REL_TOL) -> bool:
    return len(a) == len(b) and all(values_equal(x, y, rel_tol) for x, y in zip(a, b))


def sort_key(value: t.Any) -> tuple:
    """A total order over normalized values, used to align rows of unordered results."""
    if value is None:
        return (0,)
    if value is NAN:
        return (1, 1, 0.0)
    if isinstance(value, bool):
        return (2, value)
    if isinstance(value, int):
        return (1, 0, value)
    if isinstance(value, (float, Decimal)):
        number = float(value)
        # Rounding keeps values that only differ within the comparison tolerance adjacent
        return (1, 0, float(f"{number:.9g}") if math.isfinite(number) else number)
    if isinstance(value, str):
        return (3, value)
    if isinstance(value, bytes):
        return (4, value)
    if isinstance(value, date):
        return (5, _as_datetime(value).isoformat())
    if isinstance(value, tuple):
        return (8, tuple(sort_key(v) for v in value))
    return (9, type(value).__name__, repr(value))


def format_row(row: tuple, limit: int = 300) -> str:
    text = repr(row)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _compare_ordered(expected: list[tuple], actual: list[tuple], rel_tol: float) -> Comparison:
    if len(expected) == len(actual):
        mismatches = [
            i for i, (e, a) in enumerate(zip(expected, actual)) if not rows_equal(e, a, rel_tol)
        ]
        if not mismatches:
            return Comparison(True)
    else:
        mismatches = []

    unordered = _compare_unordered(expected, actual, rel_tol)
    if not unordered.equal:
        return unordered

    first = mismatches[0]
    return Comparison(
        False,
        "same rows in a different order",
        {
            "first_difference": first,
            "expected_rows": [format_row(r) for r in expected[first : first + MAX_REPORTED_ROWS]],
            "actual_rows": [format_row(r) for r in actual[first : first + MAX_REPORTED_ROWS]],
        },
    )


def _compare_unordered(expected: list[tuple], actual: list[tuple], rel_tol: float) -> Comparison:
    if len(expected) == len(actual):
        sorted_expected = sorted(expected, key=_row_key)
        sorted_actual = sorted(actual, key=_row_key)
        if all(rows_equal(e, a, rel_tol) for e, a in zip(sorted_expected, sorted_actual)):
            return Comparison(True)

    missing, unexpected = _unmatched_rows(expected, actual, rel_tol)
    if not missing and not unexpected:
        return Comparison(True)

    if len(expected) != len(actual):
        reason = f"row count differs: expected {len(expected)}, got {len(actual)}"
    else:
        reason = f"{len(missing)} of {len(expected)} rows differ"

    return Comparison(
        False,
        reason,
        {
            "missing_rows": [format_row(r) for r in missing[:MAX_REPORTED_ROWS]],
            "unexpected_rows": [format_row(r) for r in unexpected[:MAX_REPORTED_ROWS]],
            "missing_count": len(missing),
            "unexpected_count": len(unexpected),
        },
    )


def _unmatched_rows(
    expected: list[tuple], actual: list[tuple], rel_tol: float
) -> tuple[list[tuple], list[tuple]]:
    """Pairs up equal rows, returning the expected rows and actual rows that have no partner."""
    buckets: dict[tuple, list[int]] = defaultdict(list)
    for i, row in enumerate(actual):
        buckets[_row_key(row)].append(i)

    matched = [False] * len(actual)
    leftover = []
    for row in expected:
        candidates = buckets.get(_row_key(row), [])
        index = next((i for i in candidates if rows_equal(row, actual[i], rel_tol)), None)
        if index is None:
            leftover.append(row)
        else:
            candidates.remove(index)
            matched[index] = True

    unmatched = [i for i, done in enumerate(matched) if not done]
    missing = []
    for row in leftover:
        if len(unmatched) > MAX_UNMATCHED_SCAN:
            missing.append(row)
            continue
        index = next((i for i in unmatched if rows_equal(row, actual[i], rel_tol)), None)
        if index is None:
            missing.append(row)
        else:
            unmatched.remove(index)

    return missing, [actual[i] for i in unmatched]


def _row_key(row: tuple) -> tuple:
    return tuple(sort_key(v) for v in row)


def _is_number(value: t.Any) -> bool:
    return isinstance(value, (int, float, Decimal))


def _numbers_equal(a: t.Any, b: t.Any, rel_tol: float) -> bool:
    if _is_integral(a) or _is_integral(b):
        # An integer result never matches a fractional one, e.g. 5 / 2 = 2 vs. 2.5. Python
        # compares ints with floats and decimals exactly.
        return a == b
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except OverflowError:
            return False
        if math.isinf(fa) or math.isinf(fb):
            return fa == fb
        if math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=FLOAT_ABS_TOL):
            return True
        # A decimal may be the rounded form of the float, e.g. Spark's DECIMAL division result
        # 0.333333 vs. Postgres' DOUBLE PRECISION 0.3333333333333333
        if isinstance(a, Decimal) or isinstance(b, Decimal):
            return _decimals_equal(Decimal(repr(fa)), Decimal(repr(fb)))
        return False
    if isinstance(a, Decimal) and isinstance(b, Decimal):
        return _decimals_equal(a, b)
    return a == b


def _is_integral(value: t.Any) -> bool:
    """Integers and decimals without fractional digits, e.g. NUMERIC(10, 0)."""
    if isinstance(value, int):
        return True
    return (
        isinstance(value, Decimal)
        and value.is_finite()
        and t.cast(int, value.as_tuple().exponent) >= 0
    )


def _decimals_equal(a: Decimal, b: Decimal) -> bool:
    if a == b:
        return True
    if not (a.is_finite() and b.is_finite()):
        return False

    exponent = max(t.cast(int, a.as_tuple().exponent), t.cast(int, b.as_tuple().exponent))
    if -exponent < MIN_ROUNDING_SCALE:
        return False

    quantum = Decimal(1).scaleb(exponent)
    with localcontext() as context:
        context.prec = 200
        return a.quantize(quantum, ROUND_HALF_UP) == b.quantize(quantum, ROUND_HALF_UP)


def _as_datetime(value: date) -> datetime:
    return value if isinstance(value, datetime) else datetime(value.year, value.month, value.day)


def _freeze_json(value: t.Any) -> t.Any:
    if isinstance(value, dict):
        return ("{}", tuple(sorted((k, _freeze_json(v)) for k, v in value.items())))
    if isinstance(value, list):
        return tuple(_freeze_json(v) for v in value)
    return value


def _sorted_pairs(pairs: t.Iterable, zone: tzinfo) -> tuple:
    normalized = [(normalize(k, zone=zone), normalize(v, zone=zone)) for k, v in pairs]
    return tuple(sorted(normalized, key=lambda pair: (sort_key(pair[0]), sort_key(pair[1]))))
