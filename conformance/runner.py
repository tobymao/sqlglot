"""Runs conformance cases and classifies every check ("leg") of every case."""

from __future__ import annotations

import json
import time
import traceback
import typing as t
from dataclasses import asdict, dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, UnsupportedError

from conformance.cases import Case
from conformance.compare import FLOAT_REL_TOL, compare_results, format_row
from conformance.dataset import TABLES_BY_NAME
from conformance.engines import Engine, EngineError
from conformance.result import Result

# Leg statuses, from worst to best
INVALID_CASE = "invalid_case"  # the source SQL failed on its own engine: fix the case
SQLGLOT_ERROR = "sqlglot_error"  # sqlglot raised while parsing or generating
ERROR = "error"  # the generated SQL failed on its engine
MISMATCH = "mismatch"  # the generated SQL ran but returned a different result
UNSUPPORTED = "unsupported"  # sqlglot reported that it can't express the query
WEAK = "weak"  # results match, but there are no rows or only NULLs, so the check is weak
PASS = "pass"
SKIPPED = "skipped"

STATUSES = [INVALID_CASE, SQLGLOT_ERROR, ERROR, MISMATCH, UNSUPPORTED, WEAK, PASS, SKIPPED]
PASSING = {PASS, WEAK, SKIPPED}
MAX_SHOWN_ROWS = 20


@dataclass
class Leg:
    """
    One check of a case. Names are "source:<read>" (the ground truth), "parse:<read>",
    "roundtrip:<read>" and "transpile:<read>-><write>".
    """

    name: str
    status: str
    sql: str | None = None
    reason: str = ""
    details: dict[str, t.Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    rows: list[str] | None = None
    seconds: float = 0.0

    @property
    def failed(self) -> bool:
        return self.status not in PASSING


@dataclass
class CaseResult:
    case: Case
    legs: list[Leg]

    def key(self, leg: Leg) -> str:
        return f"{self.case.id} {leg.name}"


class Runner:
    def __init__(
        self,
        engines: dict[str, Engine],
        targets: t.Collection[str] | None = None,
        show_rows: bool = False,
    ) -> None:
        self.engines = engines
        self.targets = set(targets) if targets is not None else set(engines)
        self.show_rows = show_rows

    def applicable(self, case: Case) -> bool:
        return case.read in self.engines

    def run(
        self, cases: t.Iterable[Case], progress: t.Callable[[CaseResult], None]
    ) -> list[CaseResult]:
        results = []
        for case in cases:
            if self.applicable(case):
                result = self.run_case(case)
                results.append(result)
                progress(result)
        return results

    def run_case(self, case: Case) -> CaseResult:
        source = self.engines[case.read]
        source_leg = f"source:{case.read}"

        if case.skip:
            return CaseResult(case, [Leg(source_leg, SKIPPED, case.sql, case.skip)])

        start = time.perf_counter()
        try:
            truth = source.query(case.sql)
        except EngineError as e:
            return CaseResult(case, [Leg(source_leg, INVALID_CASE, case.sql, str(e))])

        legs = [
            Leg(
                source_leg,
                WEAK if truth.vacuous else PASS,
                case.sql,
                "no rows or only NULLs" if truth.vacuous else "",
                rows=self._rows(truth),
                seconds=time.perf_counter() - start,
            )
        ]

        try:
            expression = sqlglot.parse_one(case.sql, read=source.dialect)
        except Exception as e:
            legs.append(Leg(f"parse:{case.read}", SQLGLOT_ERROR, None, *_describe_exception(e)))
            return CaseResult(case, legs)

        ordered = case.ordered if case.ordered is not None else _has_top_level_order(expression)
        tables = _referenced_tables(expression)

        legs.append(
            self._run_leg(f"roundtrip:{case.read}", case, expression, source, truth, ordered)
        )

        for target in self._targets(case):
            engine = self.engines[target]
            # Cases that query engine-specific tables can only be checked on those engines
            if any(
                target not in TABLES_BY_NAME[name].engines for name in tables & set(TABLES_BY_NAME)
            ):
                continue
            legs.append(
                self._run_leg(
                    f"transpile:{case.read}->{target}", case, expression, engine, truth, ordered
                )
            )

        return CaseResult(case, legs)

    def _targets(self, case: Case) -> list[str]:
        targets = case.write if case.write is not None else self.engines
        return sorted(
            t for t in targets if t != case.read and t in self.targets and t in self.engines
        )

    def _run_leg(
        self,
        name: str,
        case: Case,
        expression: exp.Expr,
        engine: Engine,
        truth: Result,
        ordered: bool,
    ) -> Leg:
        start = time.perf_counter()
        try:
            sql = expression.copy().sql(dialect=engine.dialect, unsupported_level=ErrorLevel.RAISE)
        except UnsupportedError as e:
            return Leg(name, UNSUPPORTED, None, str(e))
        except Exception as e:
            return Leg(name, SQLGLOT_ERROR, None, *_describe_exception(e))

        passed = WEAK if truth.vacuous else PASS
        if name.startswith("roundtrip:") and sql == case.sql:
            return Leg(name, passed, sql, "identical to the source SQL, not executed")

        try:
            result = engine.query(sql)
        except EngineError as e:
            return Leg(name, ERROR, sql, str(e), seconds=time.perf_counter() - start)

        comparison = compare_results(
            truth,
            result,
            ordered=ordered,
            timezone_name=engine.timezone,
            rel_tol=case.rel_tol or FLOAT_REL_TOL,
        )
        return Leg(
            name,
            passed if comparison.equal else MISMATCH,
            sql,
            comparison.reason,
            comparison.details,
            comparison.warnings,
            rows=self._rows(result),
            seconds=time.perf_counter() - start,
        )

    def _rows(self, result: Result) -> list[str] | None:
        if not self.show_rows:
            return None
        rows = [format_row(row) for row in result.rows[:MAX_SHOWN_ROWS]]
        if len(result.rows) > MAX_SHOWN_ROWS:
            rows.append(f"... {len(result.rows) - MAX_SHOWN_ROWS} more rows")
        return rows


@dataclass
class BaselineDiff:
    # Failing legs that the baseline doesn't list (including new cases)
    new_failures: list[str] = field(default_factory=list)
    # Baseline failures that now pass
    fixed: list[str] = field(default_factory=list)
    # Legs that still fail, but with a different status than recorded
    changed: list[tuple[str, str, str]] = field(default_factory=list)


def load_baseline(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))["known_failures"]


def diff_baseline(results: list[CaseResult], baseline: dict[str, str]) -> BaselineDiff:
    diff = BaselineDiff()
    for result in results:
        for leg in result.legs:
            if leg.status == SKIPPED:
                continue
            key = result.key(leg)
            known = baseline.get(key)
            if leg.failed and known is None:
                diff.new_failures.append(key)
            elif not leg.failed and known is not None:
                diff.fixed.append(key)
            elif leg.failed and known != leg.status:
                diff.changed.append((key, t.cast(str, known), leg.status))
    return diff


def write_baseline(
    path: Path, results: list[CaseResult], baseline: dict[str, str], complete: bool
) -> dict[str, str]:
    """
    Records the failing legs of `results`. Unless the run covered every case (`complete`), the
    entries of cases that didn't run are kept.
    """
    ran = {result.case.id for result in results}
    failures = {} if complete else {k: v for k, v in baseline.items() if k.split(" ")[0] not in ran}
    for result in results:
        for leg in result.legs:
            if leg.failed:
                failures[result.key(leg)] = leg.status

    path.write_text(
        json.dumps(
            {
                "description": "Conformance legs that are known to fail. Update with "
                "`python -m conformance run --update-baseline`.",
                "known_failures": dict(sorted(failures.items())),
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return failures


def write_report(
    path: Path,
    results: list[CaseResult],
    engines: dict[str, Engine],
    diff: BaselineDiff | None,
    seconds: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "sqlglot": sqlglot.__version__,
        "engines": {
            name: {"version": engine.version(), "dialect": engine.dialect, **engine.settings()}
            for name, engine in engines.items()
        },
        "seconds": round(seconds, 2),
        "summary": summarize(results),
        "baseline": asdict(diff) if diff else None,
        "cases": [
            {
                "id": result.case.id,
                "location": result.case.location,
                "read": result.case.read,
                "sql": result.case.sql,
                "legs": [{**asdict(leg), "seconds": round(leg.seconds, 3)} for leg in result.legs],
            }
            for result in results
        ],
    }
    path.write_text(
        json.dumps(report, indent=2, default=str, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def summarize(results: list[CaseResult]) -> dict[str, dict[str, int]]:
    """Counts statuses per leg kind, e.g. {"transpile:postgres->spark": {"pass": 10, ...}}."""
    summary: dict[str, dict[str, int]] = {}
    for result in results:
        for leg in result.legs:
            counts = summary.setdefault(leg.name, {})
            counts[leg.status] = counts.get(leg.status, 0) + 1
    return dict(sorted(summary.items()))


def _has_top_level_order(expression: exp.Expr) -> bool:
    return isinstance(expression, exp.Query) and bool(expression.args.get("order"))


def _referenced_tables(expression: exp.Expr) -> set[str]:
    ctes = {cte.alias_or_name.lower() for cte in expression.find_all(exp.CTE)}
    return {
        table.name.lower()
        for table in expression.find_all(exp.Table)
        if table.name and table.name.lower() not in ctes
    }


def _describe_exception(error: Exception) -> tuple[str, dict[str, t.Any]]:
    frames = traceback.format_exception(type(error), error, error.__traceback__)
    return f"{type(error).__name__}: {error}", {"traceback": "".join(frames)[-4000:]}
