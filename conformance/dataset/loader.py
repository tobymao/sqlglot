"""Loads the dataset into an engine and proves that the engine stores exactly what was intended."""

from __future__ import annotations

import json
import typing as t

from conformance.compare import compare_results
from conformance.dataset import TABLES
from conformance.dataset.model import Table, shared
from conformance.engines import Engine, EngineError
from conformance.result import Result

META_TABLE = "conformance_meta"


class DatasetError(Exception):
    """A table doesn't read back as the rows it was loaded with."""


def ensure_dataset(
    engine: Engine,
    tables: list[Table] | None = None,
    force: bool = False,
    log: t.Callable[[str], None] = print,
) -> list[str]:
    """
    Loads every table whose definition or data changed since it was last loaded into `engine`.
    Returns the names of the tables that were (re)loaded.
    """
    loaded = _read_meta(engine)
    reloaded = []

    for table in tables if tables is not None else TABLES:
        if engine.name not in table.engines:
            continue
        if not force and loaded.get(table.name) == table.fingerprint:
            continue

        log(f"Loading {table.name} into {engine.name} ({len(table.rows)} rows)")
        engine.load_table(table)
        verify_table(engine, table)

        loaded[table.name] = table.fingerprint
        _write_meta(engine, loaded)
        reloaded.append(table.name)

    return reloaded


def verify_table(engine: Engine, table: Table) -> None:
    names = ", ".join(column.name for column in table.columns)
    actual = engine.query(f"SELECT {names} FROM {table.name}")
    expected = Result(
        columns=[column.name for column in table.columns],
        types=["dataset"] * len(table.columns),
        families=actual.families,
        rows=table.expected_rows(),
    )

    comparison = compare_results(expected, actual, ordered=False, timezone_name=engine.timezone)
    if not comparison.equal:
        details = json.dumps(comparison.details, indent=2, default=str)
        raise DatasetError(
            f"{table.name} doesn't read back correctly from {engine.name}: "
            f"{comparison.reason}\n{details}"
        )


def _read_meta(engine: Engine) -> dict[str, str]:
    try:
        return dict(
            t.cast(t.Iterable, engine.query(f"SELECT name, fingerprint FROM {META_TABLE}").rows)
        )
    except EngineError:
        return {}


def _write_meta(engine: Engine, loaded: dict[str, str]) -> None:
    columns = [shared("name", "TEXT", "STRING"), shared("fingerprint", "TEXT", "STRING")]
    rows = sorted(loaded.items())
    engine.load_table(Table(META_TABLE, columns, lambda: rows))
