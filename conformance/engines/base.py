from __future__ import annotations

import typing as t

from conformance.result import Result

if t.TYPE_CHECKING:
    from conformance.dataset.model import Table

# Applied to every statement so a pathological query can't stall a run
STATEMENT_TIMEOUT_SECONDS = 60


class EngineError(Exception):
    """The engine rejected or failed to execute a statement."""


class EngineUnavailable(Exception):
    """The engine can't be reached; unlike EngineError this aborts the run."""


class Engine:
    """
    A connection to a database engine that conformance cases run on.

    Subclasses are registered in conformance.engines.ENGINES. To add an engine, implement the
    abstract methods below, add a service to docker-compose.yml and add the engine's physical
    types to the dataset columns (conformance/dataset/tables.py).
    """

    # Engine name, also used as the key for physical types in the dataset
    name: t.ClassVar[str]
    # sqlglot dialect that targets this engine
    dialect: t.ClassVar[str]
    # Service name in docker-compose.yml
    service: t.ClassVar[str]

    def __init__(self, timezone: str = "UTC") -> None:
        self.timezone = timezone

    @property
    def external(self) -> bool:
        """True if the engine is provided by the user instead of the bundled Docker service."""
        return False

    def connect(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def version(self) -> str:
        raise NotImplementedError

    def settings(self) -> dict[str, str]:
        """Settings that affect query semantics, recorded in the report."""
        return {"timezone": self.timezone}

    def query(self, sql: str) -> Result:
        """Runs a statement and returns its rows. Raises EngineError if the engine rejects it."""
        raise NotImplementedError

    def load_table(self, table: Table) -> None:
        """Creates `table` (replacing any existing one) and inserts its rows."""
        raise NotImplementedError

    def ping(self) -> bool:
        try:
            self.connect()
            self.query("SELECT 1")
            return True
        except Exception:
            self.close()
            return False

    def column_definitions(self, table: Table) -> str:
        return ", ".join(f"{column.name} {column.types[self.name]}" for column in table.columns)
