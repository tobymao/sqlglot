from __future__ import annotations

import os
import typing as t

from conformance.engines.base import (
    STATEMENT_TIMEOUT_SECONDS,
    Engine,
    EngineError,
    EngineUnavailable,
)
from conformance.result import Result

if t.TYPE_CHECKING:
    from conformance.dataset.model import Table

_FAMILIES = {
    "bool": "boolean",
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "oid": "integer",
    "numeric": "decimal",
    "money": "decimal",
    "float4": "float32",
    "float8": "float",
    "text": "string",
    "varchar": "string",
    "bpchar": "string",
    "char": "string",
    "name": "string",
    "unknown": "string",
    "bytea": "binary",
    "date": "date",
    "time": "time",
    "timetz": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
    "interval": "interval",
    "json": "json",
    "jsonb": "json",
    "uuid": "uuid",
    "inet": "network",
    "cidr": "network",
    "void": "null",
}

# Postgres type categories (pg_type.typcategory) used when the type name isn't in _FAMILIES
_CATEGORY_FAMILIES = {"A": "array", "C": "struct", "P": "struct"}


class Postgres(Engine):
    name = "postgres"
    dialect = "postgres"
    service = "postgres"

    def __init__(self, timezone: str = "UTC") -> None:
        super().__init__(timezone)
        port = os.environ.get("CONFORMANCE_POSTGRES_PORT", "55432")
        self.dsn = os.environ.get("CONFORMANCE_POSTGRES_DSN") or (
            f"host=127.0.0.1 port={port} user=sqlglot password=sqlglot dbname=conformance"
        )
        self._connection: t.Any = None
        self._types: dict[int, tuple[str, str]] = {}

    @property
    def external(self) -> bool:
        return "CONFORMANCE_POSTGRES_DSN" in os.environ

    def connect(self) -> None:
        if self._connection is not None and not self._connection.closed:
            return

        import psycopg
        from psycopg.types.string import TextLoader

        try:
            connection = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
        except psycopg.OperationalError as e:
            raise EngineUnavailable(f"can't connect to Postgres at '{self.dsn}': {e}") from e

        # JSON is compared as text: Spark has no JSON type, and the comparator parses it anyway
        for type_name in ("json", "jsonb"):
            connection.adapters.register_loader(type_name, TextLoader)

        for setting, value in (
            ("TimeZone", self.timezone),
            ("DateStyle", "ISO, YMD"),
            ("IntervalStyle", "postgres"),
            ("statement_timeout", f"{STATEMENT_TIMEOUT_SECONDS}s"),
        ):
            connection.execute("SELECT set_config(%s, %s, false)", (setting, value))

        self._types = {
            oid: (name, category)
            for oid, name, category in connection.execute(
                "SELECT oid::INT, typname, typcategory FROM pg_type"
            )
        }
        self._connection = connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def version(self) -> str:
        return str(self.query("SHOW server_version").rows[0][0])

    def query(self, sql: str) -> Result:
        import psycopg

        self.connect()
        connection = self._connection
        try:
            # Rolling back keeps cases from leaving side effects behind
            with connection.transaction(force_rollback=True):
                cursor = connection.execute(sql)
                description = cursor.description or []
                rows = cursor.fetchall() if cursor.description else []
        except psycopg.Error as e:
            if connection.closed:
                raise EngineUnavailable(f"lost the connection to Postgres: {e}") from e
            raise EngineError(str(e).strip()) from e

        types = [self._types.get(d.type_code, (str(d.type_code), "")) for d in description]
        return Result(
            columns=[d.name for d in description],
            types=[name for name, _ in types],
            families=[
                _FAMILIES.get(name) or _CATEGORY_FAMILIES.get(category, name)
                for name, category in types
            ],
            rows=[tuple(row) for row in rows],
        )

    def load_table(self, table: Table) -> None:
        self.connect()
        connection = self._connection
        names = ", ".join(column.name for column in table.columns)

        with connection.transaction():
            connection.execute(f"DROP TABLE IF EXISTS {table.name}")
            connection.execute(f"CREATE TABLE {table.name} ({self.column_definitions(table)})")
            with connection.cursor() as cursor:
                with cursor.copy(f"COPY {table.name} ({names}) FROM STDIN") as copy:
                    for row in table.rows:
                        copy.write_row(row)
