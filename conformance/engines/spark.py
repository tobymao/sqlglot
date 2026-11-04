from __future__ import annotations

import os
import socket
import threading
import typing as t
from urllib.parse import urlparse

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
    "BooleanType": "boolean",
    "ByteType": "integer",
    "ShortType": "integer",
    "IntegerType": "integer",
    "LongType": "integer",
    "DecimalType": "decimal",
    "FloatType": "float32",
    "DoubleType": "float",
    "StringType": "string",
    "CharType": "string",
    "VarcharType": "string",
    "BinaryType": "binary",
    "DateType": "date",
    "TimeType": "time",
    # Spark's TIMESTAMP is an instant rendered in the session time zone (TIMESTAMP_LTZ)
    "TimestampType": "timestamptz",
    "TimestampNTZType": "timestamp",
    "DayTimeIntervalType": "interval",
    "YearMonthIntervalType": "interval",
    "CalendarIntervalType": "interval",
    "ArrayType": "array",
    "MapType": "map",
    "StructType": "struct",
    "VariantType": "variant",
    "NullType": "null",
}

_CONNECTION_ERRORS = ("StatusCode.UNAVAILABLE", "Connection refused", "failed to connect")


class Spark(Engine):
    """Spark, reached through Spark Connect so that no local JVM is needed."""

    name = "spark"
    dialect = "spark"
    service = "spark"

    def __init__(self, timezone: str = "UTC") -> None:
        super().__init__(timezone)
        port = os.environ.get("CONFORMANCE_SPARK_PORT", "55002")
        self.remote = os.environ.get("CONFORMANCE_SPARK_REMOTE") or f"sc://127.0.0.1:{port}"
        self._session: t.Any = None

    @property
    def external(self) -> bool:
        return "CONFORMANCE_SPARK_REMOTE" in os.environ

    def connect(self) -> None:
        if self._session is not None:
            return

        # The Spark Connect client retries unreachable servers for minutes, so fail fast instead
        url = urlparse(self.remote.split(";")[0])
        try:
            socket.create_connection((url.hostname or "localhost", url.port or 15002), 5).close()
        except OSError as e:
            raise EngineUnavailable(f"can't connect to Spark at '{self.remote}': {e}") from e

        from pyspark.logger import PySparkLogger
        from pyspark.sql import SparkSession

        # PySpark logs every failed query with its full JVM stack trace
        for logger in ("SQLQueryContextLogger", "DataFrameQueryContextLogger"):
            PySparkLogger.getLogger(logger).disabled = True

        session = SparkSession.builder.remote(self.remote).getOrCreate()
        try:
            session.conf.set("spark.sql.session.timeZone", self.timezone)
        except Exception as e:
            session.stop()
            raise EngineUnavailable(f"can't connect to Spark at '{self.remote}': {e}") from e

        self._session = session

    def close(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None

    def version(self) -> str:
        self.connect()
        return self._session.version

    def settings(self) -> dict[str, str]:
        self.connect()
        return {
            "timezone": self.timezone,
            "spark.sql.ansi.enabled": self._session.conf.get("spark.sql.ansi.enabled"),
        }

    def query(self, sql: str) -> Result:
        self.connect()
        session = self._session

        timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, session.interruptAll)
        timer.start()
        try:
            df = session.sql(sql)
            schema = df.schema
            table = df.toArrow()
        except Exception as e:
            message = _error_message(e)
            if any(marker in message for marker in _CONNECTION_ERRORS):
                raise EngineUnavailable(f"lost the connection to Spark: {message}") from e
            raise EngineError(message) from e
        finally:
            timer.cancel()

        # Arrow is used instead of collect() because it keeps time zones and duplicate column names
        try:
            columns = [table.column(i).to_pylist() for i in range(table.num_columns)]
        except (OverflowError, ValueError) as e:
            raise EngineError(f"the result can't be represented in Python: {e}") from e
        return Result(
            columns=[f.name for f in schema.fields],
            types=[f.dataType.simpleString() for f in schema.fields],
            families=[_FAMILIES.get(type(f.dataType).__name__, "other") for f in schema.fields],
            rows=list(zip(*columns)),
        )

    def load_table(self, table: Table) -> None:
        from pyspark.sql import types

        self.connect()
        session = self._session
        session.sql(f"DROP TABLE IF EXISTS {table.name}")
        session.sql(f"CREATE TABLE {table.name} ({self.column_definitions(table)}) USING parquet")

        if table.rows:
            # CHAR/VARCHAR are only allowed in table schemas; their padding and length checks
            # are applied by the table on insert
            schema = types.StructType(
                [
                    types.StructField(f.name, _without_char_types(f.dataType), f.nullable)
                    for f in session.table(table.name).schema.fields
                ]
            )
            session.createDataFrame(table.rows, schema=schema).write.insertInto(table.name)


def _without_char_types(data_type: t.Any) -> t.Any:
    from pyspark.sql import types

    if isinstance(data_type, (types.CharType, types.VarcharType)):
        return types.StringType()
    if isinstance(data_type, types.ArrayType):
        return types.ArrayType(_without_char_types(data_type.elementType), data_type.containsNull)
    if isinstance(data_type, types.MapType):
        return types.MapType(
            _without_char_types(data_type.keyType),
            _without_char_types(data_type.valueType),
            data_type.valueContainsNull,
        )
    if isinstance(data_type, types.StructType):
        return types.StructType(
            [
                types.StructField(f.name, _without_char_types(f.dataType), f.nullable)
                for f in data_type.fields
            ]
        )
    return data_type


def _error_message(error: Exception) -> str:
    return str(error).split("\n\nJVM stacktrace:")[0].strip()
