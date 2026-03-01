"""Main decompiler entry point: standard SQL → pipe SQL."""

from __future__ import annotations

import sqlglot

from .emitter import emit_pipe_query
from .serializer import serialize


def decompile(sql: str, dialect: str = "sqlite") -> str:
    """Decompile standard SQL into pipe SQL.

    Args:
        sql: A standard SQL query string.
        dialect: The SQL dialect to parse with (default: sqlite).

    Returns:
        A pipe SQL string.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)
    pipe_query = emit_pipe_query(ast, dialect=dialect)
    return serialize(pipe_query)
