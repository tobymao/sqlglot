"""Main decompiler entry point: standard SQL → pipe SQL."""

from __future__ import annotations

import sqlglot

from .emitter import emit_pipe_query
from .preprocessor import preprocess
from .serializer import serialize


def decompile(
    sql: str,
    dialect: str = "sqlite",
    schema: dict | None = None,
) -> str:
    """Decompile standard SQL into pipe SQL.

    Args:
        sql: A standard SQL query string.
        dialect: The SQL dialect to parse with (default: sqlite).
        schema: Optional schema dict for column resolution.

    Returns:
        A pipe SQL string.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)
    ast = preprocess(ast, dialect=dialect, schema=schema)
    pipe_query = emit_pipe_query(ast, dialect=dialect)
    return serialize(pipe_query)
