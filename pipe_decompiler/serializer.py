"""Serializer: convert PipeQuery to formatted pipe SQL string."""

from __future__ import annotations

from .result import PipeQuery


def serialize(pipe_query: PipeQuery) -> str:
    """Convert a PipeQuery into a pipe SQL string."""
    parts = []

    # Emit CTEs
    if pipe_query.ctes:
        cte_parts = []
        for name, cte_query in pipe_query.ctes:
            cte_body = serialize(cte_query)
            cte_parts.append(f"{name} AS ({cte_body})")
        parts.append("WITH " + ", ".join(cte_parts))

    # Emit operators
    for i, op in enumerate(pipe_query.operators):
        if i == 0:
            # First operator: no |> prefix (comes right after CTE or is the start)
            parts.append(op.sql_fragment)
        else:
            parts.append(f"|> {op.sql_fragment}")

    return " ".join(parts)
