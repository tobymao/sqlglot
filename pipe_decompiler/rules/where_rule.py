"""WHERE rule: promote WHERE clause to a pipe operator."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def promote(ast: exp.Select, dialect: str = "sqlite") -> PipeOperator | None:
    """Extract WHERE clause and return a pipe WHERE operator."""
    where = ast.args.get("where")
    if not where:
        return None

    return PipeOperator(
        op_type=PipeOpType.WHERE,
        sql_fragment=f"WHERE {where.this.sql(dialect=dialect)}",
    )
