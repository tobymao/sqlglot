"""FROM rule: extract the FROM clause as the first pipe operator."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def extract(ast: exp.Select, dialect: str = "sqlite") -> PipeOperator | None:
    """Extract FROM clause from AST and return a PipeOperator."""
    from_ = ast.args.get("from_")
    if not from_:
        return None

    table = from_.this
    return PipeOperator(
        op_type=PipeOpType.FROM,
        sql_fragment=f"FROM {table.sql(dialect=dialect)}",
    )
