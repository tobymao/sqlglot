"""Terminal rule: emit ORDER BY and LIMIT pipe operators."""

from __future__ import annotations

import re

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def _strip_table_qualifiers(sql: str) -> str:
    """Strip table qualifiers from column references (T2.col -> col)."""
    return re.sub(r"\b(\w+)\.(\w+)\b", r"\2", sql)


def emit(
    ast: exp.Select, dialect: str = "sqlite", strip_qualifiers: bool = True
) -> list[PipeOperator]:
    """Emit terminal operators: ORDER BY, LIMIT, OFFSET.

    strip_qualifiers: when True, strip table qualifiers from ORDER BY
    (needed when ORDER BY follows a SELECT that wraps in a CTE).
    """
    operators = []
    operators.extend(emit_order_only(ast, dialect=dialect, strip_qualifiers=strip_qualifiers))
    operators.extend(emit_limit_only(ast, dialect=dialect))
    return operators


def emit_order_only(
    ast: exp.Select, dialect: str = "sqlite", strip_qualifiers: bool = False
) -> list[PipeOperator]:
    """Emit only ORDER BY operator."""
    order = ast.args.get("order")
    if not order:
        return []

    order_parts = [e.sql(dialect=dialect) for e in order.expressions]
    order_str = "ORDER BY " + ", ".join(order_parts)

    if strip_qualifiers:
        order_str = _strip_table_qualifiers(order_str)

    return [PipeOperator(op_type=PipeOpType.ORDER_BY, sql_fragment=order_str)]


def emit_limit_only(ast: exp.Select, dialect: str = "sqlite") -> list[PipeOperator]:
    """Emit only LIMIT/OFFSET operators."""
    limit = ast.args.get("limit")
    offset = ast.args.get("offset")

    if not limit and not offset:
        return []

    if limit:
        limit_str = f"LIMIT {limit.expression.sql(dialect=dialect)}"
        if offset:
            limit_str += f" OFFSET {offset.expression.sql(dialect=dialect)}"
        return [PipeOperator(op_type=PipeOpType.LIMIT, sql_fragment=limit_str)]

    return [
        PipeOperator(
            op_type=PipeOpType.LIMIT,
            sql_fragment=f"LIMIT ALL OFFSET {offset.expression.sql(dialect=dialect)}",
        )
    ]
