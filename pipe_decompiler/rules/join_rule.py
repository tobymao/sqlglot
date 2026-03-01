"""JOIN rule: linearize joins into pipe operators."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def linearize(ast: exp.Select, dialect: str = "sqlite") -> list[PipeOperator]:
    """Convert each JOIN in the AST to a pipe JOIN operator."""
    joins = ast.args.get("joins") or []
    operators = []

    for join in joins:
        operators.append(
            PipeOperator(
                op_type=PipeOpType.JOIN,
                sql_fragment=join.sql(dialect=dialect),
            )
        )

    return operators
