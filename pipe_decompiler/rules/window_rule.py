"""Window rule: emit window functions as EXTEND operators and QUALIFY as WHERE."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def has_window_functions(ast: exp.Select) -> bool:
    """Check if the SELECT list has any window function expressions."""
    for expr in ast.expressions:
        if _is_window_expr(expr):
            return True
    return False


def _is_window_expr(expr: exp.Expression) -> bool:
    """Check if an expression contains a window function."""
    for node in expr.walk():
        if isinstance(node, exp.Window):
            return True
    return False


def emit(ast: exp.Select, dialect: str = "sqlite") -> list[PipeOperator]:
    """Emit EXTEND operators for window functions and WHERE for QUALIFY.

    Window functions in SELECT become |> EXTEND expressions.
    QUALIFY clause becomes |> WHERE after the EXTEND.
    """
    operators = []

    # Collect window function expressions from SELECT
    window_exprs = []
    for expr in ast.expressions:
        if _is_window_expr(expr):
            window_exprs.append(expr)

    # Emit combined EXTEND for all window functions
    if window_exprs:
        extend_parts = [e.sql(dialect=dialect) for e in window_exprs]
        operators.append(
            PipeOperator(
                op_type=PipeOpType.EXTEND,
                sql_fragment="EXTEND " + ", ".join(extend_parts),
            )
        )

    # Convert QUALIFY to WHERE
    qualify = ast.args.get("qualify")
    if qualify:
        operators.append(
            PipeOperator(
                op_type=PipeOpType.WHERE,
                sql_fragment=f"WHERE {qualify.this.sql(dialect=dialect)}",
            )
        )

    return operators


def get_non_window_exprs(ast: exp.Select) -> list[exp.Expression]:
    """Return SELECT expressions that are NOT window functions."""
    return [expr for expr in ast.expressions if not _is_window_expr(expr)]
