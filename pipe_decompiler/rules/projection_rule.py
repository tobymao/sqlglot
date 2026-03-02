"""Projection rule: emit SELECT operator for the final column list."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def _has_agg_func(expr: exp.Expression) -> bool:
    for node in expr.walk():
        if isinstance(node, exp.Window):
            return False
        if isinstance(node, exp.AggFunc):
            return True
    return False


def _has_window_func(expr: exp.Expression) -> bool:
    for node in expr.walk():
        if isinstance(node, exp.Window):
            return True
    return False


def _strip_table_qualifier_ast(expr: exp.Expression, dialect: str = "sqlite") -> str:
    """Remove table qualifiers from all column references in an expression using AST.

    Handles all patterns including quoted identifiers and function arguments.
    """
    result = expr.copy()
    for col in result.find_all(exp.Column):
        col.set("table", None)
    return result.sql(dialect=dialect)


def emit(
    ast: exp.Select,
    has_aggregate: bool,
    dialect: str = "sqlite",
    group_expr_aliases: dict[str, str] | None = None,
    has_window: bool = False,
) -> PipeOperator | None:
    """Emit a SELECT pipe operator for the projection.

    SELECT * is omitted (pipe default).
    After AGGREGATE, uses aliases for aggregate columns and strips table qualifiers.
    After EXTEND (window), uses aliases for window columns.
    group_expr_aliases: mapping from GROUP BY expression SQL to alias, for function-call
    GROUP BY expressions that need alias references after CTE wrapping.
    """
    select_exprs = ast.expressions

    if len(select_exprs) == 1 and isinstance(select_exprs[0], exp.Star):
        return None

    if has_aggregate:
        group = ast.args.get("group")

        if not group:
            # Aggregates without GROUP BY - no SELECT needed after AGGREGATE
            # since all columns are already in AGGREGATE output
            return None

        grp_aliases = group_expr_aliases or {}
        parts = []
        agg_counter = 0
        for expr in select_exprs:
            if _has_agg_func(expr):
                if isinstance(expr, exp.Alias):
                    parts.append(expr.alias)
                else:
                    agg_counter += 1
                    parts.append(f"_agg{agg_counter}")
            elif has_window and _has_window_func(expr):
                # Window column: reference by alias after EXTEND
                if isinstance(expr, exp.Alias):
                    parts.append(expr.alias)
                else:
                    parts.append(expr.sql(dialect=dialect))
            else:
                # Non-aggregate column - check for GROUP BY expression alias first
                stripped = _strip_table_qualifier_ast(expr, dialect=dialect)
                if stripped.upper() in grp_aliases:
                    parts.append(grp_aliases[stripped.upper()])
                else:
                    parts.append(stripped)

        if not parts:
            return None

        return PipeOperator(
            op_type=PipeOpType.SELECT,
            sql_fragment="SELECT " + ", ".join(parts),
        )
    else:
        parts = []
        for expr in select_exprs:
            if has_window and _has_window_func(expr):
                # Window column: reference by alias after EXTEND
                # Strip table qualifiers (CTE context after EXTEND)
                if isinstance(expr, exp.Alias):
                    parts.append(expr.alias)
                else:
                    parts.append(_strip_table_qualifier_ast(expr, dialect=dialect))
            else:
                parts.append(expr.sql(dialect=dialect))

        if not parts:
            return None

        return PipeOperator(
            op_type=PipeOpType.SELECT,
            sql_fragment="SELECT " + ", ".join(parts),
        )
