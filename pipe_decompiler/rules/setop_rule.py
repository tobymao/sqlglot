"""Set operation rule: transform UNION/INTERSECT/EXCEPT into pipe operators."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType, PipeQuery


def _collect_setops(
    node: exp.Expression,
) -> list[tuple[type | None, bool, exp.Expression]]:
    """Collect all branches of a set operation tree (left-to-right)."""
    if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
        left = _collect_setops(node.this)
        distinct = node.args.get("distinct", True)
        right_node = node.expression
        left.append((type(node), distinct, right_node))
        return left
    else:
        return [(None, False, node)]


def _setop_keyword(cls: type, distinct: bool) -> str:
    name = {exp.Union: "UNION", exp.Intersect: "INTERSECT", exp.Except: "EXCEPT"}[cls]
    if not distinct:
        name += " ALL"
    return name


def transform(ast: exp.Expression, dialect: str = "sqlite") -> PipeQuery | None:
    """Transform a set operation into a PipeQuery.

    Strategy: for each branch, generate standard SQL directly from the AST.
    Combine with set operation keywords. This avoids nested CTE issues.
    The output is standard SQL (not pipe SQL), but wrapped in a PipeQuery for consistency.
    """
    if not isinstance(ast, (exp.Union, exp.Intersect, exp.Except)):
        return None

    branches = _collect_setops(ast)
    if not branches:
        return None

    # Generate standard SQL for each branch directly from AST
    parts = []
    for i, (op_cls, distinct, branch_node) in enumerate(branches):
        branch_sql = branch_node.sql(dialect=dialect)

        if i == 0:
            parts.append(branch_sql)
        else:
            keyword = _setop_keyword(op_cls, distinct)
            parts.append(f"{keyword} {branch_sql}")

    combined_sql = " ".join(parts)

    query = PipeQuery(
        operators=[
            PipeOperator(op_type=PipeOpType.FROM, sql_fragment=combined_sql)
        ]
    )

    # Handle CTEs from the outer set operation
    with_ = ast.args.get("with_")
    if with_:
        from .cte_rule import extract_ctes

        ctes, cte_names = extract_ctes(ast, None, dialect=dialect)
        query.ctes = ctes
        query.cte_names = cte_names

    return query
