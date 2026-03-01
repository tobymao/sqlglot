"""Emitter: orchestrates rules to build a PipeQuery from an AST."""

from __future__ import annotations

from sqlglot import exp

from .result import PipeOperator, PipeOpType, PipeQuery
from .rules import (
    aggregate_rule,
    cte_rule,
    from_rule,
    join_rule,
    projection_rule,
    setop_rule,
    terminal_rule,
    where_rule,
)


def _is_distinct_without_agg(ast: exp.Select) -> bool:
    if not ast.args.get("distinct"):
        return False
    if ast.args.get("group"):
        return False
    if aggregate_rule.has_aggregates(ast):
        return False
    return True


def _emit_distinct_as_aggregate(
    ast: exp.Select, dialect: str = "sqlite"
) -> list[PipeOperator]:
    select_exprs = ast.expressions
    group_cols = []
    for expr in select_exprs:
        if isinstance(expr, exp.Star):
            return []
        group_cols.append(expr.sql(dialect=dialect))
    if not group_cols:
        return []
    return [
        PipeOperator(
            op_type=PipeOpType.AGGREGATE,
            sql_fragment="AGGREGATE GROUP BY " + ", ".join(group_cols),
        )
    ]


def _collect_select_column_names(ast: exp.Select) -> set[str]:
    """Collect column names directly available in the SELECT output.

    Only includes top-level Column references and Alias names, NOT columns
    buried inside function arguments (which aren't available after CTE wrapping).
    """
    names = set()
    for expr in ast.expressions:
        if isinstance(expr, exp.Star):
            continue
        if isinstance(expr, exp.Alias):
            names.add(expr.alias.upper())
            # Also add the inner column name if it's a direct column reference
            if isinstance(expr.this, exp.Column):
                names.add(expr.this.name.upper())
        elif isinstance(expr, exp.Column):
            names.add(expr.name.upper())
    return names


def _order_refs_outside_select(ast: exp.Select) -> bool:
    order = ast.args.get("order")
    if not order:
        return False
    if any(isinstance(e, exp.Star) for e in ast.expressions):
        return False
    select_cols = _collect_select_column_names(ast)
    for order_expr in order.expressions:
        # Check for aggregate functions in ORDER BY
        has_agg = any(isinstance(n, exp.AggFunc) for n in order_expr.walk())
        if has_agg:
            continue  # Handled separately
        for col in order_expr.find_all(exp.Column):
            if col.name.upper() not in select_cols:
                return True
    return False


def _order_has_agg_func(ast: exp.Select) -> bool:
    order = ast.args.get("order")
    if not order:
        return False
    for order_expr in order.expressions:
        for node in order_expr.walk():
            if isinstance(node, exp.AggFunc):
                return True
    return False


def _collect_order_agg_exprs(ast: exp.Select, dialect: str = "sqlite") -> list[tuple[str, str]]:
    """Collect aggregate expressions from ORDER BY that aren't in SELECT.

    Returns list of (agg_sql, alias) tuples.
    """
    order = ast.args.get("order")
    if not order:
        return []

    # Build set of aggregate SQL already in SELECT (with aliases)
    select_agg_sqls = set()
    for expr in ast.expressions:
        if aggregate_rule._has_agg_func(expr):
            if isinstance(expr, exp.Alias):
                select_agg_sqls.add(expr.this.sql(dialect=dialect).upper())
            else:
                select_agg_sqls.add(expr.sql(dialect=dialect).upper())

    result = []
    counter = 0
    seen = set()
    for order_expr in order.expressions:
        inner = order_expr.this if hasattr(order_expr, "this") else order_expr
        if any(isinstance(n, exp.AggFunc) for n in inner.walk()):
            inner_sql = inner.sql(dialect=dialect)
            upper_sql = inner_sql.upper()
            if upper_sql not in select_agg_sqls and upper_sql not in seen:
                counter += 1
                alias = f"_ord{counter}"
                result.append((inner_sql, alias))
                seen.add(upper_sql)

    return result


def _build_order_alias_map(ast: exp.Select, extra_aliases: list[tuple[str, str]], dialect: str = "sqlite") -> dict[str, str]:
    """Build a mapping of aggregate SQL -> alias for ORDER BY substitution."""
    agg_map = {}

    # From SELECT list
    agg_counter = 0
    for expr in ast.expressions:
        if aggregate_rule._has_agg_func(expr):
            if isinstance(expr, exp.Alias):
                agg_map[expr.this.sql(dialect=dialect).upper()] = expr.alias
            else:
                agg_counter += 1
                agg_map[expr.sql(dialect=dialect).upper()] = f"_agg{agg_counter}"

    # From extra ORDER BY aggregates
    for agg_sql, alias in extra_aliases:
        agg_map[agg_sql.upper()] = alias

    return agg_map


def emit_pipe_query(ast: exp.Expression, dialect: str = "sqlite") -> PipeQuery:
    """Convert a parsed SQL AST into a PipeQuery structure."""
    # Handle set operations
    if isinstance(ast, (exp.Union, exp.Intersect, exp.Except)):
        result = setop_rule.transform(ast, dialect=dialect)
        if result:
            return result

    if not isinstance(ast, exp.Select):
        return PipeQuery(
            operators=[
                PipeOperator(
                    op_type=PipeOpType.FROM,
                    sql_fragment=ast.sql(dialect=dialect),
                )
            ]
        )

    query = PipeQuery()

    # Extract CTEs
    ctes, cte_names = cte_rule.extract_ctes(ast, None, dialect=dialect)
    query.ctes = ctes
    query.cte_names = cte_names

    # Check for FROM
    from_op = from_rule.extract(ast, dialect=dialect)
    if not from_op:
        return PipeQuery(
            operators=[
                PipeOperator(
                    op_type=PipeOpType.SELECT,
                    sql_fragment="SELECT " + ", ".join(
                        e.sql(dialect=dialect) for e in ast.expressions
                    ),
                )
            ],
            ctes=ctes,
            cte_names=cte_names,
        )

    # FROM
    query.operators.append(from_op)

    # JOINs
    join_ops = join_rule.linearize(ast, dialect=dialect)
    query.operators.extend(join_ops)

    # WHERE
    where_op = where_rule.promote(ast, dialect=dialect)
    if where_op:
        query.operators.append(where_op)

    # Analyze query properties
    order_outside_select = _order_refs_outside_select(ast)
    has_group = ast.args.get("group") is not None
    has_agg = aggregate_rule.has_aggregates(ast)
    order_has_agg = _order_has_agg_func(ast)

    if _is_distinct_without_agg(ast):
        if order_outside_select:
            # ORDER BY references columns not in SELECT
            # Emit ORDER BY before DISTINCT-as-AGGREGATE, then LIMIT after
            order_ops = terminal_rule.emit_order_only(ast, dialect=dialect)
            query.operators.extend(order_ops)
            distinct_ops = _emit_distinct_as_aggregate(ast, dialect=dialect)
            query.operators.extend(distinct_ops)
            limit_ops = terminal_rule.emit_limit_only(ast, dialect=dialect)
            query.operators.extend(limit_ops)
        else:
            distinct_ops = _emit_distinct_as_aggregate(ast, dialect=dialect)
            query.operators.extend(distinct_ops)
            terminal_ops = terminal_rule.emit(ast, dialect=dialect)
            query.operators.extend(terminal_ops)

    elif has_group or has_agg:
        # Collect ORDER BY aggregate expressions not in SELECT
        extra_order_aggs = _collect_order_agg_exprs(ast, dialect=dialect) if order_has_agg else []

        # AGGREGATE rule (with extra ORDER BY aggregates)
        agg_ops, grp_aliases = aggregate_rule.emit(ast, dialect=dialect, extra_agg_exprs=extra_order_aggs)
        query.operators.extend(agg_ops)

        if order_has_agg:
            # ORDER BY with aggregate refs → use aliases, ORDER BY before SELECT
            agg_map = _build_order_alias_map(ast, extra_order_aggs, dialect=dialect)
            order_ops = _emit_order_with_aliases(ast, agg_map, dialect=dialect)
            query.operators.extend(order_ops)

            # SELECT after ORDER BY to prune extra columns
            select_op = projection_rule.emit(ast, has_aggregate=True, dialect=dialect, group_expr_aliases=grp_aliases)
            if select_op:
                query.operators.append(select_op)

            limit_ops = terminal_rule.emit_limit_only(ast, dialect=dialect)
            query.operators.extend(limit_ops)
        else:
            if order_outside_select:
                # ORDER BY references columns not in SELECT output;
                # emit ORDER BY before SELECT so GROUP BY columns are still available
                # Strip qualifiers since ORDER BY follows AGGREGATE (CTE context)
                order_ops = terminal_rule.emit_order_only(ast, dialect=dialect, strip_qualifiers=True)
                query.operators.extend(order_ops)
                select_op = projection_rule.emit(ast, has_aggregate=True, dialect=dialect, group_expr_aliases=grp_aliases)
                if select_op:
                    query.operators.append(select_op)
                limit_ops = terminal_rule.emit_limit_only(ast, dialect=dialect)
                query.operators.extend(limit_ops)
            else:
                # SELECT then ORDER BY then LIMIT
                select_op = projection_rule.emit(ast, has_aggregate=True, dialect=dialect, group_expr_aliases=grp_aliases)
                if select_op:
                    query.operators.append(select_op)
                terminal_ops = terminal_rule.emit(ast, dialect=dialect)
                query.operators.extend(terminal_ops)

    else:
        # No aggregation
        if order_outside_select:
            order_ops = terminal_rule.emit_order_only(ast, dialect=dialect)
            query.operators.extend(order_ops)
            select_op = projection_rule.emit(ast, has_aggregate=False, dialect=dialect)
            if select_op:
                query.operators.append(select_op)
            limit_ops = terminal_rule.emit_limit_only(ast, dialect=dialect)
            query.operators.extend(limit_ops)
        else:
            select_op = projection_rule.emit(ast, has_aggregate=False, dialect=dialect)
            if select_op:
                query.operators.append(select_op)
            terminal_ops = terminal_rule.emit(ast, dialect=dialect)
            query.operators.extend(terminal_ops)

    # When the main query has both explicit CTEs and pipe operators that create
    # implicit CTEs (SELECT), the transpiler can't merge them correctly.
    # Fall back to standard SQL for the entire query in this case.
    if query.ctes and any(op.op_type == PipeOpType.SELECT for op in query.operators):
        fallback_sql = ast.sql(dialect=dialect)
        return PipeQuery(
            operators=[PipeOperator(op_type=PipeOpType.FROM, sql_fragment=fallback_sql)]
        )

    return query


def _emit_order_with_aliases(
    ast: exp.Select, agg_map: dict[str, str], dialect: str = "sqlite"
) -> list[PipeOperator]:
    """Emit ORDER BY with aggregate functions replaced by their aliases."""
    order = ast.args.get("order")
    if not order:
        return []

    order_parts = []
    for order_expr in order.expressions:
        inner = order_expr.this if hasattr(order_expr, "this") else order_expr
        inner_sql = inner.sql(dialect=dialect).upper()

        if inner_sql in agg_map:
            alias = agg_map[inner_sql]
            desc = order_expr.args.get("desc") if hasattr(order_expr, "args") else False
            suffix = " DESC" if desc else ""
            order_parts.append(f"{alias}{suffix}")
        else:
            order_parts.append(order_expr.sql(dialect=dialect))

    return [
        PipeOperator(
            op_type=PipeOpType.ORDER_BY,
            sql_fragment="ORDER BY " + ", ".join(order_parts),
        )
    ]
