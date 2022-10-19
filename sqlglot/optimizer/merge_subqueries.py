from collections import defaultdict

from sqlglot import expressions as exp
from sqlglot.helper import find_new_name
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.optimizer.simplify import simplify


def merge_subqueries(expression, leave_tables_isolated=False):
    """
    Rewrite sqlglot AST to merge derived tables into the outer query.

    This also merges CTEs if they are selected from only once.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT x.a FROM x) JOIN y")
        >>> merge_subqueries(expression).sql()
        'SELECT x.a FROM x JOIN y'

    If `leave_tables_isolated` is True, this will not merge inner queries into outer
    queries if it would result in multiple table selects in a single query:
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT x.a FROM x) JOIN y")
        >>> merge_subqueries(expression, leave_tables_isolated=True).sql()
        'SELECT a FROM (SELECT x.a FROM x) JOIN y'

    Inspired by https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html

    Args:
        expression (sqlglot.Expression): expression to optimize
        leave_tables_isolated (bool):
    Returns:
        sqlglot.Expression: optimized expression
    """
    expression = merge_ctes(expression, leave_tables_isolated)
    expression = merge_derived_tables(expression, leave_tables_isolated)
    return expression


# If a derived table has these Select args, it can't be merged
UNMERGABLE_ARGS = set(exp.Select.arg_types) - {
    "expressions",
    "from",
    "joins",
    "where",
    "order",
    "hint",
}


def merge_ctes(expression, leave_tables_isolated=False):
    scopes = traverse_scope(expression)

    # All places where we select from CTEs.
    # We key on the CTE scope so we can detect CTES that are selected from multiple times.
    cte_selections = defaultdict(list)
    for outer_scope in scopes:
        for table, inner_scope in outer_scope.selected_sources.values():
            if isinstance(inner_scope, Scope) and inner_scope.is_cte:
                cte_selections[id(inner_scope)].append(
                    (
                        outer_scope,
                        inner_scope,
                        table,
                    )
                )

    singular_cte_selections = [v[0] for k, v in cte_selections.items() if len(v) == 1]
    for outer_scope, inner_scope, table in singular_cte_selections:
        inner_select = inner_scope.expression.unnest()
        from_or_join = table.find_ancestor(exp.From, exp.Join)
        if _mergeable(outer_scope, inner_select, leave_tables_isolated, from_or_join):
            alias = table.alias_or_name

            _rename_inner_sources(outer_scope, inner_scope, alias)
            _merge_from(outer_scope, inner_scope, table, alias)
            _merge_expressions(outer_scope, inner_scope, alias)
            _merge_joins(outer_scope, inner_scope, from_or_join)
            _merge_where(outer_scope, inner_scope, from_or_join)
            _merge_order(outer_scope, inner_scope)
            _merge_hints(outer_scope, inner_scope)
            _pop_cte(inner_scope)
    return expression


def merge_derived_tables(expression, leave_tables_isolated=False):
    for outer_scope in traverse_scope(expression):
        for subquery in outer_scope.derived_tables:
            inner_select = subquery.unnest()
            from_or_join = subquery.find_ancestor(exp.From, exp.Join)
            if _mergeable(outer_scope, inner_select, leave_tables_isolated, from_or_join):
                alias = subquery.alias_or_name
                inner_scope = outer_scope.sources[alias]

                _rename_inner_sources(outer_scope, inner_scope, alias)
                _merge_from(outer_scope, inner_scope, subquery, alias)
                _merge_expressions(outer_scope, inner_scope, alias)
                _merge_joins(outer_scope, inner_scope, from_or_join)
                _merge_where(outer_scope, inner_scope, from_or_join)
                _merge_order(outer_scope, inner_scope)
                _merge_hints(outer_scope, inner_scope)
    return expression


def _mergeable(outer_scope, inner_select, leave_tables_isolated, from_or_join):
    """
    Return True if `inner_select` can be merged into outer query.

    Args:
        outer_scope (Scope)
        inner_select (exp.Select)
        leave_tables_isolated (bool)
        from_or_join (exp.From|exp.Join)
    Returns:
        bool: True if can be merged
    """

    def _is_a_window_expression_in_unmergable_operation():
        window_expressions = inner_select.find_all(exp.Window)
        window_alias_names = {window.parent.alias_or_name for window in window_expressions}
        inner_select_name = inner_select.parent.alias_or_name
        unmergable_window_columns = [
            column
            for column in outer_scope.columns
            if column.find_ancestor(exp.Where, exp.Group, exp.Order, exp.Join, exp.Having, exp.AggFunc)
        ]
        window_expressions_in_unmergable = [
            column
            for column in unmergable_window_columns
            if column.table == inner_select_name and column.name in window_alias_names
        ]
        return any(window_expressions_in_unmergable)

    return (
        isinstance(outer_scope.expression, exp.Select)
        and isinstance(inner_select, exp.Select)
        and isinstance(inner_select, exp.Select)
        and not any(inner_select.args.get(arg) for arg in UNMERGABLE_ARGS)
        and inner_select.args.get("from")
        and not any(e.find(exp.AggFunc, exp.Select) for e in inner_select.expressions)
        and not (leave_tables_isolated and len(outer_scope.selected_sources) > 1)
        and not (
            isinstance(from_or_join, exp.Join)
            and inner_select.args.get("where")
            and from_or_join.side in {"FULL", "LEFT", "RIGHT"}
        )
        and not (
            isinstance(from_or_join, exp.From)
            and inner_select.args.get("where")
            and any(j.side in {"FULL", "RIGHT"} for j in outer_scope.expression.args.get("joins", []))
        )
        and not _is_a_window_expression_in_unmergable_operation()
    )


def _rename_inner_sources(outer_scope, inner_scope, alias):
    """
    Renames any sources in the inner query that conflict with names in the outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        alias (str)
    """
    taken = set(outer_scope.selected_sources)
    conflicts = taken.intersection(set(inner_scope.selected_sources))
    conflicts = conflicts - {alias}

    for conflict in conflicts:
        new_name = find_new_name(taken, conflict)

        source, _ = inner_scope.selected_sources[conflict]
        new_alias = exp.to_identifier(new_name)

        if isinstance(source, exp.Subquery):
            source.set("alias", exp.TableAlias(this=new_alias))
        elif isinstance(source, exp.Table) and source.alias:
            source.set("alias", new_alias)
        elif isinstance(source, exp.Table):
            source.replace(exp.alias_(source.copy(), new_alias))

        for column in inner_scope.source_columns(conflict):
            column.set("table", exp.to_identifier(new_name))

        inner_scope.rename_source(conflict, new_name)


def _merge_from(outer_scope, inner_scope, node_to_replace, alias):
    """
    Merge FROM clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        node_to_replace (exp.Subquery|exp.Table)
        alias (str)
    """
    new_subquery = inner_scope.expression.args.get("from").expressions[0]
    node_to_replace.replace(new_subquery)
    for join_hint in outer_scope.join_hints:
        tables = join_hint.find_all(exp.Table)
        for table in tables:
            if table.alias_or_name == node_to_replace.alias_or_name:
                table.set("this", exp.to_identifier(new_subquery.alias_or_name))
    outer_scope.remove_source(alias)
    outer_scope.add_source(new_subquery.alias_or_name, inner_scope.sources[new_subquery.alias_or_name])


def _merge_joins(outer_scope, inner_scope, from_or_join):
    """
    Merge JOIN clauses of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        from_or_join (exp.From|exp.Join)
    """

    new_joins = []
    comma_joins = inner_scope.expression.args.get("from").expressions[1:]
    for subquery in comma_joins:
        new_joins.append(exp.Join(this=subquery, kind="CROSS"))
        outer_scope.add_source(subquery.alias_or_name, inner_scope.sources[subquery.alias_or_name])

    joins = inner_scope.expression.args.get("joins") or []
    for join in joins:
        new_joins.append(join)
        outer_scope.add_source(join.alias_or_name, inner_scope.sources[join.alias_or_name])

    if new_joins:
        outer_joins = outer_scope.expression.args.get("joins", [])

        # Maintain the join order
        if isinstance(from_or_join, exp.From):
            position = 0
        else:
            position = outer_joins.index(from_or_join) + 1
        outer_joins[position:position] = new_joins

        outer_scope.expression.set("joins", outer_joins)


def _merge_expressions(outer_scope, inner_scope, alias):
    """
    Merge projections of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        alias (str)
    """
    # Collect all columns that reference the alias of the inner query
    outer_columns = defaultdict(list)
    for column in outer_scope.columns:
        if column.table == alias:
            outer_columns[column.name].append(column)

    # Replace columns with the projection expression in the inner query
    for expression in inner_scope.expression.expressions:
        projection_name = expression.alias_or_name
        if not projection_name:
            continue
        columns_to_replace = outer_columns.get(projection_name, [])
        for column in columns_to_replace:
            column.replace(expression.unalias().copy())


def _merge_where(outer_scope, inner_scope, from_or_join):
    """
    Merge WHERE clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        from_or_join (exp.From|exp.Join)
    """
    where = inner_scope.expression.args.get("where")
    if not where or not where.this:
        return

    if isinstance(from_or_join, exp.Join):
        # Merge predicates from an outer join to the ON clause
        from_or_join.on(where.this, copy=False)
        from_or_join.set("on", simplify(from_or_join.args.get("on")))
    else:
        outer_scope.expression.where(where.this, copy=False)
        outer_scope.expression.set("where", simplify(outer_scope.expression.args.get("where")))


def _merge_order(outer_scope, inner_scope):
    """
    Merge ORDER clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
    """
    if (
        any(outer_scope.expression.args.get(arg) for arg in ["group", "distinct", "having", "order"])
        or len(outer_scope.selected_sources) != 1
        or any(expression.find(exp.AggFunc) for expression in outer_scope.expression.expressions)
    ):
        return

    outer_scope.expression.set("order", inner_scope.expression.args.get("order"))


def _merge_hints(outer_scope, inner_scope):
    inner_scope_hint = inner_scope.expression.args.get("hint")
    if not inner_scope_hint:
        return
    outer_scope_hint = outer_scope.expression.args.get("hint")
    if outer_scope_hint:
        for hint_expression in inner_scope_hint.expressions:
            outer_scope_hint.append("expressions", hint_expression)
    else:
        outer_scope.expression.set("hint", inner_scope_hint)


def _pop_cte(inner_scope):
    """
    Remove CTE from the AST.

    Args:
        inner_scope (sqlglot.optimizer.scope.Scope)
    """
    cte = inner_scope.expression.parent
    with_ = cte.parent
    if len(with_.expressions) == 1:
        with_.pop()
    else:
        cte.pop()
