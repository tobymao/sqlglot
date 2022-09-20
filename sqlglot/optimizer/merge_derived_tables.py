from collections import defaultdict

from sqlglot import expressions as exp
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify


def merge_derived_tables(expression):
    """
    Rewrite sqlglot AST to merge derived tables into the outer query.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT x.a FROM x)")
        >>> merge_derived_tables(expression).sql()
        'SELECT x.a FROM x'

    Inspired by https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    for outer_scope in traverse_scope(expression):
        for subquery in outer_scope.derived_tables:
            inner_select = subquery.unnest()
            if (
                isinstance(outer_scope.expression, exp.Select)
                and isinstance(inner_select, exp.Select)
                and _mergeable(inner_select)
            ):
                alias = subquery.alias_or_name
                from_or_join = subquery.find_ancestor(exp.From, exp.Join)
                inner_scope = outer_scope.sources[alias]

                _rename_inner_sources(outer_scope, inner_scope, alias)
                _merge_from(outer_scope, inner_scope, subquery)
                _merge_joins(outer_scope, inner_scope, from_or_join)
                _merge_expressions(outer_scope, inner_scope, alias)
                _merge_where(outer_scope, inner_scope, from_or_join)
                _merge_order(outer_scope, inner_scope)
    return expression


# If a derived table has these Select args, it can't be merged
UNMERGABLE_ARGS = set(exp.Select.arg_types) - {
    "expressions",
    "from",
    "joins",
    "where",
    "order",
}


def _mergeable(inner_select):
    """
    Return True if `inner_select` can be merged into outer query.

    Args:
        inner_select (exp.Select)
    Returns:
        bool: True if can be merged
    """
    return (
        isinstance(inner_select, exp.Select)
        and not any(inner_select.args.get(arg) for arg in UNMERGABLE_ARGS)
        and inner_select.args.get("from")
        and not any(e.find(exp.AggFunc, exp.Select) for e in inner_select.expressions)
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
        new_name = _find_new_name(taken, conflict)

        source, _ = inner_scope.selected_sources[conflict]
        new_alias = exp.to_identifier(new_name)

        if isinstance(source, exp.Subquery):
            source.set("alias", exp.TableAlias(this=new_alias))
        elif isinstance(source, exp.Table) and isinstance(source.parent, exp.Alias):
            source.parent.set("alias", new_alias)
        elif isinstance(source, exp.Table):
            source.replace(exp.alias_(source.copy(), new_alias))

        for column in inner_scope.source_columns(conflict):
            column.set("table", exp.to_identifier(new_name))

        inner_scope.rename_source(conflict, new_name)


def _find_new_name(taken, base):
    """
    Searches for a new source name.

    Args:
        taken (set[str]): set of taken names
        base (str): base name to alter
    """
    i = 2
    new = f"{base}_{i}"
    while new in taken:
        i += 1
        new = f"{base}_{i}"
    return new


def _merge_from(outer_scope, inner_scope, subquery):
    """
    Merge FROM clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
        subquery (exp.Subquery)
    """
    new_subquery = inner_scope.expression.args.get("from").expressions[0]
    subquery.replace(new_subquery)
    outer_scope.remove_source(subquery.alias_or_name)
    outer_scope.add_source(
        new_subquery.alias_or_name, inner_scope.sources[new_subquery.alias_or_name]
    )


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
        outer_scope.add_source(
            subquery.alias_or_name, inner_scope.sources[subquery.alias_or_name]
        )

    joins = inner_scope.expression.args.get("joins") or []
    for join in joins:
        new_joins.append(join)
        outer_scope.add_source(
            join.alias_or_name, inner_scope.sources[join.alias_or_name]
        )

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
    # Collect all columns that for the alias of the inner query
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
            column.replace(expression.unalias())


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

    if isinstance(from_or_join, exp.Join) and from_or_join.side:
        # Merge predicates from an outer join to the ON clause
        from_or_join.on(where.this, copy=False)
        from_or_join.set("on", simplify(from_or_join.args.get("on")))
    else:
        outer_scope.expression.where(where.this, copy=False)
        outer_scope.expression.set(
            "where", simplify(outer_scope.expression.args.get("where"))
        )


def _merge_order(outer_scope, inner_scope):
    """
    Merge ORDER clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
    """
    if (
        any(
            outer_scope.expression.args.get(arg)
            for arg in ["group", "distinct", "having", "order"]
        )
        or len(outer_scope.selected_sources) != 1
        or any(
            expression.find(exp.AggFunc)
            for expression in outer_scope.expression.expressions
        )
    ):
        return

    outer_scope.expression.set("order", inner_scope.expression.args.get("order"))
