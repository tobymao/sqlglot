import itertools

from sqlglot import exp
from sqlglot.optimizer.scope import ScopeType, traverse_scope


def unnest_subqueries(expression):
    """
    Rewrite sqlglot AST to convert some predicates with subqueries into joins.

    Convert scalar subqueries into cross joins.
    Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> unnest_subqueries(expression).sql()
        'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a)\
 AS "_u_0" ON x.a = "_u_0".a WHERE ("_u_0".a = 1 AND NOT "_u_0".a IS NULL)'

    Args:
        expression (sqlglot.Expression): expression to unnest
    Returns:
        sqlglot.Expression: unnested expression
    """
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        select = scope.expression
        parent = select.parent_select
        if not parent:
            continue
        if scope.external_columns:
            decorrelate(select, parent, scope.external_columns, sequence)
        elif scope.scope_type == ScopeType.SUBQUERY:
            unnest(select, parent, sequence)

    return expression


def unnest(select, parent_select, sequence):
    if len(select.selects) > 1:
        return

    predicate = select.find_ancestor(exp.Condition)
    alias = _alias(sequence)

    if not predicate or parent_select is not predicate.parent_select:
        return

    # this subquery returns a scalar and can just be converted to a cross join
    if not isinstance(predicate, (exp.In, exp.Any)):
        having = predicate.find_ancestor(exp.Having)
        column = exp.column(select.selects[0].alias_or_name, alias)
        if having and having.parent_select is parent_select:
            column = exp.Max(this=column)
        _replace(select.parent, column)

        parent_select.join(
            select,
            join_type="CROSS",
            join_alias=alias,
            copy=False,
        )
        return

    if select.find(exp.Limit, exp.Offset):
        return

    if isinstance(predicate, exp.Any):
        predicate = predicate.find_ancestor(exp.EQ)

        if not predicate or parent_select is not predicate.parent_select:
            return

    column = _other_operand(predicate)
    value = select.selects[0]

    on = exp.condition(f'{column} = "{alias}"."{value.alias}"')
    _replace(predicate, f"NOT {on.right} IS NULL")

    parent_select.join(
        select.group_by(value.this, copy=False),
        on=on,
        join_type="LEFT",
        join_alias=alias,
        copy=False,
    )


def decorrelate(select, parent_select, external_columns, sequence):
    where = select.args.get("where")

    if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
        return

    table_alias = _alias(sequence)
    keys = []

    # for all external columns in the where statement, find the relevant predicate
    # keys to convert it into a join
    for column in external_columns:
        if column.find_ancestor(exp.Where) is not where:
            return

        predicate = column.find_ancestor(exp.Predicate)

        if not predicate or predicate.find_ancestor(exp.Where) is not where:
            return

        if isinstance(predicate, exp.Binary):
            key = (
                predicate.right
                if any(node is column for node, *_ in predicate.left.walk())
                else predicate.left
            )
        else:
            return

        keys.append((key, column, predicate))

    if not any(isinstance(predicate, exp.EQ) for *_, predicate in keys):
        return

    is_subquery_projection = any(
        node is select.parent for node in parent_select.selects if isinstance(node, exp.Subquery)
    )

    value = select.selects[0]
    key_aliases = {}
    group_by = []

    for key, _, predicate in keys:
        # if we filter on the value of the subquery, it needs to be unique
        if key == value.this:
            key_aliases[key] = value.alias
            group_by.append(key)
        else:
            if key not in key_aliases:
                key_aliases[key] = _alias(sequence)
            # all predicates that are equalities must also be in the unique
            # so that we don't do a many to many join
            if isinstance(predicate, exp.EQ) and key not in group_by:
                group_by.append(key)

    parent_predicate = select.find_ancestor(exp.Predicate)

    # if the value of the subquery is not an agg or a key, we need to collect it into an array
    # so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
    agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg
    if not value.find(exp.AggFunc) and value.this not in group_by:
        select.select(
            exp.alias_(agg_func(this=value.this), value.alias, quoted=False),
            append=False,
            copy=False,
        )

    # exists queries should not have any selects as it only checks if there are any rows
    # all selects will be added by the optimizer and only used for join keys
    if isinstance(parent_predicate, exp.Exists):
        select.args["expressions"] = []

    for key, alias in key_aliases.items():
        if key in group_by:
            # add all keys to the projections of the subquery
            # so that we can use it as a join key
            if isinstance(parent_predicate, exp.Exists) or key != value.this:
                select.select(f"{key} AS {alias}", copy=False)
        else:
            select.select(exp.alias_(agg_func(this=key.copy()), alias, quoted=False), copy=False)

    alias = exp.column(value.alias, table_alias)
    other = _other_operand(parent_predicate)

    if isinstance(parent_predicate, exp.Exists):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate, f"NOT {alias} IS NULL")
        else:
            parent_predicate = _replace(parent_predicate, "TRUE")
    elif isinstance(parent_predicate, exp.All):
        parent_predicate = _replace(
            parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> _x = {other})"
        )
    elif isinstance(parent_predicate, exp.Any):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate.parent, f"{other} = {alias}")
        else:
            parent_predicate = _replace(parent_predicate, f"ARRAY_ANY({alias}, _x -> _x = {other})")
    elif isinstance(parent_predicate, exp.In):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate, f"{other} = {alias}")
        else:
            parent_predicate = _replace(
                parent_predicate,
                f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
            )
    else:
        if is_subquery_projection:
            alias = exp.alias_(alias, select.parent.alias)
        select.parent.replace(alias)

    for key, column, predicate in keys:
        predicate.replace(exp.true())
        nested = exp.column(key_aliases[key], table_alias)

        if is_subquery_projection:
            key.replace(nested)
            continue

        if key in group_by:
            key.replace(nested)
            parent_predicate = _replace(
                parent_predicate, f"({parent_predicate} AND NOT {nested} IS NULL)"
            )
        elif isinstance(predicate, exp.EQ):
            parent_predicate = _replace(
                parent_predicate,
                f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
            )
        else:
            key.replace(exp.to_identifier("_x"))
            parent_predicate = _replace(
                parent_predicate,
                f'({parent_predicate} AND ARRAY_ANY({nested}, "_x" -> {predicate}))',
            )

    parent_select.join(
        select.group_by(*group_by, copy=False),
        on=[predicate for *_, predicate in keys if isinstance(predicate, exp.EQ)],
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )


def _alias(sequence):
    return f"_u_{next(sequence)}"


def _replace(expression, condition):
    return expression.replace(exp.condition(condition))


def _other_operand(expression):
    if isinstance(expression, exp.In):
        return expression.this

    if isinstance(expression, exp.Binary):
        return expression.right if expression.arg_key == "this" else expression.left

    return None
