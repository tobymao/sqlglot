import itertools

from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope


def unnest_subqueries(expression):
    """
    Rewrite sqlglot AST to convert some predicates with subqueries into joins.

    Convert the subquery into a group by so it is not a many to many left join.
    Unnesting can only occur if the subquery does not have LIMIT or OFFSET.
    Unnesting non correlated subqueries only happens on IN statements or = ANY statements.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> unnest_subqueries(expression).sql()
        'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS "_u_0" ON x.a = "_u_0".a WHERE "_u_0".a = 1'

    Args:
        expression (sqlglot.Expression): expression to decorrelated
    Returns:
        sqlglot.Expression: qualified expression
    """
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        select = scope.expression
        parent = select.parent_select
        if scope.external_columns:
            decorrelate(select, parent, scope.external_columns, sequence)
        else:
            unnest(select, parent, sequence)

    return expression


def unnest(select, parent_select, sequence):
    predicate = select.find_ancestor(exp.In, exp.Any)

    if not predicate or parent_select is not predicate.parent_select:
        return

    if len(select.selects) > 1 or select.find(exp.Limit, exp.Offset):
        return

    if isinstance(predicate, exp.Any):
        side = predicate.arg_key == "expression"
        predicate = predicate.find_ancestor(exp.EQ)

        if not predicate or parent_select is not predicate.parent_select:
            return

        column = predicate.left if side else predicate.right
    else:
        column = predicate.this

    value = select.selects[0]
    alias = _alias(sequence)

    on = exp.condition(f'{column.sql()} = "{alias}"."{value.alias}"')
    predicate.replace(exp.condition(f"NOT {on.right.sql()} IS NULL"))
    select.group_by(value.this, copy=False)

    parent_select.join(
        select,
        on=on,
        join_type="LEFT",
        join_alias=alias,
        copy=False,
    )


def decorrelate(select, parent_select, external_columns, sequence):
    where = select.args.get("where")

    if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
        return

    value = select.selects[0]
    table_alias = _alias(sequence)
    keys = []

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

        keys.append((key, predicate))

    if not any(isinstance(predicate, exp.EQ) for _, predicate in keys):
        return

    projections = {s.this: s for s in select.selects}
    key_aliases = {}
    group_by = []

    for key, predicate in keys:
        projection = projections.get(key)

        if projection:
            key_aliases[key] = projection.alias
            group_by.append(key)
        else:
            if key not in key_aliases:
                alias = _alias(sequence)
                key_aliases[key] = alias
            if isinstance(predicate, exp.EQ):
                group_by.append(key)


    parent_predicate = select.find_ancestor(exp.Predicate)

    for key, predicate in keys:
        predicate.replace(exp.TRUE)
        column = exp.column(key_aliases[key], table_alias)
        if key in group_by:
            key.replace(column)
        elif isinstance(predicate, exp.NEQ):
            print(predicate)
            print(parent_predicate)
            #key.replace(exp.condition(f"{column}"))

    #for key in keys:
    #    # if the join column is not in the select, we need to add it
    #    # but we can only do so if the original expression is an agg.
    #    # if the original subquery was (select foo from x where bar = y.bar)
    #    # adding bar would make the subquery result in more than 1 row...
    #    # (select foo, bar from x group by bar).
    #    # a possible optimization is to do a collect on foo and change operations to lists
    #    projection = projections.get(key)
    #    if projection:
    #        key_aliases[key] = projection.alias
    #        group_by.append(key)
    #    else:
    #        alias = _alias(sequence)
    #        key_aliases[key] = alias
    #        if not any(isinstance(p, exp.EQ) for p in keys[key]):
    #            select.select(f"ARRAY_AGG({key}) AS {alias}", copy=False)
    #        else:
    #            select.select(exp.alias_(key.copy(), alias), copy=False)
    #            group_by.append(key)
    #        #print(select)
    #        #raise
    #        #if not isinstance(predicate, exp.Exists):
    #        #    if not value.find(exp.AggFunc):
    #        #        return
    #        #    select.select(exp.alias_(key.copy(), alias), copy=False)

    value = exp.column(value.alias, table_alias)

    if isinstance(predicate, exp.Exists):
        #select = select.select(*keys, append=False)
        predicate.replace(exp.condition(f"NOT {value.sql()} IS NULL"))
    else:
        if isinstance(predicate, exp.In):
            predicate.replace(exp.EQ(this=predicate.this, expression=value))
        else:
            # select is in a subquery so we replace the parent
            select.parent.replace(value)

    parent_select.join(
        select.group_by(*group_by, copy=False),
        on=[predicate for _, predicate in keys],
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )


def _alias(sequence):
    return f"_u_{next(sequence)}"
