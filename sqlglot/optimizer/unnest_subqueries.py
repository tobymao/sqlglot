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

    predicates = []
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

        predicates.append(predicate)
        keys.append(key)

    if not any(isinstance(predicate, exp.EQ) for predicate in predicates):
        return

    projections = {s.this: s for s in select.selects}
    key_aliases = {}
    predicate = select.find_ancestor(exp.Predicate)

    for key in keys:
        if key in key_aliases:
            continue
        # if the join column is not in the select, we need to add it
        # but we can only do so if the original expression is an agg.
        # if the original subquery was (select foo from x where bar = y.bar)
        # adding bar would make the subquery result in more than 1 row...
        # (select foo, bar from x group by bar).
        # a possible optimization is to do a collect on foo and change operations to lists
        projection = projections.get(key)
        if projection:
            key_aliases[key] = projection.alias
        else:
            if not value.find(exp.AggFunc):
                return
            alias = _alias(sequence)
            select.select(exp.alias_(key.copy(), alias), copy=False)
            key_aliases[key] = alias

    for p in predicates:
        p.replace(exp.TRUE)

    for key in keys:
        key.replace(exp.column(key_aliases[key], table_alias))

    value = exp.column(value.alias, table_alias)

    if isinstance(predicate, exp.Exists):
        select = select.select(*keys, append=False)
        predicate.replace(exp.condition(f"NOT {value.sql()} IS NULL"))
    else:
        if isinstance(predicate, exp.In):
            predicate.replace(exp.EQ(this=predicate.this, expression=value))
        else:
            # select is in a subquery so we replace the parent
            select.parent.replace(value)

    parent_select.join(
        select.group_by(*key_aliases, copy=False),
        on=predicates,
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )


def _alias(sequence):
    return f"_u_{next(sequence)}"
