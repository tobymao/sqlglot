import itertools

from sqlglot.optimizer.scope import traverse_scope
import sqlglot.expressions as exp


def decorrelate_subqueries(expression):
    """
    Rewrite sqlglot AST to remove correlated subqueries.

    Subquery decorrelation can only happen if the predicate contains only equalitys and conjunctions.
    Additionally the subquery cannot have limits or offsets.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> decorrelate_subqueries(expression).sql()
        'SELECT * FROM x AS x JOIN (SELECT y.a FROM y AS y WHERE TRUE GROUP BY y.a) AS "_d_0" ON "_d_0"."a" = x.a AND ("_d_0".a) = 1 WHERE TRUE'

    Args:
        expression (sqlglot.Expression): expression to decorrelated
    Returns:
        sqlglot.Expression: qualified expression
    """
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        select = scope.expression
        where = select.args.get("where")

        if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
            continue

        for column in scope.external_columns:
            eq = column.find_ancestor(exp.EQ)

            if column.find_ancestor(exp.Where) != where or not eq:
                continue

            internal = eq.right if eq.left == column else eq.left
            value = select.selects[0]

            # if the join column is not in the select, we need to add it
            # but we can only do so if the original expression is an agg.
            # if the original subquery was (select foo from x where bar = y.bar)
            # adding bar would make the subquery result in more than 1 row...
            # (select foo, bar from x group by bar).
            # a possible optimization is to do a collect on foo and change operations to lists
            if internal not in [
                s.this if isinstance(s, exp.Alias) else s for s in scope.selects
            ]:
                if not value.find(exp.AggFunc):
                    continue
                select.select(internal, copy=False)

            alias = f"_d_{next(sequence)}"
            on = exp.and_(f"\"{alias}\".\"{internal.text('this')}\" = {column.sql()}")

            eq.replace(exp.TRUE)
            select.replace(
                exp.Column(
                    this=exp.to_identifier(value.alias_or_name),
                    table=exp.to_identifier(alias),
                )
            )

            predicate = select.find_ancestor(*exp.PREDICATES, exp.Exists)

            if predicate:
                predicate.replace(exp.TRUE)

            if isinstance(predicate, exp.Exists):
                select = select.select(internal, append=False)
            elif predicate:
                on = exp.and_(on, predicate)

            select = select.group_by(internal)

            scope.parent.expression.join(
                select,
                on=on,
                join_alias=alias,
                copy=False,
            )

    return expression
