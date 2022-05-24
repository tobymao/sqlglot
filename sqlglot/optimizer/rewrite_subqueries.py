import itertools

from sqlglot.optimizer.scope import traverse_scope
import sqlglot.expressions as exp


def rewrite_subqueries(expression):
    expression = expression.copy()
    expression = decorrelate_subqueries(expression)
    return expression


def decorrelate_subqueries(expression):
    """
    Rewrite sqlglot AST to remove correlated subqueries.

    Subquery decorrelation can only happen if the predicate contains only equalitys and conjunctions.
    Additionally the subquery cannot have limits or offsets.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> decorrelate_subqueries(expression).sql()
        'SELECT * FROM x AS x JOIN (SELECT y.a FROM y AS y WHERE TRUE GROUP BY y.a) AS "_d_0" ON _d_0.a = x.a AND ("_d_0".a) = 1 WHERE TRUE'

    Args:
        expression (sqlglot.Expression): expression to decorrelated
    Returns:
        sqlglot.Expression: qualified expression
    """
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        if scope.correlated_selectables:
            select = scope.expression
            parent_select = select.find_ancestor(exp.Select)
            where = select.args["where"]

            if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
                continue

            for external in where.find_all(exp.Column):
                table = external.text("table")
                eq = external.parent

                if table not in scope.correlated_selectables or not isinstance(
                    eq, exp.EQ
                ):
                    continue

                internal = eq.right if eq.left == external else eq.left
                value = select.selects[0]

                # if the join column is not in the select, we need to add it
                # but we can only do so if the original expression is an agg.
                # if the original subquery was (select foo from x where bar = y.bar)
                # adding bar would make the subquery result in more than 1 row...
                # (select foo, bar from x group by bar).
                # a possible optimization is to do a collect on foo and change operations to lists
                if internal not in scope.selects:
                    if not value.find(exp.AggFunc):
                        continue
                    select.select(internal, copy=False)

                alias = f"_d_{next(sequence)}"
                eq.replace(exp.TRUE)
                select.replace(
                    exp.Column(
                        this=exp.to_identifier(value.alias_or_name),
                        table=exp.to_identifier(alias),
                    )
                )

                condition = select.find_ancestor(
                    exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE
                )

                parent_select.join(
                    select.group_by(internal),
                    on=f"{alias}.{internal.text('this')} = {external.sql()} AND {condition.sql()}",
                    join_alias=alias,
                    copy=False,
                )

                condition.replace(exp.TRUE)
    return expression
