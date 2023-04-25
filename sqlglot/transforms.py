from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.helper import find_new_name

if t.TYPE_CHECKING:
    from sqlglot.generator import Generator


def unalias_group(expression: exp.Expression) -> exp.Expression:
    """
    Replace references to select aliases in GROUP BY clauses.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT a AS b FROM x GROUP BY b").transform(unalias_group).sql()
        'SELECT a AS b FROM x GROUP BY 1'

    Args:
        expression: the expression that will be transformed.

    Returns:
        The transformed expression.
    """
    if isinstance(expression, exp.Group) and isinstance(expression.parent, exp.Select):
        aliased_selects = {
            e.alias: i
            for i, e in enumerate(expression.parent.expressions, start=1)
            if isinstance(e, exp.Alias)
        }

        for group_by in expression.expressions:
            if (
                isinstance(group_by, exp.Column)
                and not group_by.table
                and group_by.name in aliased_selects
            ):
                group_by.replace(exp.Literal.number(aliased_selects.get(group_by.name)))

    return expression


def eliminate_distinct_on(expression: exp.Expression) -> exp.Expression:
    """
    Convert SELECT DISTINCT ON statements to a subquery with a window function.

    This is useful for dialects that don't support SELECT DISTINCT ON but support window functions.

    Args:
        expression: the expression that will be transformed.

    Returns:
        The transformed expression.
    """
    if (
        isinstance(expression, exp.Select)
        and expression.args.get("distinct")
        and expression.args["distinct"].args.get("on")
        and isinstance(expression.args["distinct"].args["on"], exp.Tuple)
    ):
        distinct_cols = expression.args["distinct"].pop().args["on"].expressions
        outer_selects = expression.selects
        row_number = find_new_name(expression.named_selects, "_row_number")
        window = exp.Window(
            this=exp.RowNumber(),
            partition_by=distinct_cols,
        )
        order = expression.args.get("order")
        if order:
            window.set("order", order.pop().copy())
        window = exp.alias_(window, row_number)
        expression.select(window, copy=False)
        return exp.select(*outer_selects).from_(expression.subquery()).where(f'"{row_number}" = 1')
    return expression


def eliminate_qualify(expression: exp.Expression) -> exp.Expression:
    """
    Convert SELECT statements that contain the QUALIFY clause into subqueries, filtered equivalently.

    The idea behind this transformation can be seen in Snowflake's documentation for QUALIFY:
    https://docs.snowflake.com/en/sql-reference/constructs/qualify

    Some dialects don't support window functions in the WHERE clause, so we need to include them as
    projections in the subquery, in order to refer to them in the outer filter using aliases. Also,
    if a column is referenced in the QUALIFY clause but is not selected, we need to include it too,
    otherwise we won't be able to refer to it in the outer query's WHERE clause.
    """
    if isinstance(expression, exp.Select) and expression.args.get("qualify"):
        taken = set(expression.named_selects)
        for select in expression.selects:
            if not select.alias_or_name:
                alias = find_new_name(taken, "_c")
                select.replace(exp.alias_(select.copy(), alias))
                taken.add(alias)

        outer_selects = exp.select(*[select.alias_or_name for select in expression.selects])
        qualify_filters = expression.args["qualify"].pop().this

        for expr in qualify_filters.find_all((exp.Window, exp.Column)):
            if isinstance(expr, exp.Window):
                alias = find_new_name(expression.named_selects, "_w")
                expression.select(exp.alias_(expr.copy(), alias), copy=False)
                column = exp.column(alias)
                if isinstance(expr.parent, exp.Qualify):
                    qualify_filters = column
                else:
                    expr.replace(column)
            elif expr.name not in expression.named_selects:
                expression.select(expr.copy(), copy=False)

        return outer_selects.from_(expression.subquery(alias="_t")).where(qualify_filters)

    return expression


def remove_precision_parameterized_types(expression: exp.Expression) -> exp.Expression:
    """
    Some dialects only allow the precision for parameterized types to be defined in the DDL and not in
    other expressions. This transforms removes the precision from parameterized types in expressions.
    """
    return expression.transform(
        lambda node: exp.DataType(
            **{
                **node.args,
                "expressions": [
                    node_expression
                    for node_expression in node.expressions
                    if isinstance(node_expression, exp.DataType)
                ],
            }
        )
        if isinstance(node, exp.DataType)
        else node,
    )


def unnest_to_explode(expression: exp.Expression) -> exp.Expression:
    """Convert cross join unnest into lateral view explode. Used in presto -> hive"""
    if isinstance(expression, exp.Select):
        for join in expression.args.get("joins") or []:
            unnest = join.this

            if isinstance(unnest, exp.Unnest):
                alias = unnest.args.get("alias")
                udtf = exp.Posexplode if unnest.args.get("ordinality") else exp.Explode

                expression.args["joins"].remove(join)

                for e, column in zip(unnest.expressions, alias.columns if alias else []):
                    expression.append(
                        "laterals",
                        exp.Lateral(
                            this=udtf(this=e),
                            view=True,
                            alias=exp.TableAlias(this=alias.this, columns=[column]),  # type: ignore
                        ),
                    )
    return expression


def remove_target_from_merge(expression: exp.Expression) -> exp.Expression:
    """Remove table refs from columns in when statements."""
    if isinstance(expression, exp.Merge):
        alias = expression.this.args.get("alias")
        targets = {expression.this.this}
        if alias:
            targets.add(alias.this)

        for when in expression.expressions:
            when.transform(
                lambda node: exp.column(node.name)
                if isinstance(node, exp.Column) and node.args.get("table") in targets
                else node,
                copy=False,
            )
    return expression


def preprocess(
    transforms: t.List[t.Callable[[exp.Expression], exp.Expression]],
) -> t.Callable[[Generator, exp.Expression], str]:
    """
    Creates a new transform by chaining a sequence of transformations and converts the resulting
    expression to SQL, using an appropriate `Generator.TRANSFORMS` function.

    Args:
        transforms: sequence of transform functions. These will be called in order.

    Returns:
        Function that can be used as a generator transform.
    """

    def _to_sql(self, expression: exp.Expression) -> str:
        expression = transforms[0](expression.copy())
        for t in transforms[1:]:
            expression = t(expression)
        return getattr(self, expression.key + "_sql")(expression)

    return _to_sql


UNALIAS_GROUP = {exp.Group: preprocess([unalias_group])}
ELIMINATE_DISTINCT_ON = {exp.Select: preprocess([eliminate_distinct_on])}
ELIMINATE_QUALIFY = {exp.Select: preprocess([eliminate_qualify])}
REMOVE_PRECISION_PARAMETERIZED_TYPES = {
    exp.Cast: preprocess([remove_precision_parameterized_types])
}
