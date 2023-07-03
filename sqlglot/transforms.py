from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.helper import find_new_name, name_sequence

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
        window = exp.Window(this=exp.RowNumber(), partition_by=distinct_cols)
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
                select.replace(exp.alias_(select, alias))
                taken.add(alias)

        outer_selects = exp.select(*[select.alias_or_name for select in expression.selects])
        qualify_filters = expression.args["qualify"].pop().this

        for expr in qualify_filters.find_all((exp.Window, exp.Column)):
            if isinstance(expr, exp.Window):
                alias = find_new_name(expression.named_selects, "_w")
                expression.select(exp.alias_(expr, alias), copy=False)
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
    for node in expression.find_all(exp.DataType):
        node.set(
            "expressions", [e for e in node.expressions if not isinstance(e, exp.DataTypeSize)]
        )

    return expression


def unnest_to_explode(expression: exp.Expression) -> exp.Expression:
    """Convert cross join unnest into lateral view explode (used in presto -> hive)."""
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


def explode_to_unnest(expression: exp.Expression) -> exp.Expression:
    """Convert explode/posexplode into unnest (used in hive -> presto)."""
    if isinstance(expression, exp.Select):
        from sqlglot.optimizer.scope import Scope

        taken_select_names = set(expression.named_selects)
        taken_source_names = {name for name, _ in Scope(expression).references}

        for select in expression.selects:
            to_replace = select

            pos_alias = ""
            explode_alias = ""

            if isinstance(select, exp.Alias):
                explode_alias = select.alias
                select = select.this
            elif isinstance(select, exp.Aliases):
                pos_alias = select.aliases[0].name
                explode_alias = select.aliases[1].name
                select = select.this

            if isinstance(select, (exp.Explode, exp.Posexplode)):
                is_posexplode = isinstance(select, exp.Posexplode)

                explode_arg = select.this
                unnest = exp.Unnest(expressions=[explode_arg.copy()], ordinality=is_posexplode)

                # This ensures that we won't use [POS]EXPLODE's argument as a new selection
                if isinstance(explode_arg, exp.Column):
                    taken_select_names.add(explode_arg.output_name)

                unnest_source_alias = find_new_name(taken_source_names, "_u")
                taken_source_names.add(unnest_source_alias)

                if not explode_alias:
                    explode_alias = find_new_name(taken_select_names, "col")
                    taken_select_names.add(explode_alias)

                    if is_posexplode:
                        pos_alias = find_new_name(taken_select_names, "pos")
                        taken_select_names.add(pos_alias)

                if is_posexplode:
                    column_names = [explode_alias, pos_alias]
                    to_replace.pop()
                    expression.select(pos_alias, explode_alias, copy=False)
                else:
                    column_names = [explode_alias]
                    to_replace.replace(exp.column(explode_alias))

                unnest = exp.alias_(unnest, unnest_source_alias, table=column_names)

                if not expression.args.get("from"):
                    expression.from_(unnest, copy=False)
                else:
                    expression.join(unnest, join_type="CROSS", copy=False)

    return expression


def remove_within_group_for_percentiles(expression: exp.Expression) -> exp.Expression:
    if (
        isinstance(expression, exp.WithinGroup)
        and isinstance(expression.this, (exp.PercentileCont, exp.PercentileDisc))
        and isinstance(expression.expression, exp.Order)
    ):
        quantile = expression.this.this
        input_value = t.cast(exp.Ordered, expression.find(exp.Ordered)).this
        return expression.replace(exp.ApproxQuantile(this=input_value, quantile=quantile))

    return expression


def add_recursive_cte_column_names(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.With) and expression.recursive:
        next_name = name_sequence("_c_")

        for cte in expression.expressions:
            if not cte.args["alias"].columns:
                query = cte.this
                if isinstance(query, exp.Union):
                    query = query.this

                cte.args["alias"].set(
                    "columns",
                    [exp.to_identifier(s.alias_or_name or next_name()) for s in query.selects],
                )

    return expression


def epoch_cast_to_ts(expression: exp.Expression) -> exp.Expression:
    if (
        isinstance(expression, (exp.Cast, exp.TryCast))
        and expression.name.lower() == "epoch"
        and expression.to.this in exp.DataType.TEMPORAL_TYPES
    ):
        expression.this.replace(exp.Literal.string("1970-01-01 00:00:00"))

    return expression


def preprocess(
    transforms: t.List[t.Callable[[exp.Expression], exp.Expression]],
) -> t.Callable[[Generator, exp.Expression], str]:
    """
    Creates a new transform by chaining a sequence of transformations and converts the resulting
    expression to SQL, using either the "_sql" method corresponding to the resulting expression,
    or the appropriate `Generator.TRANSFORMS` function (when applicable -- see below).

    Args:
        transforms: sequence of transform functions. These will be called in order.

    Returns:
        Function that can be used as a generator transform.
    """

    def _to_sql(self, expression: exp.Expression) -> str:
        expression_type = type(expression)

        expression = transforms[0](expression.copy())
        for t in transforms[1:]:
            expression = t(expression)

        _sql_handler = getattr(self, expression.key + "_sql", None)
        if _sql_handler:
            return _sql_handler(expression)

        transforms_handler = self.TRANSFORMS.get(type(expression))
        if transforms_handler:
            # Ensures we don't enter an infinite loop. This can happen when the original expression
            # has the same type as the final expression and there's no _sql method available for it,
            # because then it'd re-enter _to_sql.
            if expression_type is type(expression):
                raise ValueError(
                    f"Expression type {expression.__class__.__name__} requires a _sql method in order to be transformed."
                )

            return transforms_handler(self, expression)

        raise ValueError(f"Unsupported expression type {expression.__class__.__name__}.")

    return _to_sql
