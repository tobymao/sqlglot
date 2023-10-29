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
            window.set("order", order.pop())
        else:
            window.set("order", exp.Order(expressions=[c.copy() for c in distinct_cols]))

        window = exp.alias_(window, row_number)
        expression.select(window, copy=False)

        return (
            exp.select(*outer_selects, copy=False)
            .from_(expression.subquery("_t", copy=False), copy=False)
            .where(exp.column(row_number).eq(1), copy=False)
        )

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

        select_candidates = exp.Window if expression.is_star else (exp.Window, exp.Column)
        for expr in qualify_filters.find_all(select_candidates):
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

        return outer_selects.from_(expression.subquery(alias="_t", copy=False), copy=False).where(
            qualify_filters, copy=False
        )

    return expression


def remove_precision_parameterized_types(expression: exp.Expression) -> exp.Expression:
    """
    Some dialects only allow the precision for parameterized types to be defined in the DDL and not in
    other expressions. This transforms removes the precision from parameterized types in expressions.
    """
    for node in expression.find_all(exp.DataType):
        node.set(
            "expressions", [e for e in node.expressions if not isinstance(e, exp.DataTypeParam)]
        )

    return expression


def unnest_to_explode(expression: exp.Expression) -> exp.Expression:
    """Convert cross join unnest into lateral view explode (used in presto -> hive)."""
    if isinstance(expression, exp.Select):
        for join in expression.args.get("joins") or []:
            unnest = join.this

            if isinstance(unnest, exp.Unnest):
                alias = unnest.args.get("alias")
                udtf = exp.Posexplode if unnest.args.get("offset") else exp.Explode

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


def explode_to_unnest(index_offset: int = 0) -> t.Callable[[exp.Expression], exp.Expression]:
    """Convert explode/posexplode into unnest (used in hive -> presto)."""

    def _explode_to_unnest(expression: exp.Expression) -> exp.Expression:
        if isinstance(expression, exp.Select):
            from sqlglot.optimizer.scope import Scope

            taken_select_names = set(expression.named_selects)
            taken_source_names = {name for name, _ in Scope(expression).references}

            def new_name(names: t.Set[str], name: str) -> str:
                name = find_new_name(names, name)
                names.add(name)
                return name

            arrays: t.List[exp.Condition] = []
            series_alias = new_name(taken_select_names, "pos")
            series = exp.alias_(
                exp.Unnest(
                    expressions=[exp.GenerateSeries(start=exp.Literal.number(index_offset))]
                ),
                new_name(taken_source_names, "_u"),
                table=[series_alias],
            )

            # we use list here because expression.selects is mutated inside the loop
            for select in list(expression.selects):
                explode = select.find(exp.Explode)

                if explode:
                    pos_alias = ""
                    explode_alias = ""

                    if isinstance(select, exp.Alias):
                        explode_alias = select.alias
                        alias = select
                    elif isinstance(select, exp.Aliases):
                        pos_alias = select.aliases[0].name
                        explode_alias = select.aliases[1].name
                        alias = select.replace(exp.alias_(select.this, "", copy=False))
                    else:
                        alias = select.replace(exp.alias_(select, ""))
                        explode = alias.find(exp.Explode)
                        assert explode

                    is_posexplode = isinstance(explode, exp.Posexplode)
                    explode_arg = explode.this

                    # This ensures that we won't use [POS]EXPLODE's argument as a new selection
                    if isinstance(explode_arg, exp.Column):
                        taken_select_names.add(explode_arg.output_name)

                    unnest_source_alias = new_name(taken_source_names, "_u")

                    if not explode_alias:
                        explode_alias = new_name(taken_select_names, "col")

                        if is_posexplode:
                            pos_alias = new_name(taken_select_names, "pos")

                    if not pos_alias:
                        pos_alias = new_name(taken_select_names, "pos")

                    alias.set("alias", exp.to_identifier(explode_alias))

                    column = exp.If(
                        this=exp.column(series_alias).eq(exp.column(pos_alias)),
                        true=exp.column(explode_alias),
                    )

                    explode.replace(column)

                    if is_posexplode:
                        expressions = expression.expressions
                        expressions.insert(
                            expressions.index(alias) + 1,
                            exp.If(
                                this=exp.column(series_alias).eq(exp.column(pos_alias)),
                                true=exp.column(pos_alias),
                            ).as_(pos_alias),
                        )
                        expression.set("expressions", expressions)

                    if not arrays:
                        if expression.args.get("from"):
                            expression.join(series, copy=False)
                        else:
                            expression.from_(series, copy=False)

                    size: exp.Condition = exp.ArraySize(this=explode_arg.copy())
                    arrays.append(size)

                    # trino doesn't support left join unnest with on conditions
                    # if it did, this would be much simpler
                    expression.join(
                        exp.alias_(
                            exp.Unnest(
                                expressions=[explode_arg.copy()],
                                offset=exp.to_identifier(pos_alias),
                            ),
                            unnest_source_alias,
                            table=[explode_alias],
                        ),
                        join_type="CROSS",
                        copy=False,
                    )

                    if index_offset != 1:
                        size = size - 1

                    expression.where(
                        exp.column(series_alias)
                        .eq(exp.column(pos_alias))
                        .or_(
                            (exp.column(series_alias) > size).and_(exp.column(pos_alias).eq(size))
                        ),
                        copy=False,
                    )

            if arrays:
                end: exp.Condition = exp.Greatest(this=arrays[0], expressions=arrays[1:])

                if index_offset != 1:
                    end = end - (1 - index_offset)
                series.expressions[0].set("end", end)

        return expression

    return _explode_to_unnest


PERCENTILES = (exp.PercentileCont, exp.PercentileDisc)


def add_within_group_for_percentiles(expression: exp.Expression) -> exp.Expression:
    """Transforms percentiles by adding a WITHIN GROUP clause to them."""
    if (
        isinstance(expression, PERCENTILES)
        and not isinstance(expression.parent, exp.WithinGroup)
        and expression.expression
    ):
        column = expression.this.pop()
        expression.set("this", expression.expression.pop())
        order = exp.Order(expressions=[exp.Ordered(this=column)])
        expression = exp.WithinGroup(this=expression, expression=order)

    return expression


def remove_within_group_for_percentiles(expression: exp.Expression) -> exp.Expression:
    """Transforms percentiles by getting rid of their corresponding WITHIN GROUP clause."""
    if (
        isinstance(expression, exp.WithinGroup)
        and isinstance(expression.this, PERCENTILES)
        and isinstance(expression.expression, exp.Order)
    ):
        quantile = expression.this.this
        input_value = t.cast(exp.Ordered, expression.find(exp.Ordered)).this
        return expression.replace(exp.ApproxQuantile(this=input_value, quantile=quantile))

    return expression


def add_recursive_cte_column_names(expression: exp.Expression) -> exp.Expression:
    """Uses projection output names in recursive CTE definitions to define the CTEs' columns."""
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
    """Replace 'epoch' in casts by the equivalent date literal."""
    if (
        isinstance(expression, (exp.Cast, exp.TryCast))
        and expression.name.lower() == "epoch"
        and expression.to.this in exp.DataType.TEMPORAL_TYPES
    ):
        expression.this.replace(exp.Literal.string("1970-01-01 00:00:00"))

    return expression


def eliminate_semi_and_anti_joins(expression: exp.Expression) -> exp.Expression:
    """Convert SEMI and ANTI joins into equivalent forms that use EXIST instead."""
    if isinstance(expression, exp.Select):
        for join in expression.args.get("joins") or []:
            on = join.args.get("on")
            if on and join.kind in ("SEMI", "ANTI"):
                subquery = exp.select("1").from_(join.this).where(on)
                exists = exp.Exists(this=subquery)
                if join.kind == "ANTI":
                    exists = exists.not_(copy=False)

                join.pop()
                expression.where(exists, copy=False)

    return expression


def eliminate_full_outer_join(expression: exp.Expression) -> exp.Expression:
    """
    Converts a query with a FULL OUTER join to a union of identical queries that
    use LEFT/RIGHT OUTER joins instead. This transformation currently only works
    for queries that have a single FULL OUTER join.
    """
    if isinstance(expression, exp.Select):
        full_outer_joins = [
            (index, join)
            for index, join in enumerate(expression.args.get("joins") or [])
            if join.side == "FULL" and join.kind == "OUTER"
        ]

        if len(full_outer_joins) == 1:
            expression_copy = expression.copy()
            index, full_outer_join = full_outer_joins[0]
            full_outer_join.set("side", "left")
            expression_copy.args["joins"][index].set("side", "right")

            return exp.union(expression, expression_copy, copy=False)

    return expression


def move_ctes_to_top_level(expression: exp.Expression) -> exp.Expression:
    """
    Some dialects (e.g. Hive, T-SQL, Spark prior to version 3) only allow CTEs to be
    defined at the top-level, so for example queries like:

        SELECT * FROM (WITH t(c) AS (SELECT 1) SELECT * FROM t) AS subq

    are invalid in those dialects. This transformation can be used to ensure all CTEs are
    moved to the top level so that the final SQL code is valid from a syntax standpoint.

    TODO: handle name clashes whilst moving CTEs (it can get quite tricky & costly).
    """
    top_level_with = expression.args.get("with")
    for node in expression.find_all(exp.With):
        if node.parent is expression:
            continue

        inner_with = node.pop()
        if not top_level_with:
            top_level_with = inner_with
            expression.set("with", top_level_with)
        else:
            if inner_with.recursive:
                top_level_with.set("recursive", True)

            top_level_with.expressions.extend(inner_with.expressions)

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

        expression = transforms[0](expression)
        for t in transforms[1:]:
            expression = t(expression)

        _sql_handler = getattr(self, expression.key + "_sql", None)
        if _sql_handler:
            return _sql_handler(expression)

        transforms_handler = self.TRANSFORMS.get(type(expression))
        if transforms_handler:
            if expression_type is type(expression):
                if isinstance(expression, exp.Func):
                    return self.function_fallback_sql(expression)

                # Ensures we don't enter an infinite loop. This can happen when the original expression
                # has the same type as the final expression and there's no _sql method available for it,
                # because then it'd re-enter _to_sql.
                raise ValueError(
                    f"Expression type {expression.__class__.__name__} requires a _sql method in order to be transformed."
                )

            return transforms_handler(self, expression)

        raise ValueError(f"Unsupported expression type {expression.__class__.__name__}.")

    return _to_sql
