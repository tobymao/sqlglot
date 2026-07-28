from __future__ import annotations
from sqlglot import exp
from sqlglot.helper import name_sequence
from sqlglot.optimizer.scope import ScopeType, find_in_scope, traverse_scope
from sqlglot._typing import E


def unnest_subqueries(expression: E) -> E:
    """
    Rewrite sqlglot AST to convert some predicates with subqueries into joins.

    Convert scalar subqueries into cross joins.
    Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> unnest_subqueries(expression).sql()
        'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1'

    Args:
        expression (sqlglot.Expr): expression to unnest
    Returns:
        sqlglot.Expr: unnested expression
    """
    next_alias_name = name_sequence("_u_")

    for scope in traverse_scope(expression):
        select = scope.expression
        parent = select.parent_select
        if not parent:
            continue
        if scope.external_columns:
            decorrelate(select, parent, scope.external_columns, next_alias_name)
        elif scope.scope_type == ScopeType.SUBQUERY:
            unnest(select, parent, next_alias_name)

    return expression


def unnest(select, parent_select, next_alias_name):
    if len(select.selects) > 1:
        return

    predicate = select.find_ancestor(exp.Condition)
    if (
        not predicate
        # Do not unnest subqueries inside table-valued functions such as
        # FROM GENERATE_SERIES(...), FROM UNNEST(...) etc in order to preserve join order
        or (
            isinstance(predicate, exp.Func)
            and isinstance(predicate.parent, (exp.Table, exp.From, exp.Join))
        )
        or parent_select is not predicate.parent_select
        or not parent_select.args.get("from_")
        # NOT IN has three-valued semantics that the LEFT-JOIN-anti rewrite doesn't preserve:
        # a NULL in the subquery makes NOT IN evaluate to NULL for every outer row.
        or (isinstance(predicate, exp.In) and isinstance(predicate.parent, exp.Not))
    ):
        return

    if isinstance(select, exp.SetOperation):
        inner_alias = next_alias_name()
        select = exp.select(
            *(
                exp.alias_(exp.column(s.alias_or_name, inner_alias), s.alias_or_name)
                for s in select.selects
            )
        ).from_(select.subquery(inner_alias))

    alias = next_alias_name()
    clause = predicate.find_ancestor(exp.Having, exp.Where, exp.Join)

    # This subquery returns a scalar and can just be converted to a cross join
    if not isinstance(predicate, (exp.In, exp.Any)):
        column = exp.column(select.selects[0].alias_or_name, alias)

        clause_parent_select = clause.parent_select if clause else None

        if (isinstance(clause, exp.Having) and clause_parent_select is parent_select) or (
            (not clause or clause_parent_select is not parent_select)
            and (
                parent_select.args.get("group")
                or any(find_in_scope(select, exp.AggFunc) for select in parent_select.selects)
            )
        ):
            column = exp.Max(this=column)
        elif not isinstance(select.parent, exp.Subquery):
            return

        join_type = "CROSS"
        on_clause = None
        if isinstance(predicate, exp.Exists):
            # If a subquery returns no rows, cross-joining against it incorrectly eliminates all rows
            # from the parent query. Therefore, we use a LEFT JOIN that always matches (ON TRUE), then
            # check for non-NULL column values to determine whether the subquery contained rows.
            column = column.is_(exp.null()).not_()
            join_type = "LEFT"
            on_clause = exp.true()

        _replace(select.parent, column)
        parent_select.join(select, on=on_clause, join_type=join_type, join_alias=alias, copy=False)

        return

    if select.find(exp.Limit, exp.Offset):
        return

    if isinstance(predicate, exp.Any):
        predicate = predicate.find_ancestor(exp.EQ)

        if not predicate or parent_select is not predicate.parent_select:
            return

    column = _other_operand(predicate)
    value = select.selects[0]

    join_key = exp.column(value.alias, alias)
    join_key_not_null = join_key.is_(exp.null()).not_()

    if isinstance(clause, exp.Join):
        _replace(predicate, exp.true())
        parent_select.where(join_key_not_null, copy=False)
    else:
        _replace(predicate, join_key_not_null)

    group = select.args.get("group")

    if group:
        if {value.this} != set(group.expressions):
            select = (
                exp.select(exp.alias_(exp.column(value.alias, "_q"), value.alias))
                .from_(select.subquery("_q", copy=False), copy=False)
                .group_by(exp.column(value.alias, "_q"), copy=False)
            )
    elif not find_in_scope(value.this, exp.AggFunc):
        select = select.group_by(value.this, copy=False)

    parent_select.join(
        select,
        on=column.eq(join_key),
        join_type="LEFT",
        join_alias=alias,
        copy=False,
    )


def _decorrelate_single_comparison_exists(
    select, parent_select: exp.Select, external_columns, next_alias_name
) -> bool:
    # Only decorrelate an inner SELECT that is the query of an EXISTS predicate
    # (possibly wrapped in NOT) in the parent SELECT's WHERE. Require all correlated
    # references to occur in the inner WHERE, and reject grouping, aggregation,
    # row-shaping, or ordering clauses.
    #
    #   SELECT x.id                     -- parent_select
    #   FROM x AS x
    #   WHERE                           -- parent_clause
    #     EXISTS (                      -- parent_predicate; potentially wrapped in NOT
    #       SELECT 1                    -- inner SELECT
    #       FROM y AS y
    #       WHERE NOT (y.id = x.id)     -- inner WHERE / condition
    #     )

    # The inner SELECT must not have clauses that alter grouping, row shape, or ordering.
    if any(
        select.args.get(arg) for arg in ("group", "having", "qualify", "order", "distinct")
    ) or find_in_scope(select, exp.AggFunc):
        return False

    # The inner SELECT must have a WHERE clause.
    where = select.args.get("where")
    if not where:
        return False

    # Every correlated column reference must occur in this inner WHERE.
    if any(column.find_ancestor(exp.Where) is not where for column in external_columns):
        return False

    # The inner SELECT must belong to an EXISTS predicate.
    parent_predicate = select.find_ancestor(exp.Predicate)
    if not isinstance(parent_predicate, exp.Exists):
        return False

    # The EXISTS must resolve directly to this inner SELECT after removing wrappers.
    if parent_predicate.this.unnest() is not select:
        return False

    # The EXISTS must occur in a WHERE clause.
    parent_clause = parent_predicate.find_ancestor(exp.Where, exp.Having, exp.Join)
    if not isinstance(parent_clause, exp.Where):
        return False

    # That WHERE must belong to the expected parent SELECT.
    if parent_clause.parent_select is not parent_select:
        return False

    # The parent SELECT must not group or aggregate its output.
    if parent_select.args.get("group") or any(
        find_in_scope(projection, exp.AggFunc) for projection in parent_select.selects
    ):
        return False

    # The inner condition must be a supported bare or negated comparison.
    condition = where.this.unnest()
    if isinstance(condition, exp.Not):
        comparison = condition.this.unnest()
        if not isinstance(comparison, (exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE)):
            return False
    elif isinstance(condition, (exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)):
        comparison = condition
    else:
        return False

    # Determine which side of the comparison contains outer-scope column references.
    external_ids = {id(column) for column in external_columns}
    left_is_external = any(id(node) in external_ids for node in comparison.left.walk())
    right_is_external = any(id(node) in external_ids for node in comparison.right.walk())

    # Exactly one comparison operand must depend on the outer scope.
    if left_is_external == right_is_external:
        return False

    # Inner operand must depend on an inner column (and not be a constant-only expression).
    inner_key = comparison.right if left_is_external else comparison.left
    if not inner_key.find(exp.Column):
        return False

    # The outer operand must not contain inner-scope columns because they are unavailable
    # after the inner query is aggregated.
    outer_operand = comparison.left if left_is_external else comparison.right
    if any(
        isinstance(node, exp.Column) and id(node) not in external_ids
        for node in outer_operand.walk()
    ):
        return False

    # The lambda parameter name must not shadow an outer identifier.
    lambda_parameter_name = "_x"
    if any(
        identifier.name.lower() == lambda_parameter_name
        for identifier in outer_operand.find_all(exp.Identifier)
    ):
        return False

    # Replace only the copied inner operand with the lambda parameter.
    lambda_condition = condition.copy()
    copied_comparison = (
        lambda_condition.this.unnest()
        if isinstance(lambda_condition, exp.Not)
        else lambda_condition
    )
    copied_inner_operand = copied_comparison.right if left_is_external else copied_comparison.left
    copied_inner_operand.replace(exp.to_identifier(lambda_parameter_name))

    # The aggregate subquery alias names the one-row aggregate subquery; the inner keys
    # alias names its array.
    aggregate_subquery_alias = next_alias_name()
    inner_keys_alias = next_alias_name()
    inner_keys_column = exp.column(inner_keys_alias, aggregate_subquery_alias)

    # Evaluate the inner relation once, collecting every candidate key into a single array.
    select.set(
        "expressions",
        [exp.alias_(exp.ArrayAgg(this=inner_key.copy()), inner_keys_alias, quoted=False)],
    )

    # Remove the correlation by setting the WHERE clause to TRUE.
    where.set("this", exp.true())

    # Replace EXISTS with a per-outer-row array predicate: require a nonempty array
    # with at least one value satisfying the original condition. Map NULL/UNKNOWN to FALSE.
    parent_predicate.replace(
        exp.Coalesce(
            this=exp.and_(
                exp.ArraySize(this=inner_keys_column.copy()).neq(0),
                exp.ArrayAny(
                    this=inner_keys_column,
                    expression=exp.Lambda(
                        this=lambda_condition,
                        expressions=[exp.to_identifier(lambda_parameter_name)],
                    ),
                ),
                copy=False,
            ),
            expressions=[exp.false()],
        )
    )

    # Attach the aggregated inner-key array to each outer row without expanding x × y.
    # LEFT JOIN preserves outer rows when that array is absent or NULL.
    parent_select.join(
        select,
        on=exp.true(),
        join_type="LEFT",
        join_alias=aggregate_subquery_alias,
        copy=False,
    )
    return True


def decorrelate(select, parent_select, external_columns, next_alias_name):
    where = select.args.get("where")

    if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
        return
    # Handle a lone inequality before the generic equality-key decorrelator can discard
    # its negation. Returning here prevents the same subquery from being rewritten twice.
    if _decorrelate_single_comparison_exists(
        select, parent_select, external_columns, next_alias_name
    ):
        return

    table_alias = next_alias_name()
    keys = []

    # for all external columns in the where statement, find the relevant predicate
    # keys to convert it into a join
    for column in external_columns:
        if column.find_ancestor(exp.Where) is not where:
            return

        # A generic join key may be a top-level conjunct wrapped in parentheses, but it
        # must not sit beneath NOT or another semantic operator. Walking directly to WHERE
        # used to hide that distinction and allowed NOT (inner = outer) to collapse.
        predicate = column.find_ancestor(exp.Predicate)
        ancestor = predicate.parent if predicate else None
        while isinstance(ancestor, (exp.And, exp.Paren)):
            ancestor = ancestor.parent

        if ancestor is not where:
            return

        if isinstance(predicate, exp.Binary):
            key = (
                predicate.right
                if any(node is column for node in predicate.left.walk())
                else predicate.left
            )
        else:
            return

        keys.append((key, column, predicate))

    if not any(isinstance(predicate, exp.EQ) for *_, predicate in keys):
        return

    is_subquery_projection = any(
        node is select.parent
        for node in map(lambda s: s.unalias(), parent_select.selects)
        if isinstance(node, exp.Subquery)
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
                key_aliases[key] = next_alias_name()
            # all predicates that are equalities must also be in the unique
            # so that we don't do a many to many join
            if isinstance(predicate, exp.EQ) and key not in group_by:
                group_by.append(key)

    parent_predicate = select.find_ancestor(exp.Predicate)

    # When the subquery is embedded inside a function (e.g. COALESCE, TRIM) in the SELECT list,
    # the ancestor chain contains no Predicate node AND the subquery is not a direct projection.
    if parent_predicate is None and not is_subquery_projection:
        return

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
        select.set("expressions", [])

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
    op_type = type(parent_predicate.parent) if parent_predicate else None

    if isinstance(parent_predicate, exp.Exists):
        alias = exp.column(list(key_aliases.values())[0], table_alias)
        parent_predicate = _replace(parent_predicate, f"NOT {alias} IS NULL")
    elif isinstance(parent_predicate, exp.All):
        assert issubclass(op_type, exp.Binary)
        predicate = op_type(this=other, expression=exp.column("_x"))
        parent_predicate = _replace(
            parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})"
        )
    elif isinstance(parent_predicate, exp.Any):
        assert issubclass(op_type, exp.Binary)
        if value.this in group_by:
            predicate = op_type(this=other, expression=alias)
            parent_predicate = _replace(parent_predicate.parent, predicate)
        else:
            predicate = op_type(this=other, expression=exp.column("_x"))
            parent_predicate = _replace(parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})")
    elif isinstance(parent_predicate, exp.In):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate, f"{other} = {alias}")
        else:
            parent_predicate = _replace(
                parent_predicate,
                f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
            )
    else:
        if is_subquery_projection and select.parent.alias:
            alias = exp.alias_(alias, select.parent.alias)

        # COUNT always returns 0 on empty datasets, so we need take that into consideration here
        # by transforming all counts into 0 and using that as the coalesced value
        if value.find(exp.Count):

            def remove_aggs(node):
                if isinstance(node, exp.Count):
                    return exp.Literal.number(0)
                elif isinstance(node, exp.AggFunc):
                    return exp.null()
                return node

            alias = exp.Coalesce(this=alias, expressions=[value.this.transform(remove_aggs)])

        select.parent.replace(alias)

    for key, column, predicate in keys:
        predicate.replace(exp.true())
        nested = exp.column(key_aliases[key], table_alias)

        if is_subquery_projection:
            key.replace(nested)
            if not isinstance(predicate, exp.EQ):
                parent_select.where(predicate, copy=False)
            continue

        if key in group_by:
            key.replace(nested)
        elif isinstance(predicate, exp.EQ):
            parent_predicate = _replace(
                parent_predicate,
                f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
            )
        else:
            key.replace(exp.to_identifier("_x"))
            parent_predicate = _replace(
                parent_predicate,
                f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
            )

    parent_select.join(
        select.group_by(*group_by, copy=False),
        on=[predicate for *_, predicate in keys if isinstance(predicate, exp.EQ)],
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )


def _replace(expression: exp.Expr, condition: exp.ExpOrStr) -> exp.Expr:
    return expression.replace(exp.condition(condition))


def _other_operand(expression: object) -> exp.Expr | None:
    if isinstance(expression, exp.In):
        return expression.this

    if isinstance(expression, (exp.Any, exp.All)):
        return _other_operand(expression.parent)

    if isinstance(expression, exp.Binary):
        return (
            expression.right
            if isinstance(expression.left, (exp.Subquery, exp.Any, exp.Exists, exp.All))
            else expression.left
        )

    return None
