from sqlglot import expressions as exp
from sqlglot.optimizer.normalize import normalized
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify


def eliminate_joins(expression):
    """
    Remove unused joins from an expression.

    Currently, this only remove LEFT JOINs when we know that the join condition
    doesn't produce duplicate rows.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT x.a FROM x LEFT JOIN (SELECT DISTINCT y.b FROM y) AS y ON x.b = y.b"
        >>> expression = sqlglot.parse_one(sql)
        >>> eliminate_joins(expression).sql()
        'SELECT x.a FROM x'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    for scope in traverse_scope(expression):
        # If any columns in this scope aren't qualified, it's hard to determine if a join isn't used.
        # It's probably possible to infer this from the outputs of derived tables.
        # But for now, let's just skip this rule.
        if scope.unqualified_columns:
            continue

        joins = scope.expression.args.get("joins", [])

        # Reverse the joins so we can remove chains of unused joins
        for join in reversed(joins):
            if join.side != "LEFT":
                continue

            on = join.args.get("on")
            if not on:
                continue

            alias = join.this.alias_or_name

            # We need to find all columns that reference this join.
            # But columns in the ON clause shouldn't count.
            on_clause_columns = set(id(column) for column in on.find_all(exp.Column))
            join_is_used = any(column for column in scope.source_columns(alias) if id(column) not in on_clause_columns)
            if join_is_used:
                continue

            # The join condition must include the entire set of unique outputs
            inner_scope = scope.sources.get(alias)
            unique_outputs = _unique_outputs(inner_scope)
            if not unique_outputs:
                continue
            _, join_keys, _ = join_condition(join)
            remaining_unique_outputs = unique_outputs - set(c.name for c in join_keys)
            if remaining_unique_outputs:
                continue

            join.pop()
            scope.remove_source(alias)
    return expression


def _unique_outputs(scope):
    if scope.expression.args.get("distinct"):
        return set(scope.expression.named_selects)

    group = scope.expression.args.get("group")
    if group:
        grouped_expressions = set(group.expressions)
        grouped_outputs = set()

        unique_outputs = set()
        for select in scope.selects:
            output = select.unalias()
            if output in grouped_expressions:
                grouped_outputs.add(output)
                unique_outputs.add(select.alias_or_name)

        # All the grouped expressions must be in the output
        if not grouped_expressions.difference(grouped_outputs):
            return unique_outputs
        else:
            return set()

    if all(isinstance(e.unalias(), exp.AggFunc) for e in scope.selects):
        return set(scope.expression.named_selects)

    return set()


def join_condition(join):
    """
    Extract the join condition from a join expression.

    Args:
        join (exp.Join)
    Returns:
        tuple[list[str], list[str], exp.Expression]:
            Tuple of (source key, join key, remaining predicate)
    """
    name = join.this.alias_or_name
    on = join.args.get("on") or exp.TRUE
    on = on.copy()
    source_key = []
    join_key = []

    # find the join keys
    # SELECT
    # FROM x
    # JOIN y
    #   ON x.a = y.b AND y.b > 1
    #
    # should pull y.b as the join key and x.a as the source key
    if normalized(on):
        for condition in on.flatten() if isinstance(on, exp.And) else [on]:
            if isinstance(condition, exp.EQ):
                left, right = condition.unnest_operands()
                left_tables = exp.column_table_names(left)
                right_tables = exp.column_table_names(right)

                if name in left_tables and name not in right_tables:
                    join_key.append(left)
                    source_key.append(right)
                    condition.replace(exp.TRUE)
                elif name in right_tables and name not in left_tables:
                    join_key.append(right)
                    source_key.append(left)
                    condition.replace(exp.TRUE)

    on = simplify(on)
    remaining_condition = None if on == exp.TRUE else on

    return source_key, join_key, remaining_condition
