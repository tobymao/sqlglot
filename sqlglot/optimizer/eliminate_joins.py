from sqlglot import expressions as exp
from sqlglot.optimizer.normalize import normalized
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify


def eliminate_joins(expression):
    for scope in traverse_scope(expression):
        if scope.unqualified_columns:
            continue

        joins = scope.expression.args.get("joins")
        if not joins:
            continue

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
            on_clause_columns = set(
                id(column)
                for column in on.find_all(exp.Column)
            )
            used = any(
                column
                for column in scope.source_columns(alias)
                if id(column) not in on_clause_columns
            )

            if used:
                continue

            inner_scope = scope.sources.get(alias)
            unique_outputs = _unique_outputs(inner_scope)

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

        unique_outputs = set()
        for select in scope.selects:
            if select.unalias() in grouped_expressions:
                unique_outputs.add(select.alias_or_name)
        return unique_outputs

    return set()


def join_condition(join):
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
    join_condition = None if on == exp.TRUE else on

    return source_key, join_key, join_condition
