from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.scope import traverse_scope


def isolate_table_selects(expression):
    for scope in traverse_scope(expression):
        if len(scope.selected_sources) == 1:
            continue

        for (_, source) in scope.selected_sources.values():
            if not isinstance(source, exp.Table):
                continue

            if not source.alias:
                raise OptimizeError("Tables require an alias. Run qualify_tables optimization.")

            source.replace(
                exp.select("*")
                .from_(
                    alias(source.copy(), source.name or source.alias, table=True),
                    copy=False,
                )
                .subquery(source.alias, copy=False)
            )

    return expression
