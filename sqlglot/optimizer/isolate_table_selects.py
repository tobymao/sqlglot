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

            if not isinstance(source.parent, exp.Alias):
                raise OptimizeError(
                    "Tables require an alias. Run qualify_tables optimization."
                )

            parent = source.parent

            parent.replace(
                exp.select("*")
                .from_(
                    alias(source, source.name or parent.alias, table=True),
                    copy=False,
                )
                .subquery(parent.alias, copy=False)
            )

    return expression
