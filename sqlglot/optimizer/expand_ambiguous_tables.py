import sqlglot.expressions as exp
from sqlglot import select, subquery
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.scope import traverse_scope


def expand_ambiguous_tables(expression):
    for scope in traverse_scope(expression):
        if len(scope.sources) == 1:
            continue

        for source in scope.sources.values():
            if not isinstance(source, exp.Table):
                continue

            if not isinstance(source.parent, exp.Alias):
                raise OptimizeError(
                    "Tables require an alias. Run qualify_tables optimization."
                )

            parent = source.parent
            parent.replace(select("*").from_(source.copy()).subquery(parent.alias))

    return expression
