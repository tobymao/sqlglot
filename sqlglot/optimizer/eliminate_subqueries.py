import itertools

from sqlglot import alias, exp, select, table
from sqlglot.optimizer.simplify import simplify
from sqlglot.optimizer.scope import traverse_scope


def eliminate_subqueries(expression):
    """
    Rewrite duplicate subqueries from sqlglot AST.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 AS x, 2 AS y UNION ALL SELECT 1 AS x, 2 AS y")
        >>> eliminate_subqueries(expression).sql()
        'WITH _e_0 AS (SELECT 1 AS x, 2 AS y) SELECT * FROM _e_0 UNION ALL SELECT * FROM _e_0'

    Args:
        expression (sqlglot.Expression): expression to qualify
        schema (dict|sqlglot.optimizer.Schema): Database schema
    Returns:
        sqlglot.Expression: qualified expression
    """
    expression = simplify(expression)
    queries = {}

    for scope in traverse_scope(expression):
        query = scope.expression
        queries[query] = queries.get(query, []) + [query]

    sequence = itertools.count()

    for query, duplicates in queries.items():
        if len(duplicates) == 1:
            continue

        alias_ = f"_e_{next(sequence)}"

        for dup in duplicates:
            parent = dup.parent
            if isinstance(parent, exp.Subquery):
                parent.replace(alias(table(alias_), parent.alias_or_name, table=True))
            elif isinstance(parent, exp.Union):
                dup.replace(select("*").from_(alias_))

        expression.with_(alias_, as_=query, copy=False)

    return expression
