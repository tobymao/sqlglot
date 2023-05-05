from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

def expand_join_constructs(expression: exp.Expression) -> exp.Expression:
    """
    Replace "join constructs" (*) by equivalent SELECT * subqueries.

    Example:
    >>> import sqlglot
    >>> expression = sqlglot.parse_one("SELECT * FROM (tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2) AS tbl")
    >>> expand_join_constructs(expression).sql()
    'SELECT * FROM (SELECT * FROM tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2) AS tbl'

    (*) See section 7.2.1.2 in https://www.postgresql.org/docs/current/queries-table-expressions.html
    """
    for scope in traverse_scope(expression):
        if isinstance(scope.expression, exp.Table):
            for join in scope.expression.args.get("joins", []):
                if isinstance(join.this, exp.Subquery):
                    expand_join_constructs(join.this.unnest())

            outermost_subquery = scope.expression.parent
            while isinstance(outermost_subquery.parent, exp.Subquery):
                outermost_subquery = outermost_subquery.parent

            outermost_subquery.this.replace(exp.select("*").from_(scope.expression.copy()))

    return expression
