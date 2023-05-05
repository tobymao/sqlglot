from sqlglot import exp


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

    def _expand_join_constructs(expression: exp.Expression) -> exp.Expression:
        if isinstance(expression, exp.Subquery):
            unnested = expression.unnest()
            if isinstance(unnested, exp.Table):
                expression.this.replace(exp.select("*").from_(unnested.copy(), copy=False))

        return expression

    return expression.transform(_expand_join_constructs, copy=False)
