from sqlglot import exp


def expand_multi_table_selects(expression):
    """
    Replace multiple FROM expressions with JOINs.

    Example:
        >>> from sqlglot import parse_one
        >>> expand_multi_table_selects(parse_one("SELECT * FROM x, y")).sql()
        'SELECT * FROM x CROSS JOIN y'
    """
    for from_ in expression.find_all(exp.From):
        parent = from_.parent

        for query in from_.expressions[1:]:
            parent.join(
                query,
                join_type="CROSS",
                copy=False,
            )
            from_.expressions.remove(query)

    return expression
