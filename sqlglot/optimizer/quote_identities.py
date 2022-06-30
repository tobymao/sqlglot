from sqlglot import exp


def quote_identities(expression):
    """
    Rewrite sqlglot AST to ensure all identities are quoted.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT x.a AS a FROM db.x")
        >>> quote_identities(expression).sql()
        'SELECT "x"."a" AS "a" FROM "db"."x"'

    Args:
        expression (sqlglot.Expression): expression to quote
    Returns:
        sqlglot.Expression: quoted expression
    """

    def qualify(node):
        if isinstance(node, exp.Identifier):
            node.set("quoted", True)
        return node

    return expression.transform(qualify, copy=False)
