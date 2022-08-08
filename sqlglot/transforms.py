from sqlglot import expressions as exp


def unalias_group(expression):
    """
    Replace references to select aliases in GROUP BY clauses.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT a AS b FROM x GROUP BY b").transform(unalias_group).sql()
        'SELECT a AS b FROM x GROUP BY 1'
    """
    if isinstance(expression, exp.Group) and isinstance(expression.parent, exp.Select):
        aliased_selects = {
            e.alias: i
            for i, e in enumerate(expression.parent.expressions, start=1)
            if isinstance(e, exp.Alias)
        }

        expression = expression.copy()

        for col in expression.find_all(exp.Column):
            alias_index = aliased_selects.get(col.name)
            if not col.table and alias_index:
                col.replace(exp.Literal.number(alias_index))

    return expression


def preprocess(transforms, to_sql):
    """
    Create a new transform function that can be used a value in `Generator.TRANSFORMS`
    to convert expressions to SQL.

    Args:
        transforms (list[(exp.Expression) -> exp.Expression]):
            Sequence of transform functions. These will be called in order.
        to_sql ((sqlglot.generator.Generator, exp.Expression) -> str):
            Final transform that converts the resulting expression to a SQL string.
    Returns:
        (sqlglot.generator.Generator, exp.Expression) -> str:
            Function that can be used as a generator transform.
    """

    def _to_sql(self, expression):
        expression = transforms[0](expression)
        for t in transforms[1:]:
            expression = t(expression)
        return to_sql(self, expression)

    return _to_sql


def delegate(attr):
    """
    Create a new method that delegates to `attr`.

    This is useful for creating `Generator.TRANSFORMS` functions that delegate
    to existing generator methods.
    """

    def _transform(self, *args, **kwargs):
        return getattr(self, attr)(*args, **kwargs)

    return _transform


UNALIAS_GROUP = {exp.Group: preprocess([unalias_group], delegate("group_sql"))}
