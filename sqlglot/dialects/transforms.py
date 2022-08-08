from sqlglot import expressions as exp


def _unaliased_group_sql(self, expression):
    """
    Generate SQL for a GROUP BY expression that removes references to aliased selects.

    For example, this replaces:
        SELECT a AS b FROM x GROUP BY b
    With:
        SELECT a AS b FROM x GROUP BY 1

    Args:
        self (sqlglot.generator.Generator): generator instance
        expression (sqlglot.expressions.Group): GROUP BY expression
    Returns:
        str: SQL
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

    return super(self.__class__, self).group_sql(expression)


UNALIASED_GROUP = {exp.Group: _unaliased_group_sql}
