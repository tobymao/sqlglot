from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope


def quote_identities(expression, unquote_values_columns=False):
    """
    Rewrite sqlglot AST to ensure all identities are quoted.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT x.a AS a FROM db.x")
        >>> quote_identities(expression).sql()
        'SELECT "x"."a" AS "a" FROM "db"."x"'

    If `unquote_values_columns` is True then the columns from a derived table VALUES expression
    are not quoted in the SELECT expression nor the VALUES alias expression:
        >>> expression = sqlglot.parse_one("SELECT tab.a AS a FROM (VALUES (1)) AS tab(a)")
        >>> quote_identities(expression, unquote_values_columns=True).sql()
        'SELECT "tab".a AS "a" FROM (VALUES (1)) AS "tab"(a)'

    `unquote_values_columns` is required for Snowflake.

    Args:
        expression (sqlglot.Expression): expression to quote
        unquote_values_columns (bool): Indicates whether to unquote values defined in a VALUES expression.
    Returns:
        sqlglot.Expression: quoted expression
    """

    def qualify(node):
        if isinstance(node, exp.Identifier):
            node.set("quoted", True)
        return node

    quoted_expression = expression.transform(qualify, copy=False)
    if not unquote_values_columns:
        return quoted_expression

    for scope in traverse_scope(quoted_expression):
        if isinstance(scope.expression, exp.Values):
            alias = scope.expression.args.get("alias")
            if not alias:
                continue
            for column in alias.args.get("columns", []):
                column.set("quoted", False)
        if isinstance(scope.expression, exp.Select):
            values_scopes = [
                scope
                for scope in scope.sources.values()
                if isinstance(scope.expression, exp.Values)
            ]
            values_columns = []
            for value_scope in values_scopes:
                values_columns.extend(value_scope.outer_column_list)
            selected_value_columns = [
                sel_column for sel_column in scope.columns if sel_column.name in values_columns
            ]
            for selected_value_column in selected_value_columns:
                selected_value_column.this.set("quoted", False)
    return quoted_expression
