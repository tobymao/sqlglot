import sqlglot.expressions as exp
from sqlglot.optimizer.scope import traverse_scope


def qualify_tables(expression, db=None, catalog=None):
    """
    Rewrite sqlglot AST to have fully qualified tables.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        db (str): Database name
        catalog (str): Catalog name
    Returns:
        sqlglot.Expression: qualified expression
    """
    for scope in traverse_scope(expression):
        for selectable in scope.selectables.values():
            if isinstance(selectable, exp.Table):
                if not selectable.args.get("db"):
                    selectable.set("db", exp.to_identifier(db))
                if not selectable.args.get("catalog"):
                    selectable.set("catalog", exp.to_identifier(catalog))

                if not isinstance(selectable.parent, exp.Alias):
                    node = exp.alias_(selectable.copy(), selectable.this)
                    selectable.replace(node)

    return expression
