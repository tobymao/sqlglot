import sqlglot.expressions as exp
from sqlglot.optimizer.helper import quote


def qualify_tables(expression, db=None, catalog=None):
    """
    Rewrite sqlglot AST to have fully qualified tables.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM "db"."tbl" AS "tbl"'

    Args:
        expression (sqlglot.Expression): expression to qualify
        db (str): Database name
        catalog (str): Catalog name
    Returns:
        sqlglot.Expression: qualified expression
    """

    def qualify(node):
        if isinstance(node, exp.Table):
            if not node.args.get("db"):
                node.set("db", exp.to_identifier(db))
            if not node.args.get("catalog"):
                node.set("catalog", exp.to_identifier(catalog))

            quote(node, "db")
            quote(node, "catalog")
            quote(node, "this")

            if not isinstance(node.parent, exp.Alias):
                node = exp.alias_(node, node.this)
                quote(node, "alias")

            return node

        return node

    return expression.transform(qualify)
