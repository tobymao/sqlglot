import sqlglot.expressions as exp


def qualify_tables(expression, db=None, catalog=None):
    def qualify(node):
        if isinstance(node, exp.Table):
            if not node.args.get("db"):
                node.set("db", db)
            if not node.args.get("catalog"):
                node.set("catalog", catalog)
            return node
        return node

    return expression.transform(qualify)


def qualify_columns(expression, _schema):
    return expression


def optimize(expression, schema=None, db=None, catalog=None):
    """
    Rewrite a sqlglot AST into an optimized form.
    """
    expression = qualify_tables(expression, db=db, catalog=catalog)
    expression = qualify_columns(expression, schema)
    return expression
