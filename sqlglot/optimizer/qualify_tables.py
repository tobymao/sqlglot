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
