from sqlglot import expressions as exp


def quote(expression, arg):
    """If `arg` of `expression` is an Identifier, ensure it's quoted"""
    node = expression.args.get(arg)
    if node and isinstance(node, exp.Identifier):
        node.set("quoted", True)
