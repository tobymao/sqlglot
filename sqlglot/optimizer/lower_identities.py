from sqlglot import exp
from sqlglot.helper import ensure_collection


def lower_identities(expression):
    """
    Convert all unquoted identifiers to lower case.

    Assuming the schema is all lower case, this essentially makes identifiers case-insensitive.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('SELECT Bar.A AS A FROM "Foo".Bar')
        >>> lower_identities(expression).sql()
        'SELECT bar.a AS A FROM "Foo".bar'

    Args:
        expression (sqlglot.Expression): expression to quote
    Returns:
        sqlglot.Expression: quoted expression
    """
    args = set(expression.arg_types)

    if isinstance(expression, exp.Subquery):
        lower_identities(expression.this)
        args -= {"this"}

    if isinstance(expression, exp.Union):
        lower_identities(expression.left)
        lower_identities(expression.right)
        args -= {"this", "expression"}

    for arg in args:
        if arg == "expressions":
            _lower_expressions(expression)

        elif arg == "order":
            _lower_order(expression)

        elif arg == "having":
            _lower_having(expression)

        else:
            for e in ensure_collection(expression.args.get(arg)):
                if isinstance(e, exp.Expression):
                    e.transform(_lower, copy=False)

    return expression


def _lower_expressions(expression):
    for e in expression.expressions:
        # Leave output aliases as-is
        e.unalias().transform(_lower, copy=False)


def _lower_order(expression):
    order = expression.args.get("order")
    aliases = {e.alias for e in expression.expressions if isinstance(e, exp.Alias)}

    if not order:
        return

    for ordered in order.expressions:
        # Don't lower references to output aliases
        if not (
            isinstance(ordered.this, exp.Column)
            and not ordered.this.table
            and ordered.this.name in aliases
        ):
            ordered.transform(_lower, copy=False)


def _lower_having(expression):
    having = expression.args.get("having")

    if not having:
        return

    # Don't lower references to output aliases
    for agg in having.find_all(exp.AggFunc):
        agg.transform(_lower, copy=False)


def _lower(node):
    if isinstance(node, exp.Identifier) and not node.quoted:
        node.set("this", node.this.lower())
    return node
