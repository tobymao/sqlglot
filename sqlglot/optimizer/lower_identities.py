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
    # We need to leave the output aliases unchanged, so the selects need special handling
    _lower_selects(expression)

    # These clauses can reference output aliases and also need special handling
    _lower_order(expression)
    _lower_having(expression)

    # We've already handled these args, so don't traverse into them
    traversed = {"expressions", "order", "having"}

    if isinstance(expression, exp.Subquery):
        # Root subquery, e.g. (SELECT A AS A FROM X) LIMIT 1
        lower_identities(expression.this)
        traversed |= {"this"}

    if isinstance(expression, exp.Union):
        # Union, e.g. SELECT A AS A FROM X UNION SELECT A AS A FROM X
        lower_identities(expression.left)
        lower_identities(expression.right)
        traversed |= {"this", "expression"}

    for k, v in expression.args.items():
        if k in traversed:
            continue

        for child in ensure_collection(v):
            if isinstance(child, exp.Expression):
                child.transform(_lower, copy=False)

    return expression


def _lower_selects(expression):
    for e in expression.expressions:
        # Leave output aliases as-is
        e.unalias().transform(_lower, copy=False)


def _lower_order(expression):
    order = expression.args.get("order")

    if not order:
        return

    output_aliases = {e.alias for e in expression.expressions if isinstance(e, exp.Alias)}

    for ordered in order.expressions:
        # Don't lower references to output aliases
        if not (
            isinstance(ordered.this, exp.Column)
            and not ordered.this.table
            and ordered.this.name in output_aliases
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
