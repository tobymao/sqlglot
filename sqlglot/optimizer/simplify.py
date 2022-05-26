from sqlglot.expressions import FALSE, NULL, TRUE
import sqlglot.expressions as exp


def simplify(expression):
    """
    Rewrite sqlglot AST to simplify expressions.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("TRUE AND TRUE")
        >>> simplify(expression).sql()
        'TRUE'

    Args:
        expression (sqlglot.Expression): expression to simplify
    Returns:
        sqlglot.Expression: simplified expression
    """
    expression = expression.copy()

    # wrap because you cannot replace a node without a parent
    expression = exp.Paren(this=expression)

    while True:
        start = hash(expression)
        expression = simplify_equality(expression)
        expression = simplify_not(expression)
        expression = simplify_conjunctions(expression)
        expression = simplify_parens(expression)

        if start == hash(expression):
            break

    return expression.this


def simplify_equality(expression):
    for node in reverse_traverse(expression, exp.EQ):
        left = node.left
        right = node.right

        if NULL in (left, right):
            node.replace(NULL)
        elif left == right:
            node.replace(TRUE)
        elif (
            isinstance(left, exp.Literal)
            and isinstance(right, exp.Literal)
            and left.is_string
            and right.is_string
            and left != right
        ):
            node.replace(FALSE)
    return expression


def simplify_not(expression):
    for node in reverse_traverse(expression, exp.Not):
        if always_true(node.this):
            node.replace(FALSE)
        elif node.this == FALSE:
            node.replace(TRUE)
    return expression


def simplify_conjunctions(expression):
    for node in reverse_traverse(expression, exp.And, exp.Or):
        left = node.left
        right = node.right

        if isinstance(node, exp.And):
            if NULL in (left, right):
                node.replace(NULL)
            elif FALSE in (left, right):
                node.replace(FALSE)
            elif always_true(left) and always_true(right):
                node.replace(TRUE)
        elif isinstance(node, exp.Or):
            if always_true(left) or always_true(right):
                node.replace(TRUE)
            elif left == FALSE and right == FALSE:
                node.replace(FALSE)
            elif (
                (left == NULL and right == NULL)
                or (left == NULL and right == FALSE)
                or (left == FALSE and right == NULL)
            ):
                node.replace(NULL)
            elif left == FALSE:
                node.replace(right)
            elif right == FALSE:
                node.replace(left)
    return expression


def simplify_parens(expression):
    for node in reverse_traverse(expression, exp.Paren):
        if not isinstance(node.this, exp.Binary):
            node.replace(node.this)
    return expression


def always_true(expression):
    return expression == TRUE or isinstance(expression, exp.Literal)


def reverse_traverse(expression, *types):
    for node in reversed(list(expression.find_all(types or exp.Expression, bfs=False))):
        yield node
