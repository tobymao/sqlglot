from sqlglot.helper import while_changing
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

    def _simplify(expression):
        expression = expression.transform(simplify_equality, copy=False)
        expression = expression.transform(simplify_not, copy=False)
        expression = expression.transform(simplify_conjunctions, copy=False)
        # future work
        # elimination, absorbption, and commutativity
        expression = expression.transform(simplify_parens, copy=False)
        remove_where_true(expression)
        return expression

    expression = while_changing(expression, _simplify)
    return expression


def simplify_equality(expression):
    if isinstance(expression, exp.EQ):
        left = expression.left
        right = expression.right

        if NULL in (left, right):
            return NULL
        if left == right:
            return TRUE
        if (
            isinstance(left, exp.Literal)
            and isinstance(right, exp.Literal)
            and left.is_string
            and right.is_string
            and left != right
        ):
            return FALSE
    return expression


def simplify_not(expression):
    if isinstance(expression, exp.Not):
        if always_true(expression.this):
            return FALSE
        if expression.this == FALSE:
            return TRUE
    return expression


def simplify_conjunctions(expression):
    if isinstance(expression, (exp.And, exp.Or)):
        left = expression.left
        right = expression.right

        if isinstance(expression, exp.And):
            flatten(left, right, kind=exp.And)

            if NULL in (left, right):
                return NULL
            if FALSE in (left, right):
                return FALSE
            if always_true(left) and always_true(right):
                return TRUE
            if always_true(left):
                return right
            if always_true(right):
                return left
            if left == right:
                return left
            if is_complement(left, right):
                return FALSE
        elif isinstance(expression, exp.Or):
            flatten(left, right, kind=exp.Or)

            if always_true(left) or always_true(right):
                return TRUE
            if left == FALSE and right == FALSE:
                return FALSE
            if (
                (left == NULL and right == NULL)
                or (left == NULL and right == FALSE)
                or (left == FALSE and right == NULL)
            ):
                return NULL
            if left == FALSE:
                return right
            if right == FALSE:
                return left
            if left == right:
                return left
            if is_complement(left, right):
                return TRUE
    elif isinstance(expression, exp.Not) and isinstance(expression.this, exp.Not):
        # double negation
        # NOT NOT x -> x
        return expression.this.this
    return expression


def flatten(*expressions, kind):
    for expression in expressions:
        if isinstance(expression, exp.Paren) and isinstance(expression.this, kind):
            expression.replace(expression.this)


def is_complement(a, b):
    return exp.not_(a) == b or exp.not_(b) == a


def simplify_parens(expression):
    if isinstance(expression, exp.Paren) and not isinstance(
        expression.this, exp.Binary
    ):
        return expression.this
    return expression


def remove_where_true(expression):
    for where in expression.find_all(exp.Where):
        if always_true(where.this):
            where.parent.args["where"] = None


def always_true(expression):
    return expression == TRUE or isinstance(expression, exp.Literal)
