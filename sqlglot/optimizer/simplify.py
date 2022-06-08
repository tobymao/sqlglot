import itertools

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
        expression = expression.transform(flatten, copy=False)
        expression = expression.transform(simplify_conjunctions, copy=False)
        expression = expression.transform(compare_and_prune, copy=False)
        expression = expression.transform(absorb, copy=False)
        expression = expression.transform(eliminate, copy=False)
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
        if isinstance(expression.this, exp.Not):
            # double negation
            # NOT NOT x -> x
            return expression.this.this
    return expression


def simplify_conjunctions(expression):
    if isinstance(expression, exp.Connector):
        left = expression.left
        right = expression.right

        if isinstance(expression, exp.And):
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
        elif isinstance(expression, exp.Or):
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
    return expression


def compare_and_prune(expression):
    """
    Sorts ANDs and ORs, removing duplicates and compliment expressions.
    """
    if isinstance(expression, exp.And):
        return _compare_and_prune(expression, FALSE, exp.and_)
    if isinstance(expression, exp.Or):
        return _compare_and_prune(expression, TRUE, exp.or_)
    return expression


def _compare_and_prune(connector, compliment, result_func):
    args = set(connector.flatten())

    for a, b in itertools.combinations(args, 2):
        if is_complement(a, b):
            return compliment

    return result_func(*sorted(args, key=lambda a: a.sql()))


def flatten(expression):
    """
    A AND (B AND C) -> A AND B AND C
    A OR (B OR C) -> A OR B OR C
    """
    if isinstance(expression, exp.Connector):
        for node in expression.args.values():
            child = node.unnest()
            if isinstance(child, expression.__class__):
                node.replace(child)
    return expression


def absorb(expression):
    """
    A AND (A OR B) -> A
    A OR (A AND B) -> A
    A AND (NOT A OR B) -> A AND B
    A OR (NOT A AND B) -> A OR B
    """
    if isinstance(expression, exp.And):
        return _absorb(expression, exp.Or)
    if isinstance(expression, exp.Or):
        return _absorb(expression, exp.And)
    return expression


def _absorb(connector, kind):
    for a, b in itertools.permutations(connector.unnest_operands()):
        if isinstance(a, kind):
            if b in (a.left, a.right):
                return b
            if exp.not_(b) == a.left:
                return connector.__class__(this=b, expression=a.right)
            if exp.not_(b) == a.right:
                return connector.__class__(this=b, expression=a.left)
    return connector


def eliminate(expression):
    """
    (A AND B) OR (A AND NOT B) -> A
    (A OR B) AND (A OR NOT B) -> A
    """
    if isinstance(expression, exp.Or):
        return _eliminate(expression, exp.And)
    if isinstance(expression, exp.And):
        return _eliminate(expression, exp.Or)
    return expression


def _eliminate(connector, kind):
    left, right = connector.unnest_operands()

    if isinstance(left, kind) and isinstance(right, kind):
        aa, ab = left.unnest_operands()
        ba, bb = right.unnest_operands()

        for lhs, rhs in [
            ([(aa, ab), (ab, aa)], (ba, bb)),
            ([(ba, bb), (bb, ba)], (aa, ab)),
        ]:
            for a, b in lhs:
                if a in rhs and exp.not_(b) in rhs:
                    return a
    return connector


def is_complement(a, b):
    return exp.not_(a) == b or exp.not_(b) == a


def simplify_parens(expression):
    if (
        isinstance(expression, exp.Paren)
        and not isinstance(expression.this, exp.Select)
        and (
            not isinstance(expression.parent, (exp.Condition, exp.Binary))
            or not isinstance(expression.this, exp.Binary)
        )
    ):
        return expression.this
    return expression


def remove_where_true(expression):
    for where in expression.find_all(exp.Where):
        if always_true(where.this):
            where.parent.set("where", None)
    for join in expression.find_all(exp.Join):
        if always_true(join.args.get("on")):
            join.set("kind", "CROSS")
            join.set("on", None)


def always_true(expression):
    return expression == TRUE or isinstance(expression, exp.Literal)
