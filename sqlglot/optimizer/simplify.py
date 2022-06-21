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
        expression = expression.transform(absorb_and_eliminate, copy=False)
        expression = expression.transform(simplify_parens, copy=False)
        return expression

    expression = while_changing(expression, _simplify)
    remove_where_true(expression)
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

        if left == right:
            return left

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
    args = {
        expression.sql(normalize=True, identify=True): expression
        for expression in connector.flatten()
    }

    for a, b in itertools.combinations(args.values(), 2):
        if is_complement(a, b):
            return compliment

    return result_func(*(args[sql] for sql in sorted(args)))


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


def absorb_and_eliminate(expression):
    """
    absorption:
        A AND (A OR B) -> A
        A OR (A AND B) -> A
        A AND (NOT A OR B) -> A AND B
        A OR (NOT A AND B) -> A OR B
    elimination:
        (A AND B) OR (A AND NOT B) -> A
        (A OR B) AND (A OR NOT B) -> A
    """
    if isinstance(expression, exp.And):
        return _absorb_and_eliminate(expression, exp.Or)
    if isinstance(expression, exp.Or):
        return _absorb_and_eliminate(expression, exp.And)
    return expression


def _absorb_and_eliminate(connector, kind):
    for a, b in itertools.permutations(connector.flatten(), 2):
        a = a.unnest()
        b = b.unnest()
        if isinstance(a, kind):
            aa, ab = a.unnest_operands()

            # absorb
            if b in (aa, ab):
                a.replace(exp.FALSE if kind == exp.And else exp.TRUE)
            elif is_not(b, aa):
                aa.replace(exp.TRUE if kind == exp.And else exp.FALSE)
            elif is_not(b, ab):
                ab.replace(exp.TRUE if kind == exp.And else exp.FALSE)
            elif isinstance(b, kind):
                # eliminate
                rhs = b.unnest_operands()
                ba, bb = rhs

                if aa in rhs and (is_not(ab, ba) or is_not(ab, bb)):
                    a.replace(aa)
                    b.replace(aa)
                elif ab in rhs and (is_not(aa, ba) or is_not(aa, bb)):
                    a.replace(ab)
                    b.replace(ab)

    return connector


def is_complement(a, b):
    return is_not(a, b) or is_not(b, a)


def is_not(a, b):
    return isinstance(b, exp.Not) and b.this == a


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
