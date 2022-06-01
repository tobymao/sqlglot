from sqlglot.optimizer.simplify import simplify
import sqlglot.expressions as exp


def conjunctive_normal_form(expression):
    expression = simplify(expression)
    expression = expression.transform(double_negation, copy=False)
    expression = expression.transform(de_morgans_law, copy=False)
    while True:
        start = hash(expression)
        expression = simplify(distributive_law(expression))
        if start == hash(expression):
            break

    return expression


def double_negation(expression):
    """
    NOT NOT x -> x
    """
    if isinstance(expression, exp.Not) and isinstance(expression.this, exp.Not):
        return expression.this.this
    return expression


def de_morgans_law(expression):
    """
    NOT (x OR y) -> NOT x AND NOT y
    NOT (x AND y) -> NOT x OR NOT y
    """

    if isinstance(expression, exp.Not) and isinstance(expression.this, exp.Paren):
        condition = _inner(expression.this)

        if isinstance(condition, exp.And):
            return exp.or_(exp.not_(condition.left), exp.not_(condition.right))

        if isinstance(condition, exp.Or):
            return exp.and_(exp.not_(condition.left), exp.not_(condition.right))

    return expression


def distributive_law(expression):
    """
    x OR (y AND z) -> (x OR y) AND (x OR z)
    (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
    """
    if isinstance(expression, exp.Or):
        l = _inner(expression.left)
        r = _inner(expression.right)

        if isinstance(r, exp.And):
            if _is_nested(l):
                exp.replace_children(
                    l,
                    lambda c: exp.and_(
                        exp.paren(exp.or_(c, r.left)),
                        exp.paren(exp.or_(c, r.right)),
                    ),
                )
            else:
                l = exp.and_(exp.or_(l, r.left), exp.or_(l, r.right))
            return l
        if isinstance(l, exp.And):
            if _is_nested(r):
                exp.replace_children(
                    r,
                    lambda c: exp.and_(
                        exp.paren(exp.or_(c, l.left)),
                        exp.paren(exp.or_(c, l.right)),
                    ),
                )
            else:
                r = exp.and_(exp.or_(r, l.left), exp.or_(r, l.right))
            return r

    exp.replace_children(expression, distributive_law)
    return expression


def _is_nested(expression):
    return bool(expression.find(exp.And, exp.Or))


def _inner(expression):
    while isinstance(expression, exp.Paren):
        expression = expression.this
    return expression
