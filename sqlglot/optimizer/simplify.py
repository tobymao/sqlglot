import datetime
import functools
import itertools
from collections import deque
from decimal import Decimal

from sqlglot import exp
from sqlglot.expressions import FALSE, NULL, TRUE
from sqlglot.generator import Generator
from sqlglot.helper import first, while_changing

GENERATOR = Generator(normalize=True, identify=True)


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

    def _simplify(expression, root=True):
        node = expression
        node = rewrite_between(node)
        node = uniq_sort(node)
        node = absorb_and_eliminate(node)
        exp.replace_children(node, lambda e: _simplify(e, False))
        node = simplify_not(node)
        node = flatten(node)
        node = simplify_connectors(node)
        node = remove_compliments(node)
        node.parent = expression.parent
        node = simplify_literals(node)
        node = simplify_parens(node)
        if root:
            expression.replace(node)
        return node

    expression = while_changing(expression, _simplify)
    remove_where_true(expression)
    return expression


def rewrite_between(expression: exp.Expression) -> exp.Expression:
    """Rewrite x between y and z to x >= y AND x <= z.

    This is done because comparison simplification is only done on lt/lte/gt/gte.
    """
    if isinstance(expression, exp.Between):
        return exp.and_(
            exp.GTE(this=expression.this.copy(), expression=expression.args["low"]),
            exp.LTE(this=expression.this.copy(), expression=expression.args["high"]),
        )
    return expression


def simplify_not(expression):
    """
    Demorgan's Law
    NOT (x OR y) -> NOT x AND NOT y
    NOT (x AND y) -> NOT x OR NOT y
    """
    if isinstance(expression, exp.Not):
        if isinstance(expression.this, exp.Null):
            return exp.null()
        if isinstance(expression.this, exp.Paren):
            condition = expression.this.unnest()
            if isinstance(condition, exp.And):
                return exp.or_(exp.not_(condition.left), exp.not_(condition.right))
            if isinstance(condition, exp.Or):
                return exp.and_(exp.not_(condition.left), exp.not_(condition.right))
            if isinstance(condition, exp.Null):
                return exp.null()
        if always_true(expression.this):
            return exp.false()
        if expression.this == FALSE:
            return exp.true()
        if isinstance(expression.this, exp.Not):
            # double negation
            # NOT NOT x -> x
            return expression.this.this
    return expression


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


def simplify_connectors(expression):
    def _simplify_connectors(expression, left, right):
        if isinstance(expression, exp.Connector):
            if left == right:
                return left
            if isinstance(expression, exp.And):
                if FALSE in (left, right):
                    return exp.false()
                if NULL in (left, right):
                    return exp.null()
                if always_true(left) and always_true(right):
                    return exp.true()
                if always_true(left):
                    return right
                if always_true(right):
                    return left
                return _simplify_comparison(expression, left, right)
            elif isinstance(expression, exp.Or):
                if always_true(left) or always_true(right):
                    return exp.true()
                if left == FALSE and right == FALSE:
                    return exp.false()
                if (
                    (left == NULL and right == NULL)
                    or (left == NULL and right == FALSE)
                    or (left == FALSE and right == NULL)
                ):
                    return exp.null()
                if left == FALSE:
                    return right
                if right == FALSE:
                    return left
                return _simplify_comparison(expression, left, right, or_=True)
        return None

    return _flat_simplify(expression, _simplify_connectors)


LT_LTE = (exp.LT, exp.LTE)
GT_GTE = (exp.GT, exp.GTE)

COMPARISONS = (
    *LT_LTE,
    *GT_GTE,
    exp.EQ,
    exp.NEQ,
)

INVERSE_COMPARISONS = {
    exp.LT: exp.GT,
    exp.GT: exp.LT,
    exp.LTE: exp.GTE,
    exp.GTE: exp.LTE,
}


def _simplify_comparison(expression, left, right, or_=False):
    if isinstance(left, COMPARISONS) and isinstance(right, COMPARISONS):
        ll, lr = left.args.values()
        rl, rr = right.args.values()

        largs = {ll, lr}
        rargs = {rl, rr}

        matching = largs & rargs
        columns = {m for m in matching if isinstance(m, exp.Column)}

        if matching and columns:
            try:
                l = first(largs - columns)
                r = first(rargs - columns)
            except StopIteration:
                return expression

            # make sure the comparison is always of the form x > 1 instead of 1 < x
            if left.__class__ in INVERSE_COMPARISONS and l == ll:
                left = INVERSE_COMPARISONS[left.__class__](this=lr, expression=ll)
            if right.__class__ in INVERSE_COMPARISONS and r == rl:
                right = INVERSE_COMPARISONS[right.__class__](this=rr, expression=rl)

            if l.is_number and r.is_number:
                l = float(l.name)
                r = float(r.name)
            elif l.is_string and r.is_string:
                l = l.name
                r = r.name
            else:
                return None

            for (a, av), (b, bv) in itertools.permutations(((left, l), (right, r))):
                if isinstance(a, LT_LTE) and isinstance(b, LT_LTE):
                    return left if (av > bv if or_ else av <= bv) else right
                if isinstance(a, GT_GTE) and isinstance(b, GT_GTE):
                    return left if (av < bv if or_ else av >= bv) else right

                # we can't ever shortcut to true because the column could be null
                if isinstance(a, exp.LT) and isinstance(b, GT_GTE):
                    if not or_ and av <= bv:
                        return exp.false()
                elif isinstance(a, exp.GT) and isinstance(b, LT_LTE):
                    if not or_ and av >= bv:
                        return exp.false()
                elif isinstance(a, exp.EQ):
                    if isinstance(b, exp.LT):
                        return exp.false() if av >= bv else a
                    if isinstance(b, exp.LTE):
                        return exp.false() if av > bv else a
                    if isinstance(b, exp.GT):
                        return exp.false() if av <= bv else a
                    if isinstance(b, exp.GTE):
                        return exp.false() if av < bv else a
                    if isinstance(b, exp.NEQ):
                        return exp.false() if av == bv else a
    return None


def remove_compliments(expression):
    """
    Removing compliments.

    A AND NOT A -> FALSE
    A OR NOT A -> TRUE
    """
    if isinstance(expression, exp.Connector):
        compliment = exp.false() if isinstance(expression, exp.And) else exp.true()

        for a, b in itertools.permutations(expression.flatten(), 2):
            if is_complement(a, b):
                return compliment
    return expression


def uniq_sort(expression):
    """
    Uniq and sort a connector.

    C AND A AND B AND B -> A AND B AND C
    """
    if isinstance(expression, exp.Connector):
        result_func = exp.and_ if isinstance(expression, exp.And) else exp.or_
        flattened = tuple(expression.flatten())
        deduped = {GENERATOR.generate(e): e for e in flattened}
        arr = tuple(deduped.items())

        # check if the operands are already sorted, if not sort them
        # A AND C AND B -> A AND B AND C
        for i, (sql, e) in enumerate(arr[1:]):
            if sql < arr[i][0]:
                expression = result_func(*(deduped[sql] for sql in sorted(deduped)))
                break
        else:
            # we didn't have to sort but maybe we need to dedup
            if len(deduped) < len(flattened):
                expression = result_func(*deduped.values())

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
    if isinstance(expression, exp.Connector):
        kind = exp.Or if isinstance(expression, exp.And) else exp.And

        for a, b in itertools.permutations(expression.flatten(), 2):
            if isinstance(a, kind):
                aa, ab = a.unnest_operands()

                # absorb
                if is_complement(b, aa):
                    aa.replace(exp.true() if kind == exp.And else exp.false())
                elif is_complement(b, ab):
                    ab.replace(exp.true() if kind == exp.And else exp.false())
                elif (set(b.flatten()) if isinstance(b, kind) else {b}) < set(a.flatten()):
                    a.replace(exp.false() if kind == exp.And else exp.true())
                elif isinstance(b, kind):
                    # eliminate
                    rhs = b.unnest_operands()
                    ba, bb = rhs

                    if aa in rhs and (is_complement(ab, ba) or is_complement(ab, bb)):
                        a.replace(aa)
                        b.replace(aa)
                    elif ab in rhs and (is_complement(aa, ba) or is_complement(aa, bb)):
                        a.replace(ab)
                        b.replace(ab)

    return expression


def simplify_literals(expression):
    if isinstance(expression, exp.Binary):
        return _flat_simplify(expression, _simplify_binary)
    elif isinstance(expression, exp.Neg):
        this = expression.this
        if this.is_number:
            value = this.name
            if value[0] == "-":
                return exp.Literal.number(value[1:])
            return exp.Literal.number(f"-{value}")

    return expression


def _simplify_binary(expression, a, b):
    if isinstance(expression, exp.Is):
        if isinstance(b, exp.Not):
            c = b.this
            not_ = True
        else:
            c = b
            not_ = False

        if c == NULL:
            if isinstance(a, exp.Literal):
                return exp.true() if not_ else exp.false()
            if a == NULL:
                return exp.false() if not_ else exp.true()
    elif isinstance(expression, (exp.NullSafeEQ, exp.NullSafeNEQ)):
        return None
    elif NULL in (a, b):
        return exp.null()

    if a.is_number and b.is_number:
        a = int(a.name) if a.is_int else Decimal(a.name)
        b = int(b.name) if b.is_int else Decimal(b.name)

        if isinstance(expression, exp.Add):
            return exp.Literal.number(a + b)
        if isinstance(expression, exp.Sub):
            return exp.Literal.number(a - b)
        if isinstance(expression, exp.Mul):
            return exp.Literal.number(a * b)
        if isinstance(expression, exp.Div):
            if isinstance(a, int) and isinstance(b, int):
                return exp.Literal.number(a // b)
            return exp.Literal.number(a / b)

        boolean = eval_boolean(expression, a, b)

        if boolean:
            return boolean
    elif a.is_string and b.is_string:
        boolean = eval_boolean(expression, a, b)

        if boolean:
            return boolean
    elif isinstance(a, exp.Cast) and isinstance(b, exp.Interval):
        a, b = extract_date(a), extract_interval(b)
        if a and b:
            if isinstance(expression, exp.Add):
                return date_literal(a + b)
            if isinstance(expression, exp.Sub):
                return date_literal(a - b)
    elif isinstance(a, exp.Interval) and isinstance(b, exp.Cast):
        a, b = extract_interval(a), extract_date(b)
        # you cannot subtract a date from an interval
        if a and b and isinstance(expression, exp.Add):
            return date_literal(a + b)

    return None


def simplify_parens(expression):
    if (
        isinstance(expression, exp.Paren)
        and not isinstance(expression.this, exp.Select)
        and (
            not isinstance(expression.parent, (exp.Condition, exp.Binary))
            or isinstance(expression.this, (exp.Is, exp.Like))
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


def is_complement(a, b):
    return isinstance(b, exp.Not) and b.this == a


def eval_boolean(expression, a, b):
    if isinstance(expression, (exp.EQ, exp.Is)):
        return boolean_literal(a == b)
    if isinstance(expression, exp.NEQ):
        return boolean_literal(a != b)
    if isinstance(expression, exp.GT):
        return boolean_literal(a > b)
    if isinstance(expression, exp.GTE):
        return boolean_literal(a >= b)
    if isinstance(expression, exp.LT):
        return boolean_literal(a < b)
    if isinstance(expression, exp.LTE):
        return boolean_literal(a <= b)
    return None


def extract_date(cast):
    # The "fromisoformat" conversion could fail if the cast is used on an identifier,
    # so in that case we can't extract the date.
    try:
        if cast.args["to"].this == exp.DataType.Type.DATE:
            return datetime.date.fromisoformat(cast.name)
        if cast.args["to"].this == exp.DataType.Type.DATETIME:
            return datetime.datetime.fromisoformat(cast.name)
    except ValueError:
        return None


def extract_interval(interval):
    try:
        from dateutil.relativedelta import relativedelta  # type: ignore
    except ModuleNotFoundError:
        return None

    n = int(interval.name)
    unit = interval.text("unit").lower()

    if unit == "year":
        return relativedelta(years=n)
    if unit == "month":
        return relativedelta(months=n)
    if unit == "week":
        return relativedelta(weeks=n)
    if unit == "day":
        return relativedelta(days=n)
    return None


def date_literal(date):
    expr_type = exp.DataType.build("DATETIME" if isinstance(date, datetime.datetime) else "DATE")
    return exp.Cast(this=exp.Literal.string(date), to=expr_type)


def boolean_literal(condition):
    return exp.true() if condition else exp.false()


def _flat_simplify(expression, simplifier):
    operands = []
    queue = deque(expression.flatten(unnest=False))
    size = len(queue)

    while queue:
        a = queue.popleft()

        for b in queue:
            result = simplifier(expression, a, b)

            if result:
                queue.remove(b)
                queue.append(result)
                break
        else:
            operands.append(a)

    if len(operands) < size:
        return functools.reduce(lambda a, b: expression.__class__(this=a, expression=b), operands)
    return expression
