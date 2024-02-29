from __future__ import annotations

import datetime
import functools
import itertools
import typing as t
from collections import deque
from decimal import Decimal

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.helper import first, is_iterable, merge_ranges, while_changing
from sqlglot.optimizer.scope import find_all_in_scope, walk_in_scope

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    DateTruncBinaryTransform = t.Callable[
        [exp.Expression, datetime.date, str, Dialect], t.Optional[exp.Expression]
    ]

# Final means that an expression should not be simplified
FINAL = "final"


class UnsupportedUnit(Exception):
    pass


def simplify(
    expression: exp.Expression, constant_propagation: bool = False, dialect: DialectType = None
):
    """
    Rewrite sqlglot AST to simplify expressions.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("TRUE AND TRUE")
        >>> simplify(expression).sql()
        'TRUE'

    Args:
        expression (sqlglot.Expression): expression to simplify
        constant_propagation: whether the constant propagation rule should be used

    Returns:
        sqlglot.Expression: simplified expression
    """

    dialect = Dialect.get_or_raise(dialect)

    def _simplify(expression, root=True):
        if expression.meta.get(FINAL):
            return expression

        # group by expressions cannot be simplified, for example
        # select x + 1 + 1 FROM y GROUP BY x + 1 + 1
        # the projection must exactly match the group by key
        group = expression.args.get("group")

        if group and hasattr(expression, "selects"):
            groups = set(group.expressions)
            group.meta[FINAL] = True

            for e in expression.selects:
                for node, *_ in e.walk():
                    if node in groups:
                        e.meta[FINAL] = True
                        break

            having = expression.args.get("having")
            if having:
                for node, *_ in having.walk():
                    if node in groups:
                        having.meta[FINAL] = True
                        break

        # Pre-order transformations
        node = expression
        node = rewrite_between(node)
        node = uniq_sort(node, root)
        node = absorb_and_eliminate(node, root)
        node = simplify_concat(node)
        node = simplify_conditionals(node)

        if constant_propagation:
            node = propagate_constants(node, root)

        exp.replace_children(node, lambda e: _simplify(e, False))

        # Post-order transformations
        node = simplify_not(node)
        node = flatten(node)
        node = simplify_connectors(node, root)
        node = remove_complements(node, root)
        node = simplify_coalesce(node)
        node.parent = expression.parent
        node = simplify_literals(node, root)
        node = simplify_equality(node)
        node = simplify_parens(node)
        node = simplify_datetrunc(node, dialect)
        node = sort_comparison(node)
        node = simplify_startswith(node)

        if root:
            expression.replace(node)
        return node

    expression = while_changing(expression, _simplify)
    remove_where_true(expression)
    return expression


def catch(*exceptions):
    """Decorator that ignores a simplification function if any of `exceptions` are raised"""

    def decorator(func):
        def wrapped(expression, *args, **kwargs):
            try:
                return func(expression, *args, **kwargs)
            except exceptions:
                return expression

        return wrapped

    return decorator


def rewrite_between(expression: exp.Expression) -> exp.Expression:
    """Rewrite x between y and z to x >= y AND x <= z.

    This is done because comparison simplification is only done on lt/lte/gt/gte.
    """
    if isinstance(expression, exp.Between):
        negate = isinstance(expression.parent, exp.Not)

        expression = exp.and_(
            exp.GTE(this=expression.this.copy(), expression=expression.args["low"]),
            exp.LTE(this=expression.this.copy(), expression=expression.args["high"]),
            copy=False,
        )

        if negate:
            expression = exp.paren(expression, copy=False)

    return expression


COMPLEMENT_COMPARISONS = {
    exp.LT: exp.GTE,
    exp.GT: exp.LTE,
    exp.LTE: exp.GT,
    exp.GTE: exp.LT,
    exp.EQ: exp.NEQ,
    exp.NEQ: exp.EQ,
}


def simplify_not(expression):
    """
    Demorgan's Law
    NOT (x OR y) -> NOT x AND NOT y
    NOT (x AND y) -> NOT x OR NOT y
    """
    if isinstance(expression, exp.Not):
        this = expression.this
        if is_null(this):
            return exp.null()
        if this.__class__ in COMPLEMENT_COMPARISONS:
            return COMPLEMENT_COMPARISONS[this.__class__](
                this=this.this, expression=this.expression
            )
        if isinstance(this, exp.Paren):
            condition = this.unnest()
            if isinstance(condition, exp.And):
                return exp.paren(
                    exp.or_(
                        exp.not_(condition.left, copy=False),
                        exp.not_(condition.right, copy=False),
                        copy=False,
                    )
                )
            if isinstance(condition, exp.Or):
                return exp.paren(
                    exp.and_(
                        exp.not_(condition.left, copy=False),
                        exp.not_(condition.right, copy=False),
                        copy=False,
                    )
                )
            if is_null(condition):
                return exp.null()
        if always_true(this):
            return exp.false()
        if is_false(this):
            return exp.true()
        if isinstance(this, exp.Not):
            # double negation
            # NOT NOT x -> x
            return this.this
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


def simplify_connectors(expression, root=True):
    def _simplify_connectors(expression, left, right):
        if left == right:
            return left
        if isinstance(expression, exp.And):
            if is_false(left) or is_false(right):
                return exp.false()
            if is_null(left) or is_null(right):
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
            if is_false(left) and is_false(right):
                return exp.false()
            if (
                (is_null(left) and is_null(right))
                or (is_null(left) and is_false(right))
                or (is_false(left) and is_null(right))
            ):
                return exp.null()
            if is_false(left):
                return right
            if is_false(right):
                return left
            return _simplify_comparison(expression, left, right, or_=True)

    if isinstance(expression, exp.Connector):
        return _flat_simplify(expression, _simplify_connectors, root)
    return expression


LT_LTE = (exp.LT, exp.LTE)
GT_GTE = (exp.GT, exp.GTE)

COMPARISONS = (
    *LT_LTE,
    *GT_GTE,
    exp.EQ,
    exp.NEQ,
    exp.Is,
)

INVERSE_COMPARISONS: t.Dict[t.Type[exp.Expression], t.Type[exp.Expression]] = {
    exp.LT: exp.GT,
    exp.GT: exp.LT,
    exp.LTE: exp.GTE,
    exp.GTE: exp.LTE,
}

NONDETERMINISTIC = (exp.Rand, exp.Randn)


def _simplify_comparison(expression, left, right, or_=False):
    if isinstance(left, COMPARISONS) and isinstance(right, COMPARISONS):
        ll, lr = left.args.values()
        rl, rr = right.args.values()

        largs = {ll, lr}
        rargs = {rl, rr}

        matching = largs & rargs
        columns = {m for m in matching if not _is_constant(m) and not m.find(*NONDETERMINISTIC)}

        if matching and columns:
            try:
                l = first(largs - columns)
                r = first(rargs - columns)
            except StopIteration:
                return expression

            if l.is_number and r.is_number:
                l = float(l.name)
                r = float(r.name)
            elif l.is_string and r.is_string:
                l = l.name
                r = r.name
            else:
                l = extract_date(l)
                if not l:
                    return None
                r = extract_date(r)
                if not r:
                    return None

            for (a, av), (b, bv) in itertools.permutations(((left, l), (right, r))):
                if isinstance(a, LT_LTE) and isinstance(b, LT_LTE):
                    return left if (av > bv if or_ else av <= bv) else right
                if isinstance(a, GT_GTE) and isinstance(b, GT_GTE):
                    return left if (av < bv if or_ else av >= bv) else right

                # we can't ever shortcut to true because the column could be null
                if not or_:
                    if isinstance(a, exp.LT) and isinstance(b, GT_GTE):
                        if av <= bv:
                            return exp.false()
                    elif isinstance(a, exp.GT) and isinstance(b, LT_LTE):
                        if av >= bv:
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


def remove_complements(expression, root=True):
    """
    Removing complements.

    A AND NOT A -> FALSE
    A OR NOT A -> TRUE
    """
    if isinstance(expression, exp.Connector) and (root or not expression.same_parent):
        complement = exp.false() if isinstance(expression, exp.And) else exp.true()

        for a, b in itertools.permutations(expression.flatten(), 2):
            if is_complement(a, b):
                return complement
    return expression


def uniq_sort(expression, root=True):
    """
    Uniq and sort a connector.

    C AND A AND B AND B -> A AND B AND C
    """
    if isinstance(expression, exp.Connector) and (root or not expression.same_parent):
        result_func = exp.and_ if isinstance(expression, exp.And) else exp.or_
        flattened = tuple(expression.flatten())
        deduped = {gen(e): e for e in flattened}
        arr = tuple(deduped.items())

        # check if the operands are already sorted, if not sort them
        # A AND C AND B -> A AND B AND C
        for i, (sql, e) in enumerate(arr[1:]):
            if sql < arr[i][0]:
                expression = result_func(*(e for _, e in sorted(arr)), copy=False)
                break
        else:
            # we didn't have to sort but maybe we need to dedup
            if len(deduped) < len(flattened):
                expression = result_func(*deduped.values(), copy=False)

    return expression


def absorb_and_eliminate(expression, root=True):
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
    if isinstance(expression, exp.Connector) and (root or not expression.same_parent):
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


def propagate_constants(expression, root=True):
    """
    Propagate constants for conjunctions in DNF:

    SELECT * FROM t WHERE a = b AND b = 5 becomes
    SELECT * FROM t WHERE a = 5 AND b = 5

    Reference: https://www.sqlite.org/optoverview.html
    """

    if (
        isinstance(expression, exp.And)
        and (root or not expression.same_parent)
        and sqlglot.optimizer.normalize.normalized(expression, dnf=True)
    ):
        constant_mapping = {}
        for expr, *_ in walk_in_scope(expression, prune=lambda node, *_: isinstance(node, exp.If)):
            if isinstance(expr, exp.EQ):
                l, r = expr.left, expr.right

                # TODO: create a helper that can be used to detect nested literal expressions such
                # as CAST(123456 AS BIGINT), since we usually want to treat those as literals too
                if isinstance(l, exp.Column) and isinstance(r, exp.Literal):
                    constant_mapping[l] = (id(l), r)

        if constant_mapping:
            for column in find_all_in_scope(expression, exp.Column):
                parent = column.parent
                column_id, constant = constant_mapping.get(column) or (None, None)
                if (
                    column_id is not None
                    and id(column) != column_id
                    and not (isinstance(parent, exp.Is) and isinstance(parent.expression, exp.Null))
                ):
                    column.replace(constant.copy())

    return expression


INVERSE_DATE_OPS: t.Dict[t.Type[exp.Expression], t.Type[exp.Expression]] = {
    exp.DateAdd: exp.Sub,
    exp.DateSub: exp.Add,
    exp.DatetimeAdd: exp.Sub,
    exp.DatetimeSub: exp.Add,
}

INVERSE_OPS: t.Dict[t.Type[exp.Expression], t.Type[exp.Expression]] = {
    **INVERSE_DATE_OPS,
    exp.Add: exp.Sub,
    exp.Sub: exp.Add,
}


def _is_number(expression: exp.Expression) -> bool:
    return expression.is_number


def _is_interval(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.Interval) and extract_interval(expression) is not None


@catch(ModuleNotFoundError, UnsupportedUnit)
def simplify_equality(expression: exp.Expression) -> exp.Expression:
    """
    Use the subtraction and addition properties of equality to simplify expressions:

        x + 1 = 3 becomes x = 2

    There are two binary operations in the above expression: + and =
    Here's how we reference all the operands in the code below:

          l     r
        x + 1 = 3
        a   b
    """
    if isinstance(expression, COMPARISONS):
        l, r = expression.left, expression.right

        if l.__class__ not in INVERSE_OPS:
            return expression

        if r.is_number:
            a_predicate = _is_number
            b_predicate = _is_number
        elif _is_date_literal(r):
            a_predicate = _is_date_literal
            b_predicate = _is_interval
        else:
            return expression

        if l.__class__ in INVERSE_DATE_OPS:
            l = t.cast(exp.IntervalOp, l)
            a = l.this
            b = l.interval()
        else:
            l = t.cast(exp.Binary, l)
            a, b = l.left, l.right

        if not a_predicate(a) and b_predicate(b):
            pass
        elif not a_predicate(b) and b_predicate(a):
            a, b = b, a
        else:
            return expression

        return expression.__class__(
            this=a, expression=INVERSE_OPS[l.__class__](this=r, expression=b)
        )
    return expression


def simplify_literals(expression, root=True):
    if isinstance(expression, exp.Binary) and not isinstance(expression, exp.Connector):
        return _flat_simplify(expression, _simplify_binary, root)

    if isinstance(expression, exp.Neg):
        this = expression.this
        if this.is_number:
            value = this.name
            if value[0] == "-":
                return exp.Literal.number(value[1:])
            return exp.Literal.number(f"-{value}")

    if type(expression) in INVERSE_DATE_OPS:
        return _simplify_binary(expression, expression.this, expression.interval()) or expression

    return expression


def _simplify_binary(expression, a, b):
    if isinstance(expression, exp.Is):
        if isinstance(b, exp.Not):
            c = b.this
            not_ = True
        else:
            c = b
            not_ = False

        if is_null(c):
            if isinstance(a, exp.Literal):
                return exp.true() if not_ else exp.false()
            if is_null(a):
                return exp.false() if not_ else exp.true()
    elif isinstance(expression, (exp.NullSafeEQ, exp.NullSafeNEQ)):
        return None
    elif is_null(a) or is_null(b):
        return exp.null()

    if a.is_number and b.is_number:
        num_a = int(a.name) if a.is_int else Decimal(a.name)
        num_b = int(b.name) if b.is_int else Decimal(b.name)

        if isinstance(expression, exp.Add):
            return exp.Literal.number(num_a + num_b)
        if isinstance(expression, exp.Mul):
            return exp.Literal.number(num_a * num_b)

        # We only simplify Sub, Div if a and b have the same parent because they're not associative
        if isinstance(expression, exp.Sub):
            return exp.Literal.number(num_a - num_b) if a.parent is b.parent else None
        if isinstance(expression, exp.Div):
            # engines have differing int div behavior so intdiv is not safe
            if (isinstance(num_a, int) and isinstance(num_b, int)) or a.parent is not b.parent:
                return None
            return exp.Literal.number(num_a / num_b)

        boolean = eval_boolean(expression, num_a, num_b)

        if boolean:
            return boolean
    elif a.is_string and b.is_string:
        boolean = eval_boolean(expression, a.this, b.this)

        if boolean:
            return boolean
    elif _is_date_literal(a) and isinstance(b, exp.Interval):
        a, b = extract_date(a), extract_interval(b)
        if a and b:
            if isinstance(expression, (exp.Add, exp.DateAdd, exp.DatetimeAdd)):
                return date_literal(a + b)
            if isinstance(expression, (exp.Sub, exp.DateSub, exp.DatetimeSub)):
                return date_literal(a - b)
    elif isinstance(a, exp.Interval) and _is_date_literal(b):
        a, b = extract_interval(a), extract_date(b)
        # you cannot subtract a date from an interval
        if a and b and isinstance(expression, exp.Add):
            return date_literal(a + b)
    elif _is_date_literal(a) and _is_date_literal(b):
        if isinstance(expression, exp.Predicate):
            a, b = extract_date(a), extract_date(b)
            boolean = eval_boolean(expression, a, b)
            if boolean:
                return boolean

    return None


def simplify_parens(expression):
    if not isinstance(expression, exp.Paren):
        return expression

    this = expression.this
    parent = expression.parent

    if not isinstance(this, exp.Select) and (
        not isinstance(parent, (exp.Condition, exp.Binary))
        or isinstance(parent, exp.Paren)
        or not isinstance(this, exp.Binary)
        or (isinstance(this, exp.Predicate) and not isinstance(parent, exp.Predicate))
        or (isinstance(this, exp.Add) and isinstance(parent, exp.Add))
        or (isinstance(this, exp.Mul) and isinstance(parent, exp.Mul))
        or (isinstance(this, exp.Mul) and isinstance(parent, (exp.Add, exp.Sub)))
    ):
        return this
    return expression


NONNULL_CONSTANTS = (
    exp.Literal,
    exp.Boolean,
)

CONSTANTS = (
    exp.Literal,
    exp.Boolean,
    exp.Null,
)


def _is_nonnull_constant(expression: exp.Expression) -> bool:
    return isinstance(expression, NONNULL_CONSTANTS) or _is_date_literal(expression)


def _is_constant(expression: exp.Expression) -> bool:
    return isinstance(expression, CONSTANTS) or _is_date_literal(expression)


def simplify_coalesce(expression):
    # COALESCE(x) -> x
    if (
        isinstance(expression, exp.Coalesce)
        and (not expression.expressions or _is_nonnull_constant(expression.this))
        # COALESCE is also used as a Spark partitioning hint
        and not isinstance(expression.parent, exp.Hint)
    ):
        return expression.this

    if not isinstance(expression, COMPARISONS):
        return expression

    if isinstance(expression.left, exp.Coalesce):
        coalesce = expression.left
        other = expression.right
    elif isinstance(expression.right, exp.Coalesce):
        coalesce = expression.right
        other = expression.left
    else:
        return expression

    # This transformation is valid for non-constants,
    # but it really only does anything if they are both constants.
    if not _is_constant(other):
        return expression

    # Find the first constant arg
    for arg_index, arg in enumerate(coalesce.expressions):
        if _is_constant(arg):
            break
    else:
        return expression

    coalesce.set("expressions", coalesce.expressions[:arg_index])

    # Remove the COALESCE function. This is an optimization, skipping a simplify iteration,
    # since we already remove COALESCE at the top of this function.
    coalesce = coalesce if coalesce.expressions else coalesce.this

    # This expression is more complex than when we started, but it will get simplified further
    return exp.paren(
        exp.or_(
            exp.and_(
                coalesce.is_(exp.null()).not_(copy=False),
                expression.copy(),
                copy=False,
            ),
            exp.and_(
                coalesce.is_(exp.null()),
                type(expression)(this=arg.copy(), expression=other.copy()),
                copy=False,
            ),
            copy=False,
        )
    )


CONCATS = (exp.Concat, exp.DPipe)


def simplify_concat(expression):
    """Reduces all groups that contain string literals by concatenating them."""
    if not isinstance(expression, CONCATS) or (
        # We can't reduce a CONCAT_WS call if we don't statically know the separator
        isinstance(expression, exp.ConcatWs) and not expression.expressions[0].is_string
    ):
        return expression

    if isinstance(expression, exp.ConcatWs):
        sep_expr, *expressions = expression.expressions
        sep = sep_expr.name
        concat_type = exp.ConcatWs
        args = {}
    else:
        expressions = expression.expressions
        sep = ""
        concat_type = exp.Concat
        args = {
            "safe": expression.args.get("safe"),
            "coalesce": expression.args.get("coalesce"),
        }

    new_args = []
    for is_string_group, group in itertools.groupby(
        expressions or expression.flatten(), lambda e: e.is_string
    ):
        if is_string_group:
            new_args.append(exp.Literal.string(sep.join(string.name for string in group)))
        else:
            new_args.extend(group)

    if len(new_args) == 1 and new_args[0].is_string:
        return new_args[0]

    if concat_type is exp.ConcatWs:
        new_args = [sep_expr] + new_args

    return concat_type(expressions=new_args, **args)


def simplify_conditionals(expression):
    """Simplifies expressions like IF, CASE if their condition is statically known."""
    if isinstance(expression, exp.Case):
        this = expression.this
        for case in expression.args["ifs"]:
            cond = case.this
            if this:
                # Convert CASE x WHEN matching_value ... to CASE WHEN x = matching_value ...
                cond = cond.replace(this.pop().eq(cond))

            if always_true(cond):
                return case.args["true"]

            if always_false(cond):
                case.pop()
                if not expression.args["ifs"]:
                    return expression.args.get("default") or exp.null()
    elif isinstance(expression, exp.If) and not isinstance(expression.parent, exp.Case):
        if always_true(expression.this):
            return expression.args["true"]
        if always_false(expression.this):
            return expression.args.get("false") or exp.null()

    return expression


def simplify_startswith(expression: exp.Expression) -> exp.Expression:
    """
    Reduces a prefix check to either TRUE or FALSE if both the string and the
    prefix are statically known.

    Example:
        >>> from sqlglot import parse_one
        >>> simplify_startswith(parse_one("STARTSWITH('foo', 'f')")).sql()
        'TRUE'
    """
    if (
        isinstance(expression, exp.StartsWith)
        and expression.this.is_string
        and expression.expression.is_string
    ):
        return exp.convert(expression.name.startswith(expression.expression.name))

    return expression


DateRange = t.Tuple[datetime.date, datetime.date]


def _datetrunc_range(date: datetime.date, unit: str, dialect: Dialect) -> t.Optional[DateRange]:
    """
    Get the date range for a DATE_TRUNC equality comparison:

    Example:
        _datetrunc_range(date(2021-01-01), 'year') == (date(2021-01-01), date(2022-01-01))
    Returns:
        tuple of [min, max) or None if a value can never be equal to `date` for `unit`
    """
    floor = date_floor(date, unit, dialect)

    if date != floor:
        # This will always be False, except for NULL values.
        return None

    return floor, floor + interval(unit)


def _datetrunc_eq_expression(left: exp.Expression, drange: DateRange) -> exp.Expression:
    """Get the logical expression for a date range"""
    return exp.and_(
        left >= date_literal(drange[0]),
        left < date_literal(drange[1]),
        copy=False,
    )


def _datetrunc_eq(
    left: exp.Expression, date: datetime.date, unit: str, dialect: Dialect
) -> t.Optional[exp.Expression]:
    drange = _datetrunc_range(date, unit, dialect)
    if not drange:
        return None

    return _datetrunc_eq_expression(left, drange)


def _datetrunc_neq(
    left: exp.Expression, date: datetime.date, unit: str, dialect: Dialect
) -> t.Optional[exp.Expression]:
    drange = _datetrunc_range(date, unit, dialect)
    if not drange:
        return None

    return exp.and_(
        left < date_literal(drange[0]),
        left >= date_literal(drange[1]),
        copy=False,
    )


DATETRUNC_BINARY_COMPARISONS: t.Dict[t.Type[exp.Expression], DateTruncBinaryTransform] = {
    exp.LT: lambda l, dt, u, d: l
    < date_literal(dt if dt == date_floor(dt, u, d) else date_floor(dt, u, d) + interval(u)),
    exp.GT: lambda l, dt, u, d: l >= date_literal(date_floor(dt, u, d) + interval(u)),
    exp.LTE: lambda l, dt, u, d: l < date_literal(date_floor(dt, u, d) + interval(u)),
    exp.GTE: lambda l, dt, u, d: l >= date_literal(date_ceil(dt, u, d)),
    exp.EQ: _datetrunc_eq,
    exp.NEQ: _datetrunc_neq,
}
DATETRUNC_COMPARISONS = {exp.In, *DATETRUNC_BINARY_COMPARISONS}
DATETRUNCS = (exp.DateTrunc, exp.TimestampTrunc)


def _is_datetrunc_predicate(left: exp.Expression, right: exp.Expression) -> bool:
    return isinstance(left, DATETRUNCS) and _is_date_literal(right)


@catch(ModuleNotFoundError, UnsupportedUnit)
def simplify_datetrunc(expression: exp.Expression, dialect: Dialect) -> exp.Expression:
    """Simplify expressions like `DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE)`"""
    comparison = expression.__class__

    if isinstance(expression, DATETRUNCS):
        date = extract_date(expression.this)
        if date and expression.unit:
            return date_literal(date_floor(date, expression.unit.name.lower(), dialect))
    elif comparison not in DATETRUNC_COMPARISONS:
        return expression

    if isinstance(expression, exp.Binary):
        l, r = expression.left, expression.right

        if not _is_datetrunc_predicate(l, r):
            return expression

        l = t.cast(exp.DateTrunc, l)
        unit = l.unit.name.lower()
        date = extract_date(r)

        if not date:
            return expression

        return DATETRUNC_BINARY_COMPARISONS[comparison](l.this, date, unit, dialect) or expression
    elif isinstance(expression, exp.In):
        l = expression.this
        rs = expression.expressions

        if rs and all(_is_datetrunc_predicate(l, r) for r in rs):
            l = t.cast(exp.DateTrunc, l)
            unit = l.unit.name.lower()

            ranges = []
            for r in rs:
                date = extract_date(r)
                if not date:
                    return expression
                drange = _datetrunc_range(date, unit, dialect)
                if drange:
                    ranges.append(drange)

            if not ranges:
                return expression

            ranges = merge_ranges(ranges)

            return exp.or_(*[_datetrunc_eq_expression(l, drange) for drange in ranges], copy=False)

    return expression


def sort_comparison(expression: exp.Expression) -> exp.Expression:
    if expression.__class__ in COMPLEMENT_COMPARISONS:
        l, r = expression.this, expression.expression
        l_column = isinstance(l, exp.Column)
        r_column = isinstance(r, exp.Column)
        l_const = _is_constant(l)
        r_const = _is_constant(r)

        if (l_column and not r_column) or (r_const and not l_const):
            return expression
        if (r_column and not l_column) or (l_const and not r_const) or (gen(l) > gen(r)):
            return INVERSE_COMPARISONS.get(expression.__class__, expression.__class__)(
                this=r, expression=l
            )
    return expression


# CROSS joins result in an empty table if the right table is empty.
# So we can only simplify certain types of joins to CROSS.
# Or in other words, LEFT JOIN x ON TRUE != CROSS JOIN x
JOINS = {
    ("", ""),
    ("", "INNER"),
    ("RIGHT", ""),
    ("RIGHT", "OUTER"),
}


def remove_where_true(expression):
    for where in expression.find_all(exp.Where):
        if always_true(where.this):
            where.parent.set("where", None)
    for join in expression.find_all(exp.Join):
        if (
            always_true(join.args.get("on"))
            and not join.args.get("using")
            and not join.args.get("method")
            and (join.side, join.kind) in JOINS
        ):
            join.set("on", None)
            join.set("side", None)
            join.set("kind", "CROSS")


def always_true(expression):
    return (isinstance(expression, exp.Boolean) and expression.this) or isinstance(
        expression, exp.Literal
    )


def always_false(expression):
    return is_false(expression) or is_null(expression)


def is_complement(a, b):
    return isinstance(b, exp.Not) and b.this == a


def is_false(a: exp.Expression) -> bool:
    return type(a) is exp.Boolean and not a.this


def is_null(a: exp.Expression) -> bool:
    return type(a) is exp.Null


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


def cast_as_date(value: t.Any) -> t.Optional[datetime.date]:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.datetime.fromisoformat(value).date()
    except ValueError:
        return None


def cast_as_datetime(value: t.Any) -> t.Optional[datetime.datetime]:
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(year=value.year, month=value.month, day=value.day)
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None


def cast_value(value: t.Any, to: exp.DataType) -> t.Optional[t.Union[datetime.date, datetime.date]]:
    if not value:
        return None
    if to.is_type(exp.DataType.Type.DATE):
        return cast_as_date(value)
    if to.is_type(*exp.DataType.TEMPORAL_TYPES):
        return cast_as_datetime(value)
    return None


def extract_date(cast: exp.Expression) -> t.Optional[t.Union[datetime.date, datetime.date]]:
    if isinstance(cast, exp.Cast):
        to = cast.to
    elif isinstance(cast, exp.TsOrDsToDate) and not cast.args.get("format"):
        to = exp.DataType.build(exp.DataType.Type.DATE)
    else:
        return None

    if isinstance(cast.this, exp.Literal):
        value: t.Any = cast.this.name
    elif isinstance(cast.this, (exp.Cast, exp.TsOrDsToDate)):
        value = extract_date(cast.this)
    else:
        return None
    return cast_value(value, to)


def _is_date_literal(expression: exp.Expression) -> bool:
    return extract_date(expression) is not None


def extract_interval(expression):
    try:
        n = int(expression.name)
        unit = expression.text("unit").lower()
        return interval(unit, n)
    except (UnsupportedUnit, ModuleNotFoundError, ValueError):
        return None


def date_literal(date):
    return exp.cast(
        exp.Literal.string(date),
        (
            exp.DataType.Type.DATETIME
            if isinstance(date, datetime.datetime)
            else exp.DataType.Type.DATE
        ),
    )


def interval(unit: str, n: int = 1):
    from dateutil.relativedelta import relativedelta

    if unit == "year":
        return relativedelta(years=1 * n)
    if unit == "quarter":
        return relativedelta(months=3 * n)
    if unit == "month":
        return relativedelta(months=1 * n)
    if unit == "week":
        return relativedelta(weeks=1 * n)
    if unit == "day":
        return relativedelta(days=1 * n)
    if unit == "hour":
        return relativedelta(hours=1 * n)
    if unit == "minute":
        return relativedelta(minutes=1 * n)
    if unit == "second":
        return relativedelta(seconds=1 * n)

    raise UnsupportedUnit(f"Unsupported unit: {unit}")


def date_floor(d: datetime.date, unit: str, dialect: Dialect) -> datetime.date:
    if unit == "year":
        return d.replace(month=1, day=1)
    if unit == "quarter":
        if d.month <= 3:
            return d.replace(month=1, day=1)
        elif d.month <= 6:
            return d.replace(month=4, day=1)
        elif d.month <= 9:
            return d.replace(month=7, day=1)
        else:
            return d.replace(month=10, day=1)
    if unit == "month":
        return d.replace(month=d.month, day=1)
    if unit == "week":
        # Assuming week starts on Monday (0) and ends on Sunday (6)
        return d - datetime.timedelta(days=d.weekday() - dialect.WEEK_OFFSET)
    if unit == "day":
        return d

    raise UnsupportedUnit(f"Unsupported unit: {unit}")


def date_ceil(d: datetime.date, unit: str, dialect: Dialect) -> datetime.date:
    floor = date_floor(d, unit, dialect)

    if floor == d:
        return d

    return floor + interval(unit)


def boolean_literal(condition):
    return exp.true() if condition else exp.false()


def _flat_simplify(expression, simplifier, root=True):
    if root or not expression.same_parent:
        operands = []
        queue = deque(expression.flatten(unnest=False))
        size = len(queue)

        while queue:
            a = queue.popleft()

            for b in queue:
                result = simplifier(expression, a, b)

                if result and result is not expression:
                    queue.remove(b)
                    queue.appendleft(result)
                    break
            else:
                operands.append(a)

        if len(operands) < size:
            return functools.reduce(
                lambda a, b: expression.__class__(this=a, expression=b), operands
            )
    return expression


def gen(expression: t.Any) -> str:
    """Simple pseudo sql generator for quickly generating sortable and uniq strings.

    Sorting and deduping sql is a necessary step for optimization. Calling the actual
    generator is expensive so we have a bare minimum sql generator here.
    """
    if expression is None:
        return "_"
    if is_iterable(expression):
        return ",".join(gen(e) for e in expression)
    if not isinstance(expression, exp.Expression):
        return str(expression)

    etype = type(expression)
    if etype in GEN_MAP:
        return GEN_MAP[etype](expression)
    return f"{expression.key} {gen(expression.args.values())}"


GEN_MAP = {
    exp.Add: lambda e: _binary(e, "+"),
    exp.And: lambda e: _binary(e, "AND"),
    exp.Anonymous: lambda e: _anonymous(e),
    exp.Between: lambda e: f"{gen(e.this)} BETWEEN {gen(e.args.get('low'))} AND {gen(e.args.get('high'))}",
    exp.Boolean: lambda e: "TRUE" if e.this else "FALSE",
    exp.Bracket: lambda e: f"{gen(e.this)}[{gen(e.expressions)}]",
    exp.Column: lambda e: ".".join(gen(p) for p in e.parts),
    exp.DataType: lambda e: f"{e.this.name} {gen(tuple(e.args.values())[1:])}",
    exp.Div: lambda e: _binary(e, "/"),
    exp.Dot: lambda e: _binary(e, "."),
    exp.EQ: lambda e: _binary(e, "="),
    exp.GT: lambda e: _binary(e, ">"),
    exp.GTE: lambda e: _binary(e, ">="),
    exp.Identifier: lambda e: f'"{e.name}"' if e.quoted else e.name,
    exp.ILike: lambda e: _binary(e, "ILIKE"),
    exp.In: lambda e: f"{gen(e.this)} IN ({gen(tuple(e.args.values())[1:])})",
    exp.Is: lambda e: _binary(e, "IS"),
    exp.Like: lambda e: _binary(e, "LIKE"),
    exp.Literal: lambda e: f"'{e.name}'" if e.is_string else e.name,
    exp.LT: lambda e: _binary(e, "<"),
    exp.LTE: lambda e: _binary(e, "<="),
    exp.Mod: lambda e: _binary(e, "%"),
    exp.Mul: lambda e: _binary(e, "*"),
    exp.Neg: lambda e: _unary(e, "-"),
    exp.NEQ: lambda e: _binary(e, "<>"),
    exp.Not: lambda e: _unary(e, "NOT"),
    exp.Null: lambda e: "NULL",
    exp.Or: lambda e: _binary(e, "OR"),
    exp.Paren: lambda e: f"({gen(e.this)})",
    exp.Sub: lambda e: _binary(e, "-"),
    exp.Subquery: lambda e: f"({gen(e.args.values())})",
    exp.Table: lambda e: gen(e.args.values()),
    exp.Var: lambda e: e.name,
}


def _anonymous(e: exp.Anonymous) -> str:
    this = e.this
    if isinstance(this, str):
        name = this.upper()
    elif isinstance(this, exp.Identifier):
        name = f'"{this.name}"' if this.quoted else this.name.upper()
    else:
        raise ValueError(
            f"Anonymous.this expects a str or an Identifier, got '{this.__class__.__name__}'."
        )

    return f"{name} {','.join(gen(e) for e in e.expressions)}"


def _binary(e: exp.Binary, op: str) -> str:
    return f"{gen(e.left)} {op} {gen(e.right)}"


def _unary(e: exp.Unary, op: str) -> str:
    return f"{op} {gen(e.this)}"
