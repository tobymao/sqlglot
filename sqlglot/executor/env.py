import datetime
import inspect
import re
import statistics
from functools import wraps

from sqlglot import exp
from sqlglot.helper import PYTHON_VERSION


class reverse_key:
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        return other.obj == self.obj

    def __lt__(self, other):
        return other.obj < self.obj


def filter_nulls(func, empty_null=True):
    @wraps(func)
    def _func(values):
        filtered = tuple(v for v in values if v is not None)
        if not filtered and empty_null:
            return None
        return func(filtered)

    return _func


def null_if_any(*required):
    """
    Decorator that makes a function return `None` if any of the `required` arguments are `None`.

    This also supports decoration with no arguments, e.g.:

        @null_if_any
        def foo(a, b): ...

    In which case all arguments are required.
    """
    f = None
    if len(required) == 1 and callable(required[0]):
        f = required[0]
        required = ()

    def decorator(func):
        if required:
            required_indices = [
                i for i, param in enumerate(inspect.signature(func).parameters) if param in required
            ]

            def predicate(*args):
                return any(args[i] is None for i in required_indices)

        else:

            def predicate(*args):
                return any(a is None for a in args)

        @wraps(func)
        def _func(*args):
            if predicate(*args):
                return None
            return func(*args)

        return _func

    if f:
        return decorator(f)

    return decorator


@null_if_any("substr", "this")
def str_position(substr, this, position=None):
    position = position - 1 if position is not None else position
    return this.find(substr, position) + 1


@null_if_any("this")
def substring(this, start=None, length=None):
    if start is None:
        return this
    elif start == 0:
        return ""
    elif start < 0:
        start = len(this) + start
    else:
        start -= 1

    end = None if length is None else start + length

    return this[start:end]


@null_if_any
def cast(this, to):
    if to == exp.DataType.Type.DATE:
        return datetime.date.fromisoformat(this)
    if to == exp.DataType.Type.DATETIME:
        return datetime.datetime.fromisoformat(this)
    if to in exp.DataType.TEXT_TYPES:
        return str(this)
    if to in {exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE}:
        return float(this)
    if to in exp.DataType.NUMERIC_TYPES:
        return int(this)
    raise NotImplementedError(f"Casting to '{to}' not implemented.")


def ordered(this, desc, nulls_first):
    if desc:
        return reverse_key(this)
    return this


@null_if_any
def interval(this, unit):
    if unit == "DAY":
        return datetime.timedelta(days=float(this))
    raise NotImplementedError


ENV = {
    "exp": exp,
    # aggs
    "ARRAYAGG": list,
    "AVG": filter_nulls(statistics.fmean if PYTHON_VERSION >= (3, 8) else statistics.mean),  # type: ignore
    "COUNT": filter_nulls(lambda acc: sum(1 for _ in acc), False),
    "MAX": filter_nulls(max),
    "MIN": filter_nulls(min),
    "SUM": filter_nulls(sum),
    # scalar functions
    "ABS": null_if_any(lambda this: abs(this)),
    "ADD": null_if_any(lambda e, this: e + this),
    "ARRAYANY": null_if_any(lambda arr, func: any(func(e) for e in arr)),
    "BETWEEN": null_if_any(lambda this, low, high: low <= this and this <= high),
    "BITWISEAND": null_if_any(lambda this, e: this & e),
    "BITWISELEFTSHIFT": null_if_any(lambda this, e: this << e),
    "BITWISEOR": null_if_any(lambda this, e: this | e),
    "BITWISERIGHTSHIFT": null_if_any(lambda this, e: this >> e),
    "BITWISEXOR": null_if_any(lambda this, e: this ^ e),
    "CAST": cast,
    "COALESCE": lambda *args: next((a for a in args if a is not None), None),
    "CONCAT": null_if_any(lambda *args: "".join(args)),
    "CONCATWS": null_if_any(lambda this, *args: this.join(args)),
    "DIV": null_if_any(lambda e, this: e / this),
    "EQ": null_if_any(lambda this, e: this == e),
    "EXTRACT": null_if_any(lambda this, e: getattr(e, this)),
    "GT": null_if_any(lambda this, e: this > e),
    "GTE": null_if_any(lambda this, e: this >= e),
    "IFNULL": lambda e, alt: alt if e is None else e,
    "IF": lambda predicate, true, false: true if predicate else false,
    "INTDIV": null_if_any(lambda e, this: e // this),
    "INTERVAL": interval,
    "LIKE": null_if_any(
        lambda this, e: bool(re.match(e.replace("_", ".").replace("%", ".*"), this))
    ),
    "LOWER": null_if_any(lambda arg: arg.lower()),
    "LT": null_if_any(lambda this, e: this < e),
    "LTE": null_if_any(lambda this, e: this <= e),
    "MOD": null_if_any(lambda e, this: e % this),
    "MUL": null_if_any(lambda e, this: e * this),
    "NEQ": null_if_any(lambda this, e: this != e),
    "ORD": null_if_any(ord),
    "ORDERED": ordered,
    "POW": pow,
    "STRPOSITION": str_position,
    "SUB": null_if_any(lambda e, this: e - this),
    "SUBSTRING": substring,
    "TIMESTRTOTIME": null_if_any(lambda arg: datetime.datetime.fromisoformat(arg)),
    "UPPER": null_if_any(lambda arg: arg.upper()),
}
