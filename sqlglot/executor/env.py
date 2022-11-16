import datetime
import inspect
import re
import statistics
from functools import wraps

from sqlglot.helper import PYTHON_VERSION


class reverse_key:
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        return other.obj == self.obj

    def __lt__(self, other):
        return other.obj < self.obj


def filter_nulls(func):
    @wraps(func)
    def _func(values):
        return func(v for v in values if v is not None)

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


ENV = {
    "__builtins__": {},
    "datetime": datetime,
    "locals": locals,
    "re": re,
    "bool": bool,
    "float": float,
    "int": int,
    "str": str,
    "desc": reverse_key,
    "SUM": filter_nulls(sum),
    "AVG": filter_nulls(statistics.fmean if PYTHON_VERSION >= (3, 8) else statistics.mean),  # type: ignore
    "COUNT": filter_nulls(lambda acc: sum(1 for _ in acc)),
    "MAX": filter_nulls(max),
    "MIN": filter_nulls(min),
    "POW": pow,
    "CONCAT": null_if_any(lambda *args: "".join(args)),
    "STR_POSITION": str_position,
    "UPPER": null_if_any(lambda arg: arg.upper()),
    "LOWER": null_if_any(lambda arg: arg.lower()),
    "ORD": null_if_any(ord),
    "IFNULL": lambda e, alt: alt if e is None else e,
    "SUBSTRING": substring,
    "COALESCE": lambda *args: next((a for a in args if a is not None), None),
}
