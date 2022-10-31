import datetime
import re
import statistics

from sqlglot.helper import PYTHON_VERSION


class reverse_key:
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        return other.obj == self.obj

    def __lt__(self, other):
        return other.obj < self.obj


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
    "SUM": sum,
    "AVG": statistics.fmean if PYTHON_VERSION >= (3, 8) else statistics.mean,  # type: ignore
    "COUNT": lambda acc: sum(1 for e in acc if e is not None),
    "MAX": max,
    "MIN": min,
    "POW": pow,
}
