# ruff: noqa: F405
"""
## Exprs

Every AST node in SQLGlot is represented by a subclass of `Expr`.

This module contains the implementation of all supported `Expr` types. Additionally,
it exposes a number of helper functions, which are mainly used to programmatically build
SQL expressions, such as `sqlglot.expressions.select`.

----
"""

import typing as t

from sqlglot.expressions.core import *  # noqa: F401,F403
from sqlglot.expressions.datatypes import *  # noqa: F401,F403
from sqlglot.expressions.constraints import *  # noqa: F401,F403
from sqlglot.expressions.properties import *  # noqa: F401,F403
from sqlglot.expressions.query import *  # noqa: F401,F403
from sqlglot.expressions.ddl import *  # noqa: F401,F403
from sqlglot.expressions.dml import *  # noqa: F401,F403
from sqlglot.expressions.math import *  # noqa: F401,F403
from sqlglot.expressions.string import *  # noqa: F401,F403
from sqlglot.expressions.temporal import *  # noqa: F401,F403
from sqlglot.expressions.aggregate import *  # noqa: F401,F403
from sqlglot.expressions.array import *  # noqa: F401,F403
from sqlglot.expressions.json import *  # noqa: F401,F403
from sqlglot.expressions.functions import *  # noqa: F401,F403
from sqlglot.expressions.builders import *  # noqa: F401,F403

# Explicitly import private helpers (not exported by star imports)
from sqlglot.expressions.core import (  # noqa: F401,E402
    Expression,
    _apply_builder,
    _apply_child_list_builder,
    _apply_list_builder,
    _apply_conjunction_builder,
    _apply_set_operation,
    _combine,
    _wrap,
    _is_wrong_expression,
    _to_s,
)
from sqlglot.expressions.query import _apply_cte_builder  # noqa: F401,E402
from sqlglot.expressions.dml import DML  # noqa: F401,E402
from sqlglot.expressions.array import _ExplodeOuter  # noqa: F401,E402

from sqlglot.helper import subclasses

ALL_FUNCTIONS = subclasses(__name__, Func, {AggFunc, Anonymous, Func})
FUNCTION_BY_NAME = {name: func for func in ALL_FUNCTIONS for name in func.sql_names()}


def _init_subclasses(cls: t.Type[Expr]) -> None:
    # mypyc fires __init_subclass__ before setting compiled ClassVar attributes,
    # so required_args may have been computed from the inherited arg_types rather
    # than the class-specific one. Recompute now that all modules are fully loaded.
    for sub in cls.__subclasses__():
        sub.required_args = {k for k, v in sub.arg_types.items() if v}
        _init_subclasses(sub)


_init_subclasses(Expr)


def _build_expr_classes(cls: t.Type[Expr], result: t.Dict[str, t.Type[Expr]]) -> None:
    result[cls.key] = cls
    for sub in cls.__subclasses__():
        _build_expr_classes(sub, result)


EXPR_CLASSES: t.Dict[str, t.Type[Expr]] = {}
_build_expr_classes(Expr, EXPR_CLASSES)
