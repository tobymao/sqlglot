from __future__ import annotations

import importlib
import typing as t

if t.TYPE_CHECKING:
    from sqlglot.optimizer.optimizer import RULES as RULES, optimize as optimize
    from sqlglot.optimizer.scope import (
        Scope as Scope,
        build_scope as build_scope,
        find_all_in_scope as find_all_in_scope,
        find_in_scope as find_in_scope,
        traverse_scope as traverse_scope,
        walk_in_scope as walk_in_scope,
    )

_LAZY_ATTRS = {
    "RULES": "sqlglot.optimizer.optimizer",
    "optimize": "sqlglot.optimizer.optimizer",
    "Scope": "sqlglot.optimizer.scope",
    "build_scope": "sqlglot.optimizer.scope",
    "find_all_in_scope": "sqlglot.optimizer.scope",
    "find_in_scope": "sqlglot.optimizer.scope",
    "traverse_scope": "sqlglot.optimizer.scope",
    "walk_in_scope": "sqlglot.optimizer.scope",
}


def __getattr__(name: str) -> t.Any:
    module_name = _LAZY_ATTRS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(_LAZY_ATTRS)
