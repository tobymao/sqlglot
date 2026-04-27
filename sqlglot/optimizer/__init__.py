# ruff: noqa: F401
"""Lazy re-exports from optimizer submodules.

Eager re-exports here trip a circular import under sqlglot[c]: importing
sqlglot loads compiled `expressions.builders`, which eagerly wires up its
links to compiled optimizer modules, which causes Python to run this
file. The eager `from sqlglot.optimizer.optimizer import ...` then asks
for `sqlglot.Schema`, but `sqlglot/__init__.py` hasn't bound it yet.

PEP 562 `__getattr__` defers the import to first attribute access, by
which point sqlglot is fully loaded. Tracked upstream in python/mypy#21299.
"""

from __future__ import annotations

import typing as t

# Only for type checkers and IDEs; runtime resolution goes through __getattr__.
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

# Explicit, because some names collide between optimizer.py and submodules
# (e.g. `qualify` is both a function and a submodule).
_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "RULES": ("sqlglot.optimizer.optimizer", "RULES"),
    "optimize": ("sqlglot.optimizer.optimizer", "optimize"),
    "Scope": ("sqlglot.optimizer.scope", "Scope"),
    "build_scope": ("sqlglot.optimizer.scope", "build_scope"),
    "find_all_in_scope": ("sqlglot.optimizer.scope", "find_all_in_scope"),
    "find_in_scope": ("sqlglot.optimizer.scope", "find_in_scope"),
    "traverse_scope": ("sqlglot.optimizer.scope", "traverse_scope"),
    "walk_in_scope": ("sqlglot.optimizer.scope", "walk_in_scope"),
}


def __getattr__(name: str) -> t.Any:
    import importlib

    target = _LAZY_ATTRS.get(name)
    if target is not None:
        module_name, attr = target
        value = getattr(importlib.import_module(module_name), attr)
    else:
        # Submodule fallback so `from sqlglot.optimizer import qualify` works.
        try:
            value = importlib.import_module(f"{__name__}.{name}")
        except ModuleNotFoundError:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value
