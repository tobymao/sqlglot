# ruff: noqa: F401
"""Re-exports from optimizer submodules, deferred to first use.

Under mypyc's ``separate=True``, importing ``sqlglot`` can transitively
trigger this package's ``__init__.py`` before ``sqlglot`` has finished
binding names like ``exp`` / ``Schema``. Eager ``from sqlglot.optimizer.optimizer
import ...`` at module top used to kick off a circular-import cascade.
PEP 562 ``__getattr__`` lets the names resolve only when they're first
actually accessed, by which point ``sqlglot``'s namespace is settled.
"""

from __future__ import annotations

import typing as _t

if _t.TYPE_CHECKING:
    from sqlglot.optimizer.optimizer import RULES as RULES, optimize as optimize
    from sqlglot.optimizer.scope import (
        Scope as Scope,
        build_scope as build_scope,
        find_all_in_scope as find_all_in_scope,
        find_in_scope as find_in_scope,
        traverse_scope as traverse_scope,
        walk_in_scope as walk_in_scope,
    )

# Explicit re-export map. An explicit mapping avoids the ambiguity of
# fuzzy "search the first module that has the name" — several submodules
# share names with functions inside `optimizer.py` (e.g. both a
# `qualify` submodule and a `qualify` function re-exported in the
# `optimizer` module), so callers doing `optimizer.qualify.qualify(...)`
# need the submodule, not the function.
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


def __getattr__(name: str) -> _t.Any:
    import importlib

    target = _LAZY_ATTRS.get(name)
    if target is not None:
        module_name, attr = target
        value = getattr(importlib.import_module(module_name), attr)
    else:
        # Fall back to loading `sqlglot.optimizer.<name>` as a submodule.
        # The old eager __init__ used to populate these as a side effect
        # of its imports; under lazy resolution we have to do it explicitly.
        try:
            value = importlib.import_module(f"{__name__}.{name}")
        except ModuleNotFoundError:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value
