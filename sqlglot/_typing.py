from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    import sqlglot

B = t.TypeVar("B", bound="sqlglot.exp.Binary")
E = t.TypeVar("E", bound="sqlglot.exp.Expr")
F = t.TypeVar("F", bound="sqlglot.exp.Func")
T = t.TypeVar("T")
