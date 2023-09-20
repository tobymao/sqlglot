from __future__ import annotations

import typing as t

import sqlglot


class Comparable(t.Protocol):
    def __lt__(self, __other: t.Any) -> bool:
        ...

    def __le__(self, __other: t.Any) -> bool:
        ...

    def __gt__(self, __other: t.Any) -> bool:
        ...

    def __ge__(self, __other: t.Any) -> bool:
        ...


C = t.TypeVar("C", bound=Comparable)
E = t.TypeVar("E", bound="sqlglot.exp.Expression")
T = t.TypeVar("T")
