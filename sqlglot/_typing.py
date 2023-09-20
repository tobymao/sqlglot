from __future__ import annotations

import typing as t

import sqlglot

# A little hack for backwards compatibility with Python 3.7, which doesn't have Protocols.
A = t.TypeVar("A", bound=t.Any)

E = t.TypeVar("E", bound="sqlglot.exp.Expression")
T = t.TypeVar("T")
