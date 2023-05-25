from __future__ import annotations

import typing as t

import sqlglot

E = t.TypeVar("E", bound="sqlglot.exp.Expression")
F = t.TypeVar("F", bound="sqlglot.exp.Func")
T = t.TypeVar("T")
