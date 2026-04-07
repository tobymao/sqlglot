from __future__ import annotations

import typing as t
from collections.abc import Mapping, Sequence

if t.TYPE_CHECKING:
    from typing_extensions import ParamSpec

    import sqlglot
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.errors import ErrorLevel

    P = ParamSpec("P")

B = t.TypeVar("B", bound="sqlglot.exp.Binary")
E = t.TypeVar("E", bound="sqlglot.exp.Expr")
F = t.TypeVar("F", bound="sqlglot.exp.Func")
T = t.TypeVar("T")

BuilderArgs = Sequence[t.Any]
"""Sequence of arguments passed to builder functions."""


class _DialectArg(t.TypedDict, total=False):
    dialect: DialectType


class ParserNoDialectArgs(t.TypedDict, total=False):
    error_level: ErrorLevel | None
    error_message_context: int
    max_errors: int


class ParserArgs(ParserNoDialectArgs, _DialectArg, total=False):
    pass


class GeneratorNoDialectArgs(t.TypedDict, total=False):
    pretty: bool | int | None
    identify: str | bool
    normalize: bool
    pad: int
    indent: int
    normalize_functions: str | bool | None
    unsupported_level: ErrorLevel
    max_unsupported: int
    leading_comma: bool
    max_text_width: int
    comments: bool


class GeneratorArgs(GeneratorNoDialectArgs, _DialectArg, total=False):
    pass


class GraphHTMLArgs(t.TypedDict, total=False):
    imports: bool
    options: Mapping[str, object] | None


class SchemaArgs(t.TypedDict, total=False):
    visible: dict[str, object] | None
    dialect: DialectType
    normalize: bool
    udf_mapping: dict[str, object] | None
