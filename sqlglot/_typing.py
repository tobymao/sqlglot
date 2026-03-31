from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from collections.abc import Mapping
    import sqlglot
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.errors import ErrorLevel

B = t.TypeVar("B", bound="sqlglot.exp.Binary")
E = t.TypeVar("E", bound="sqlglot.exp.Expr")
F = t.TypeVar("F", bound="sqlglot.exp.Func")
T = t.TypeVar("T")


class _DialectArg(t.TypedDict, total=False):
    dialect: DialectType


class ParserNoDialectArgs(t.TypedDict, total=False):
    error_level: t.Optional[ErrorLevel]
    error_message_context: int
    max_errors: int


class ParserArgs(ParserNoDialectArgs, _DialectArg, total=False):
    pass


class GeneratorNoDialectArgs(t.TypedDict, total=False):
    pretty: t.Optional[t.Union[bool, int]]
    identify: t.Union[str, bool]
    normalize: bool
    pad: int
    indent: int
    normalize_functions: t.Optional[t.Union[str, bool]]
    unsupported_level: ErrorLevel
    max_unsupported: int
    leading_comma: bool
    max_text_width: int
    comments: bool


class GeneratorArgs(GeneratorNoDialectArgs, _DialectArg, total=False):
    pass


class GraphHTMLArgs(t.TypedDict, total=False):
    imports: bool
    options: t.Optional[Mapping[str, object]]
