from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from collections.abc import Mapping
    import sqlglot
    from collections.abc import Sequence
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.errors import ErrorLevel
    from sqlglot.tokenizer_core import Token, TokenType
    from sqlglot.expressions.core import ExpOrStr
    from sqlglot.expressions.datatypes import DType

B = t.TypeVar("B", bound="sqlglot.exp.Binary")
E = t.TypeVar("E", bound="sqlglot.exp.Expr")
F = t.TypeVar("F", bound="sqlglot.exp.Func")
T = t.TypeVar("T")
ExprArgs = t.Union[
    t.Optional[ExpOrStr],
    Sequence[ExpOrStr],
    DType,
    TokenType,
    ErrorLevel,
    Token,
    Sequence["ExprArgs"],
]


class _DialectArg(t.TypedDict, total=False):
    dialect: DialectType


class _CopyArg(t.TypedDict, total=False):
    copy: bool


class ParserNoDialectNoTableArgs(t.TypedDict, total=False):
    error_level: t.Optional[ErrorLevel]
    error_message_context: int
    max_errors: int


class ParserNoDialectArgs(ParserNoDialectNoTableArgs, total=False):
    table: t.Optional[t.Union[Sequence[ExpOrStr], bool]]


class ParserCopyArgs(ParserNoDialectArgs, _CopyArg, total=False):
    pass


class ParserCopyNoErrorLevelArgs(ParserNoDialectArgs, _CopyArg, total=False):
    pass


class ParserDialectNoCopyArgs(ParserNoDialectArgs, _DialectArg, total=False):
    pass


class ParserArgs(ParserDialectNoCopyArgs, _CopyArg, total=False):
    pass


class GeneratorNoDialectNoCopyArgs(t.TypedDict, total=False):
    pretty: t.Optional[bool | int]
    identify: str | bool
    normalize: bool
    pad: int
    indent: int
    normalize_functions: t.Optional[str | bool]
    unsupported_level: ErrorLevel
    max_unsupported: int
    leading_comma: bool
    max_text_width: int
    comments: bool


class GeneratorNoDialectArgs(GeneratorNoDialectNoCopyArgs, _CopyArg, total=False):
    pass


class GeneratorDialectNoCopyArgs(GeneratorNoDialectNoCopyArgs, _DialectArg, total=False):
    pass


class GeneratorArgs(GeneratorNoDialectArgs, _DialectArg, total=False):
    pass


class GraphHTMLArgs(t.TypedDict, total=False):
    imports: bool
    options: t.Optional[Mapping[str, object]]
