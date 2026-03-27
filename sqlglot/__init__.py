# ruff: noqa: F401, E402
"""
.. include:: ../README.md

----
"""

from __future__ import annotations

# bootstrap mypyc runtime: compiled .so modules do a top-level `import HASH__mypyc`,
# but the runtime .so lives inside sqlglot/. Pre-load it into sys.modules.
# this is only needed for editable builds
from collections.abc import Collection
import sys
from pathlib import Path
from builtins import type as Type

for path in Path(__file__).parent.glob("*__mypyc*.so"):
    name = path.stem.split(".")[0]
    if name not in sys.modules:
        import importlib.util

        spec = importlib.util.spec_from_file_location(name, path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)

import logging
import typing as t

from sqlglot import expressions as exp
from sqlglot.dialects.dialect import Dialect as Dialect, Dialects as Dialects
from sqlglot.diff import diff as diff
from sqlglot.errors import (
    ErrorLevel as ErrorLevel,
    ParseError as ParseError,
    TokenError as TokenError,
    UnsupportedError as UnsupportedError,
)
from sqlglot.expressions import (
    Expr as Expr,
    alias_ as alias,
    and_ as and_,
    case as case,
    cast as cast,
    column as column,
    condition as condition,
    delete as delete,
    except_ as except_,
    find_tables as find_tables,
    from_ as from_,
    func as func,
    insert as insert,
    intersect as intersect,
    maybe_parse as maybe_parse,
    merge as merge,
    not_ as not_,
    or_ as or_,
    select as select,
    subquery as subquery,
    table_ as table,
    to_column as to_column,
    to_identifier as to_identifier,
    to_table as to_table,
    union as union,
)
from sqlglot.generator import Generator as Generator
from sqlglot.parser import Parser as Parser
from sqlglot.schema import MappingSchema as MappingSchema, Schema as Schema
from sqlglot.tokens import Token as Token, Tokenizer as Tokenizer, TokenType as TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E, GeneratorDialectNoCopyArgs, ParserNoDialectArgs
    from typing_extensions import Unpack
    from sqlglot.dialects.dialect import DialectType as DialectType

logger = logging.getLogger("sqlglot")


try:
    from sqlglot._version import __version__, __version_tuple__  # type: ignore[import-not-found]
except ImportError:
    logger.error(
        "Unable to set __version__, run `pip install -e .` or `python setup.py develop` first."
    )


pretty = False
"""Whether to format generated SQL by default."""


def tokenize(sql: str, read: DialectType = None, dialect: DialectType = None) -> list[Token]:
    """
    Tokenizes the given SQL string.

    Args:
        sql: the SQL code string to tokenize.
        read: the SQL dialect to apply during tokenizing (eg. "spark", "hive", "presto", "mysql").
        dialect: the SQL dialect (alias for read).

    Returns:
        The resulting list of tokens.
    """
    return Dialect.get_or_raise(read or dialect).tokenize(sql)


def parse(
    sql: str,
    read: DialectType = None,
    dialect: DialectType = None,
    **opts: Unpack[ParserNoDialectArgs],
) -> list[t.Optional[Expr]]:
    """
    Parses the given SQL string into a collection of syntax trees, one per parsed SQL statement.

    Args:
        sql: the SQL code string to parse.
        read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql").
        dialect: the SQL dialect (alias for read).
        **opts: other `sqlglot.parser.Parser` options.

    Returns:
        The resulting syntax tree collection.
    """
    return Dialect.get_or_raise(read or dialect).parse(sql, **opts)


@t.overload
def parse_one(
    sql: str,
    *,
    read: DialectType = None,
    dialect: DialectType = None,
    into: Type[E],
    **opts: Unpack[ParserNoDialectArgs],
) -> E: ...


@t.overload
def parse_one(
    sql: str,
    *,
    read: DialectType = None,
    dialect: DialectType = None,
    into: Collection[Type[exp.Expr]],
    **opts: Unpack[ParserNoDialectArgs],
) -> exp.Expr: ...


@t.overload
def parse_one(
    sql: str,
    read: DialectType = None,
    dialect: DialectType = None,
    **opts: Unpack[ParserNoDialectArgs],
) -> Expr: ...


def parse_one(
    sql: str,
    read: DialectType = None,
    dialect: DialectType = None,
    into: t.Optional[exp.IntoType] = None,
    **opts: Unpack[ParserNoDialectArgs],
) -> Expr:
    """
    Parses the given SQL string and returns a syntax tree.

    Args:
        sql: the SQL code string to parse.
        read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql").
        dialect: the SQL dialect (alias for read)
        into: the SQLGlot Expr to parse into.
        **opts: other `sqlglot.parser.Parser` options.

    Returns:
        A single syntax tree if one statement is parsed, otherwise a Block expression containing all parsed syntax trees.
    """

    dialect = Dialect.get_or_raise(read or dialect)

    if into:
        result = dialect.parse_into(into, sql, **opts)
    else:
        result = dialect.parse(sql, **opts)

    if not result or result[0] is None:
        raise ParseError(f"No expression was parsed from '{sql}'")

    return exp.Block(expressions=result) if len(result) > 1 else result[0]


def transpile(
    sql: str,
    read: DialectType = None,
    write: DialectType = None,
    identity: bool = True,
    error_level: t.Optional[ErrorLevel] = None,
    **opts: Unpack[GeneratorDialectNoCopyArgs],
) -> list[str]:
    """
    Parses the given SQL string in accordance with the source dialect and returns a list of SQL strings transformed
    to conform to the target dialect. Each string in the returned list represents a single transformed SQL statement.

    Args:
        sql: the SQL code string to transpile.
        read: the source dialect used to parse the input string (eg. "spark", "hive", "presto", "mysql").
        write: the target dialect into which the input should be transformed (eg. "spark", "hive", "presto", "mysql").
        identity: if set to `True` and if the target dialect is not specified the source dialect will be used as both:
            the source and the target dialect.
        error_level: the desired error level of the parser.
        **opts: other `sqlglot.generator.Generator` options.

    Returns:
        The list of transpiled SQL statements.
    """
    write = (read if write is None else write) if identity else write
    write = Dialect.get_or_raise(write)
    return [
        write.generate(expression, copy=False, **opts) if expression else ""
        for expression in parse(sql, read, error_level=error_level)
    ]
