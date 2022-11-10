from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.helper import AutoName


class ErrorLevel(AutoName):
    IGNORE = auto()  # Ignore any parser errors
    WARN = auto()  # Log any parser errors with ERROR level
    RAISE = auto()  # Collect all parser errors and raise a single exception
    IMMEDIATE = auto()  # Immediately raise an exception on the first parser error


class SqlglotError(Exception):
    pass


class UnsupportedError(SqlglotError):
    pass


class ParseError(SqlglotError):
    pass


class TokenError(SqlglotError):
    pass


class OptimizeError(SqlglotError):
    pass


class SchemaError(SqlglotError):
    pass


class ExecuteError(SqlglotError):
    pass


def concat_errors(errors: t.Sequence[t.Any], maximum: int) -> str:
    msg = [str(e) for e in errors[:maximum]]
    remaining = len(errors) - maximum
    if remaining > 0:
        msg.append(f"... and {remaining} more")
    return "\n\n".join(msg)
