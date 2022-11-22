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
    def __init__(
        self,
        message: str,
        description: t.Optional[str] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start_context: t.Optional[str] = None,
        highlight: t.Optional[str] = None,
        end_context: t.Optional[str] = None,
        error_props: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    ):
        super().__init__(message)
        self.error_props = error_props or []
        props = {
            "description": description,
            "line": line,
            "col": col,
            "start_context": start_context,
            "highlight": highlight,
            "end_context": end_context,
        }
        if any(p is not None for p in props.values()):
            self.error_props.append(props)


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


def concat_error_props(errors: t.Sequence[ParseError], maximum: int) -> t.List[t.Dict[str, t.Any]]:
    return [props for e in errors[:maximum] for props in e.error_props]
