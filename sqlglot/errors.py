from enum import auto

from sqlglot.helper import AutoName


class ErrorLevel(AutoName):
    IGNORE = auto()
    WARN = auto()
    RAISE = auto()


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
