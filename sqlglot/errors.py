from enum import auto

from sqlglot.helper import AutoName


class ErrorLevel(AutoName):
    IGNORE = auto()
    WARN = auto()
    RAISE = auto()


class UnsupportedError(ValueError):
    pass


class ParseError(ValueError):
    pass


class TokenError(ValueError):
    pass
