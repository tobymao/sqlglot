from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot import parser, generator, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    build_timetostr_or_tochar,
    build_formatted_time,
    rename_func,
    unit_to_var,
)
from sqlglot.helper import seq_get

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

DATE_DELTA = t.Union[exp.DateAdd, exp.DateSub]


def _date_delta_sql(name: str) -> t.Callable[[Dremio.Generator, DATE_DELTA], str]:
    def _delta_sql(self: Dremio.Generator, expression: DATE_DELTA) -> str:
        unit = expression.text("unit")

        if not unit or unit.upper() == "DAY":
            return self.func(name, expression.this, expression.expression)

        # to support units we need to use TIMESTAMPADD function
        increment = expression.expression
        if isinstance(expression, exp.DateSub):
            if isinstance(increment, exp.Literal):
                value = increment.to_py() if increment.is_number else int(increment.name)
                increment = exp.Literal.number(value * -1)
            else:
                increment *= exp.Literal.number(-1)
        return self.func("TIMESTAMPADD", unit_to_var(expression), increment, expression.this)

    return _delta_sql


def to_char_is_numeric_handler(args: t.List, dialect: DialectType) -> exp.TimeToStr | exp.ToChar:
    expression = build_timetostr_or_tochar(args, dialect)
    fmt = seq_get(args, 1)

    if fmt and isinstance(expression, exp.ToChar) and fmt.is_string and "#" in fmt.name:
        # Only mark as numeric if format is a literal containing #
        expression.set("is_numeric", True)

    return expression


class Dremio(Dialect):
    SUPPORTS_USER_DEFINED_TYPES = False
    CONCAT_COALESCE = True
    TYPED_DIVISION = True
    SUPPORTS_SEMI_ANTI_JOIN = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_VALUES_DEFAULT = False

    TIME_MAPPING = {
        # year
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        # month / day
        "MM": "%m",
        "mm": "%m",
        "MON": "%b",
        "mon": "%b",
        "MONTH": "%B",
        "month": "%B",
        "DDD": "%j",
        "ddd": "%j",
        "DD": "%d",
        "dd": "%d",
        "DY": "%a",
        "dy": "%a",
        "DAY": "%A",
        "day": "%A",
        # hours / minutes / seconds
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "HH": "%I",
        "hh": "%I",  # 24- / 12-hour
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FFF": "%f",
        "fff": "%f",
        "AMPM": "%p",
        "ampm": "%p",
        # ISO week / century etc.
        "WW": "%W",
        "ww": "%W",
        "D": "%w",
        "d": "%w",
        "CC": "%C",
        "cc": "%C",
        # timezone
        "TZD": "%Z",
        "tzd": "%Z",  # abbreviation (UTC, PST, ...)
        "TZO": "%z",
        "tzo": "%z",  # numeric offset (+0200)
    }

    class Parser(parser.Parser):
        LOG_DEFAULTS_TO_LN = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "TO_CHAR": to_char_is_numeric_handler,
            "DATE_FORMAT": build_formatted_time(exp.TimeToStr, "dremio"),
            "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "dremio"),
        }

    class Generator(generator.Generator):
        NVL2_SUPPORTED = False
        SUPPORTS_CONVERT_TIMEZONE = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        LIMIT_ONLY_LITERALS = True
        MULTI_ARG_DISTINCT = False
        SUPPORTS_BETWEEN_FLAGS = True

        # https://docs.dremio.com/current/reference/sql/data-types/
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.SMALLINT: "INT",
            exp.DataType.Type.TINYINT: "INT",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.NCHAR: "VARCHAR",
            exp.DataType.Type.CHAR: "VARCHAR",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.ARRAY: "LIST",
            exp.DataType.Type.BIT: "BOOLEAN",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ToChar: rename_func("TO_CHAR"),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.DateAdd: _date_delta_sql("DATE_ADD"),
            exp.DateSub: _date_delta_sql("DATE_SUB"),
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Reject time-zone–aware TIMESTAMPs, which Dremio does not accept
            """
            if expression.is_type(
                exp.DataType.Type.TIMESTAMPTZ,
                exp.DataType.Type.TIMESTAMPLTZ,
            ):
                self.unsupported("Dremio does not support time-zone-aware TIMESTAMP")

            return super().datatype_sql(expression)

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "//", ("/*", "*/")]
