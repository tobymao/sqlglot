from __future__ import annotations

import typing as t
from sqlglot import expressions as exp
from sqlglot import parser, generator, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    build_timetostr_or_tochar,
    build_formatted_time,
    build_date_delta,
    rename_func,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

DATE_DELTA = t.Union[exp.DateAdd, exp.DateSub]


def _date_delta_sql(name: str) -> t.Callable[[Dremio.Generator, DATE_DELTA], str]:
    def _delta_sql(self: Dremio.Generator, expression: DATE_DELTA) -> str:
        unit = expression.text("unit").upper()

        # Fallback to default behavior if unit is missing or 'DAY'
        if not unit or unit == "DAY":
            return self.func(name, expression.this, expression.expression)

        this_sql = self.sql(expression, "this")
        expr_sql = self.sql(expression, "expression")

        interval_sql = f"CAST({expr_sql} AS INTERVAL {unit})"
        return f"{name}({this_sql}, {interval_sql})"

    return _delta_sql


def to_char_is_numeric_handler(args: t.List, dialect: DialectType) -> exp.TimeToStr | exp.ToChar:
    expression = build_timetostr_or_tochar(args, dialect)
    fmt = seq_get(args, 1)

    if fmt and isinstance(expression, exp.ToChar) and fmt.is_string and "#" in fmt.name:
        # Only mark as numeric if format is a literal containing #
        expression.set("is_numeric", True)

    return expression


def build_date_delta_with_cast_interval(
    expression_class: t.Type[DATE_DELTA],
) -> t.Callable[[t.List[exp.Expression]], exp.Expression]:
    fallback_builder = build_date_delta(expression_class)

    def _builder(args):
        if len(args) == 2:
            date_arg, interval_arg = args

            if (
                isinstance(interval_arg, exp.Cast)
                and isinstance(interval_arg.to, exp.DataType)
                and isinstance(interval_arg.to.this, exp.Interval)
            ):
                return expression_class(
                    this=date_arg,
                    expression=interval_arg.this,
                    unit=interval_arg.to.this.unit,
                )

            return expression_class(this=date_arg, expression=interval_arg)

        return fallback_builder(args)

    return _builder


def datetype_handler(args: t.List[exp.Expression], dialect: DialectType) -> exp.Expression:
    year, month, day = args

    if all(isinstance(arg, exp.Literal) and arg.is_int for arg in (year, month, day)):
        date_str = f"{int(year.this):04d}-{int(month.this):02d}-{int(day.this):02d}"
        return exp.Date(this=exp.Literal.string(date_str))

    return exp.Cast(
        this=exp.Concat(
            expressions=[
                year,
                exp.Literal.string("-"),
                month,
                exp.Literal.string("-"),
                day,
            ]
        ),
        to=exp.DataType.build("DATE"),
    )


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

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "//", ("/*", "*/")]

    class Parser(parser.Parser):
        LOG_DEFAULTS_TO_LN = True

        NO_PAREN_FUNCTION_PARSERS = {
            **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
            "CURRENT_DATE_UTC": lambda self: self._parse_current_date_utc(),
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAY_GENERATE_RANGE": exp.GenerateSeries.from_arg_list,
            "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
            "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
            "DATE_ADD": build_date_delta_with_cast_interval(exp.DateAdd),
            "DATE_FORMAT": build_formatted_time(exp.TimeToStr, "dremio"),
            "DATE_SUB": build_date_delta_with_cast_interval(exp.DateSub),
            "REGEXP_MATCHES": exp.RegexpLike.from_arg_list,
            "REPEATSTR": exp.Repeat.from_arg_list,
            "TO_CHAR": to_char_is_numeric_handler,
            "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "dremio"),
            "DATE_PART": exp.Extract.from_arg_list,
            "DATETYPE": datetype_handler,
        }

        def _parse_current_date_utc(self) -> exp.Cast:
            if self._match(TokenType.L_PAREN):
                self._match_r_paren()

            return exp.Cast(
                this=exp.AtTimeZone(
                    this=exp.CurrentTimestamp(),
                    zone=exp.Literal.string("UTC"),
                ),
                to=exp.DataType.build("DATE"),
            )

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
            exp.BitwiseAndAgg: rename_func("BIT_AND"),
            exp.BitwiseOrAgg: rename_func("BIT_OR"),
            exp.ToChar: rename_func("TO_CHAR"),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.DateAdd: _date_delta_sql("DATE_ADD"),
            exp.DateSub: _date_delta_sql("DATE_SUB"),
            exp.GenerateSeries: rename_func("ARRAY_GENERATE_RANGE"),
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

        def cast_sql(self, expression: exp.Cast, safe_prefix: str | None = None) -> str:
            # Match: CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)
            if expression.is_type(exp.DataType.Type.DATE):
                at_time_zone = expression.this

                if (
                    isinstance(at_time_zone, exp.AtTimeZone)
                    and isinstance(at_time_zone.this, exp.CurrentTimestamp)
                    and isinstance(at_time_zone.args["zone"], exp.Literal)
                    and at_time_zone.text("zone").upper() == "UTC"
                ):
                    return "CURRENT_DATE_UTC"

            return super().cast_sql(expression, safe_prefix)
