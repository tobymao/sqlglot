from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot import parser
from sqlglot.dialects.dialect import (
    build_date_delta,
    build_formatted_time,
    build_timetostr_or_tochar,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

DATE_DELTA = t.Union[exp.DateAdd, exp.DateSub]


def to_char_is_numeric_handler(args: t.List, dialect: DialectType) -> exp.TimeToStr | exp.ToChar:
    expression = build_timetostr_or_tochar(args, dialect)
    fmt = seq_get(args, 1)

    if fmt and isinstance(expression, exp.ToChar) and fmt.is_string and "#" in fmt.name:
        # Only mark as numeric if format is a literal containing #
        expression.set("is_numeric", True)

    return expression


def build_date_delta_with_cast_interval(
    expression_class: t.Type[DATE_DELTA],
) -> t.Callable[[t.List[exp.Expr]], exp.Expr]:
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


def datetype_handler(args: t.List[exp.Expr], dialect: DialectType) -> exp.Expr:
    from sqlglot.dialects.dialect import Dialect

    year, month, day = args

    if all(isinstance(arg, exp.Literal) and arg.is_int for arg in (year, month, day)):
        date_str = f"{int(year.this):04d}-{int(month.this):02d}-{int(day.this):02d}"
        return exp.Date(this=exp.Literal.string(date_str))

    dialect = Dialect.get_or_raise(dialect)

    return exp.Cast(
        this=exp.Concat(
            expressions=[
                year,
                exp.Literal.string("-"),
                month,
                exp.Literal.string("-"),
                day,
            ],
            coalesce=dialect.CONCAT_COALESCE,
        ),
        to=exp.DType.DATE.into_expr(),
    )


class DremioParser(parser.Parser):
    LOG_DEFAULTS_TO_LN = True

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

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
            to=exp.DType.DATE.into_expr(),
        )
