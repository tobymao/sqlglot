from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import (
    binary_from_function,
    build_date_delta,
    build_formatted_time,
    build_timetostr_or_tochar,
    build_trunc,
)
from sqlglot.helper import mypyc_attr, seq_get
from sqlglot.tokens import TokenType

DATE_UNITS = {"DAY", "WEEK", "MONTH", "YEAR", "HOUR", "MINUTE", "SECOND"}


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/zeroifnull.htm
def _build_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/nullifzero.htm
def _build_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


@mypyc_attr(allow_interpreted_subclasses=True)
class ExasolParser(parser.Parser):
    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        **{f"ADD_{unit}S": build_date_delta(exp.DateAdd, default_unit=unit) for unit in DATE_UNITS},
        **{
            f"{unit}S_BETWEEN": build_date_delta(exp.DateDiff, default_unit=unit)
            for unit in DATE_UNITS
        },
        "APPROXIMATE_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
        "BIT_AND": binary_from_function(exp.BitwiseAnd),
        "BIT_OR": binary_from_function(exp.BitwiseOr),
        "BIT_XOR": binary_from_function(exp.BitwiseXor),
        "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
        "BIT_LSHIFT": binary_from_function(exp.BitwiseLeftShift),
        "BIT_RSHIFT": binary_from_function(exp.BitwiseRightShift),
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/convert_tz.htm
        "CONVERT_TZ": lambda args: exp.ConvertTimezone(
            source_tz=seq_get(args, 1),
            target_tz=seq_get(args, 2),
            timestamp=seq_get(args, 0),
            options=seq_get(args, 3),
        ),
        "CURDATE": exp.CurrentDate.from_arg_list,
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/date_trunc.htm#DATE_TRUNC
        "DATE_TRUNC": lambda args: exp.TimestampTrunc(this=seq_get(args, 1), unit=seq_get(args, 0)),
        "DIV": binary_from_function(exp.IntDiv),
        "EVERY": lambda args: exp.All(this=seq_get(args, 0)),
        "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
        "FROM_POSIX_TIME": exp.UnixToTime.from_arg_list,
        "HASH_SHA": exp.SHA.from_arg_list,
        "HASH_SHA1": exp.SHA.from_arg_list,
        "HASH_MD5": exp.MD5.from_arg_list,
        "HASHTYPE_MD5": exp.MD5Digest.from_arg_list,
        "HASH_SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
        "HASH_SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
        "NOW": exp.CurrentTimestamp.from_arg_list,
        "NULLIFZERO": _build_nullifzero,
        "REGEXP_LIKE": lambda args: exp.RegexpLike(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            flag=seq_get(args, 2),
            full_match=True,
        ),
        "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
        "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            replacement=seq_get(args, 2),
            position=seq_get(args, 3),
            occurrence=seq_get(args, 4),
        ),
        "TRUNC": build_trunc,
        "TRUNCATE": build_trunc,
        "TO_CHAR": build_timetostr_or_tochar,
        "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "exasol"),
        "USER": exp.CurrentUser.from_arg_list,
        "VAR_POP": exp.VariancePop.from_arg_list,
        "ZEROIFNULL": _build_zeroifnull,
    }
    CONSTRAINT_PARSERS = {
        **parser.Parser.CONSTRAINT_PARSERS,
        "COMMENT": lambda self: self.expression(
            exp.CommentColumnConstraint,
            this=self._match(TokenType.IS) and self._parse_string(),
        ),
    }

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.RLIKE: lambda self, this: self.expression(
            exp.RegexpLike,
            this=this,
            expression=self._parse_bitwise(),
            full_match=True,
        ),
    }

    FUNC_TOKENS = {
        *parser.Parser.FUNC_TOKENS,
        TokenType.SYSTIMESTAMP,
    }

    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.SYSTIMESTAMP: exp.Systimestamp,
        TokenType.CURRENT_SCHEMA: exp.CurrentSchema,
    }

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/listagg.htm
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/group_concat.htm
        **dict.fromkeys(("GROUP_CONCAT", "LISTAGG"), lambda self: self._parse_group_concat()),
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/json_value.htm
        "JSON_VALUE": lambda self: self._parse_json_value(),
        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/json_extract.htm
        "JSON_EXTRACT": lambda self: self._parse_json_extract(),
    }

    def _parse_column(self) -> t.Optional[exp.Expr]:
        column = super()._parse_column()
        if not isinstance(column, exp.Column):
            return column
        table_ident = column.args.get("table")
        if (
            isinstance(table_ident, exp.Identifier)
            and table_ident.name.upper() == "LOCAL"
            and not bool(table_ident.args.get("quoted"))
        ):
            column.set("table", None)
        return column

    def _parse_json_extract(self) -> exp.JSONExtract:
        args = self._parse_expressions()
        self._match_r_paren()

        expression = exp.JSONExtract(expressions=args)

        if self._match_texts("EMITS"):
            expression.set("emits", self._parse_schema())

        return expression

    ODBC_DATETIME_LITERALS = {
        "d": exp.Date,
        "ts": exp.Timestamp,
    }
