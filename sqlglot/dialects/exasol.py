from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    build_formatted_time,
    rename_func,
    strposition_sql,
    timestrtotime_sql,
    unit_to_str,
    timestamptrunc_sql,
    build_date_delta,
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


def _sha2_sql(self: Exasol.Generator, expression: exp.SHA2) -> str:
    length = expression.text("length")
    func_name = "HASH_SHA256" if length == "256" else "HASH_SHA512"
    return self.func(func_name, expression.this)


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc%5Bate%5D%20(datetime).htm
# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc%5Bate%5D%20(number).htm
def _build_trunc(args: t.List[exp.Expression], dialect: DialectType) -> exp.Expression:
    first, second = seq_get(args, 0), seq_get(args, 1)

    if not first or not second:
        return exp.Anonymous(this="TRUNC", expressions=args)

    if not first.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        first = annotate_types(first, dialect=dialect)

    if first.is_type(exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP) and second.is_string:
        return exp.DateTrunc(this=first, unit=second)

    return exp.Anonymous(this="TRUNC", expressions=args)


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/zeroifnull.htm
def _build_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/nullifzero.htm
def _build_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


class Exasol(Dialect):
    TIME_MAPPING = {
        "yyyy": "%Y",
        "YYYY": "%Y",
        "yy": "%y",
        "YY": "%y",
        "mm": "%m",
        "MM": "%m",
        "MONTH": "%B",
        "MON": "%b",
        "dd": "%d",
        "DD": "%d",
        "DAY": "%A",
        "DY": "%a",
        "H12": "%I",
        "H24": "%H",
        "HH": "%H",
        "ID": "%u",
        "vW": "%V",
        "IW": "%V",
        "vYYY": "%G",
        "IYYY": "%G",
        "MI": "%M",
        "SS": "%S",
        "uW": "%W",
        "UW": "%U",
        "Z": "%z",
    }

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "USER": TokenType.CURRENT_USER,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/if.htm
            "ENDIF": TokenType.END,
            "LONG VARCHAR": TokenType.TEXT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_days.htm
            "ADD_DAYS": build_date_delta(exp.DateAdd, default_unit="DAY"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_years.htm
            "ADD_YEARS": build_date_delta(exp.DateAdd, default_unit="YEAR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_months.htm
            "ADD_MONTHS": build_date_delta(exp.DateAdd, default_unit="MONTH"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_weeks.htm
            "ADD_WEEKS": build_date_delta(exp.DateAdd, default_unit="WEEK"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_hour.htm
            "ADD_HOURS": build_date_delta(exp.DateAdd, default_unit="HOUR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_minutes.htm
            "ADD_MINUTES": build_date_delta(exp.DateAdd, default_unit="MINUTE"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/add_seconds.htm
            "ADD_SECONDS": build_date_delta(exp.DateAdd, default_unit="SECOND"),
            "BIT_AND": binary_from_function(exp.BitwiseAnd),
            "BIT_OR": binary_from_function(exp.BitwiseOr),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BIT_LSHIFT": binary_from_function(exp.BitwiseLeftShift),
            "BIT_RSHIFT": binary_from_function(exp.BitwiseRightShift),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/date_trunc.htm#DATE_TRUNC
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "EVERY": lambda args: exp.All(this=seq_get(args, 0)),
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "HASH_SHA": exp.SHA.from_arg_list,
            "HASH_SHA1": exp.SHA.from_arg_list,
            "HASH_MD5": exp.MD5.from_arg_list,
            "HASHTYPE_MD5": exp.MD5Digest.from_arg_list,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                position=seq_get(args, 3),
                occurrence=seq_get(args, 4),
            ),
            "HASH_SHA256": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(256)
            ),
            "HASH_SHA512": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(512)
            ),
            "TRUNC": _build_trunc,
            "TRUNCATE": _build_trunc,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "APPROXIMATE_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "TO_CHAR": build_formatted_time(exp.ToChar, "exasol"),
            "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "exasol"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/convert_tz.htm
            "CONVERT_TZ": lambda args: exp.ConvertTimezone(
                source_tz=seq_get(args, 1),
                target_tz=seq_get(args, 2),
                timestamp=seq_get(args, 0),
                options=seq_get(args, 3),
            ),
            "NULLIFZERO": _build_nullifzero,
            "ZEROIFNULL": _build_zeroifnull,
        }
        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "COMMENT": lambda self: self.expression(
                exp.CommentColumnConstraint,
                this=self._match(TokenType.IS) and self._parse_string(),
            ),
        }

    class Generator(generator.Generator):
        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#StringDataType
        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
            exp.DataType.Type.TEXT: "LONG VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
        }

        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        DATE_ADD_FUNCTION_BY_UNIT = {
            "DAY": "ADD_DAYS",
            "WEEK": "ADD_WEEKS",
            "MONTH": "ADD_MONTHS",
            "YEAR": "ADD_YEARS",
            "HOUR": "ADD_HOURS",
            "MINUTE": "ADD_MINUTES",
            "SECOND": "ADD_SECONDS",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Exasol supports a fixed default precision of 3 for TIMESTAMP WITH LOCAL TIME ZONE
            # and does not allow specifying a different custom precision
            if expression.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return "TIMESTAMP WITH LOCAL TIME ZONE"

            return super().datatype_sql(expression)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/every.htm
            exp.All: rename_func("EVERY"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_and.htm
            exp.BitwiseAnd: rename_func("BIT_AND"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_or.htm
            exp.BitwiseOr: rename_func("BIT_OR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_not.htm
            exp.BitwiseNot: rename_func("BIT_NOT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_lshift.htm
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_rshift.htm
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_xor.htm
            exp.BitwiseXor: rename_func("BIT_XOR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/every.htm
            exp.All: rename_func("EVERY"),
            exp.DateTrunc: lambda self, e: self.func("TRUNC", e.this, unit_to_str(e)),
            exp.DatetimeTrunc: timestamptrunc_sql(),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/edit_distance.htm#EDIT_DISTANCE
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/mod.htm
            exp.Mod: rename_func("MOD"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/regexp_substr.htm
            exp.RegexpExtract: unsupported_args("parameters", "group")(
                rename_func("REGEXP_SUBSTR")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/regexp_replace.htm
            exp.RegexpReplace: unsupported_args("modifiers")(rename_func("REGEXP_REPLACE")),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/var_pop.htm
            exp.VariancePop: rename_func("VAR_POP"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/approximate_count_distinct.htm
            exp.ApproxDistinct: unsupported_args("accuracy")(
                rename_func("APPROXIMATE_COUNT_DISTINCT")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_char%20(datetime).htm
            exp.ToChar: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_date.htm
            exp.TsOrDsToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimestampTrunc: timestamptrunc_sql(),
            exp.StrToTime: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.CurrentUser: lambda *_: "CURRENT_USER",
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TZ",
                e.this,
                "'UTC'",
                e.args.get("zone"),
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/instr.htm
            exp.StrPosition: lambda self, e: (
                strposition_sql(
                    self, e, func_name="INSTR", supports_position=True, supports_occurrence=True
                )
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha%5B1%5D.htm#HASH_SHA%5B1%5D
            exp.SHA: rename_func("HASH_SHA"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha256.htm
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha512.htm
            exp.SHA2: _sha2_sql,
            exp.MD5: rename_func("HASH_MD5"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hashtype_md5.htm
            exp.MD5Digest: rename_func("HASHTYPE_MD5"),
            # https://docs.exasol.com/db/latest/sql/create_view.htm
            exp.CommentColumnConstraint: lambda self, e: f"COMMENT IS {self.sql(e, 'this')}",
        }

        def converttimezone_sql(self, expression: exp.ConvertTimezone) -> str:
            from_tz = expression.args.get("source_tz")
            to_tz = expression.args.get("target_tz")
            datetime = expression.args.get("timestamp")
            options = expression.args.get("options")

            return self.func("CONVERT_TZ", datetime, from_tz, to_tz, options)

        def if_sql(self, expression: exp.If) -> str:
            this = self.sql(expression, "this")
            true = self.sql(expression, "true")
            false = self.sql(expression, "false")
            return f"IF {this} THEN {true} ELSE {false} ENDIF"

        def dateadd_sql(self, expression: exp.DateAdd) -> str:
            unit = expression.text("unit").upper() or "DAY"
            func_name = self.DATE_ADD_FUNCTION_BY_UNIT.get(unit)
            if not func_name:
                self.unsupported(f"'{unit}' is not supported in Exasol.")
                return self.function_fallback_sql(expression)

            return self.func(func_name, expression.this, expression.expression)
