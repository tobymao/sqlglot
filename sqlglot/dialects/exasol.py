from __future__ import annotations
from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    rename_func,
    binary_from_function,
    build_formatted_time,
    timestrtotime_sql,
)
from sqlglot.helper import seq_get
from sqlglot.generator import unsupported_args
from sqlglot.tokens import TokenType


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
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "BIT_AND": binary_from_function(exp.BitwiseAnd),
            "BIT_OR": binary_from_function(exp.BitwiseOr),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BIT_LSHIFT": binary_from_function(exp.BitwiseLeftShift),
            "BIT_RSHIFT": binary_from_function(exp.BitwiseRightShift),
            "EVERY": lambda args: exp.All(this=seq_get(args, 0)),
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                position=seq_get(args, 3),
                occurrence=seq_get(args, 4),
            ),
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
            exp.DataType.Type.TEXT: "VARCHAR",
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
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/edit_distance.htm#EDIT_DISTANCE
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/mod.htm
            exp.Mod: rename_func("MOD"),
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
            exp.StrToTime: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.CurrentUser: lambda *_: "CURRENT_USER",
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TZ",
                e.this,
                "'UTC'",
                e.args.get("zone"),
            ),
        }

        def converttimezone_sql(self, expression: exp.ConvertTimezone) -> str:
            from_tz = expression.args.get("source_tz")
            to_tz = expression.args.get("target_tz")
            datetime = expression.args.get("timestamp")
            options = expression.args.get("options")

            return self.func("CONVERT_TZ", datetime, from_tz, to_tz, options)
