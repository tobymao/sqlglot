from sqlglot import expressions as exp
from sqlglot import parser, generator, tokens
from sqlglot.dialects.dialect import Dialect, build_formatted_time


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
        "YY": "%y",
        # month / day
        "MM": "%m",
        "MON": "%b",
        "MONTH": "%B",
        "DDD": "%j",
        "DD": "%d",
        "DY": "%a",
        "DAY": "%A",
        # hours / minutes / seconds
        "HH24": "%H",
        "HH12": "%I",
        "HH": "%I",  # 24- / 12-hour
        "MI": "%M",
        "SS": "%S",
        "FFF": "%f",
        "AMPM": "%p",
        # ISO week / century etc.
        "WW": "%W",
        "D": "%w",
        "CC": "%C",
        # timezone
        "TZD": "%Z",  # abbreviation  (UTC, PST, ...)
        "TZO": "%z",  # numeric offset (+0200)
    }

    class Parser(parser.Parser):
        LOG_DEFAULTS_TO_LN = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "TO_CHAR": build_formatted_time(exp.TimeToStr, "dremio"),
        }

    class Generator(generator.Generator):
        NVL2_SUPPORTED = False
        SUPPORTS_CONVERT_TIMEZONE = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        LIMIT_ONLY_LITERALS = True
        MULTI_ARG_DISTINCT = False

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
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
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
