from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.helper import list_get
from sqlglot.time import format_time


class TSQL(Dialect):
    date_name_mapping = {
        "weekday": "%A",
        "dw": "%A",
        "w": "%A",
        "month": "%b",
        "mm": "%b",
        "m": "%b"
    }
    time_mapping = {
        "yyyy": "%Y",
        "yy": "%Y",
        "year": "%Y",
        "qq": "%q",
        "q": "%q",
        "quarter": "%q",
        "dayofyear": "%j",
        "day": "%d",
        "dy": "%d",
        "y": "%d",
        "week": "%W",
        "ww": "%W",
        "wk": "%W",
        "hour": "%h",
        "hh": "%h",
        "minute": "%M",
        "mi": "%M",
        "n": "%M",
        "second": "%S",
        "ss": "%S",
        "s": "%S",
        "millisecond": "%f",
        "ms": "%f",
        "weekday": "%W",
        "dw":"%W",
        "month":"%m",
        "mm":"%m",
        "m":"%m"
    }
    null_ordering = "nulls_are_small"
    time_format = "'yyyy-mm-dd hh:mm:ss'"

    class Tokenizer(Tokenizer):
        IDENTIFIERS = ['"', ("[", "]")]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "BIT": TokenType.BOOLEAN,
            "REAL": TokenType.FLOAT,
            "NTEXT": TokenType.TEXT,
            "SMALLDATETIME": TokenType.DATETIME,
            "DATETIME2": TokenType.DATETIME,
            "DATETIMEOFFSET": TokenType.TIMESTAMPTZ,
            "TIME": TokenType.TIMESTAMP,
            "VARBINARY": TokenType.BINARY,
            "IMAGE": TokenType.IMAGE,
            "MONEY": TokenType.MONEY,
            "SMALLMONEY": TokenType.SMALLMONEY,
            "ROWVERSION": TokenType.ROWVERSION,
            "UNIQUEIDENTIFIER": TokenType.UNIQUEIDENTIFIER,
            "XML": TokenType.XML,
            "SQL_VARIANT": TokenType.VARIANT,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "CHARINDEX": exp.StrPosition.from_arg_list,
            "DATENAME": lambda args: exp.TimeToStr(
                this=list_get(args, 1),
                format=exp.Literal(
                    this=format_time(list_get(args, 0).sql(), {**TSQL.time_mapping,**TSQL.date_name_mapping}),
                    is_string=True,
                ),
            ),
            "DATEPART": lambda args: exp.TimeToStr(
                this=list_get(args, 1),
                format=exp.Literal(
                    this=format_time(list_get(args, 0).sql(),TSQL.time_mapping),
                    is_string=True,
                ),
            ),
        }

        def _parse_convert(self):
            to = self._parse_types()
            self._match(TokenType.COMMA)
            this = self._parse_field()
            return self.expression(exp.Cast, this=this, to=to)

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
        }
