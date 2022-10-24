from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, rename_func
from sqlglot.expressions import DataType
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.time import format_time
from sqlglot.tokens import Tokenizer, TokenType

FULL_FORMAT_TIME_MAPPING = {"weekday": "%A", "dw": "%A", "w": "%A", "month": "%B", "mm": "%B", "m": "%B"}
DATE_DELTA_INTERVAL = {
    "year": "year",
    "yyyy": "year",
    "yy": "year",
    "quarter": "quarter",
    "qq": "quarter",
    "q": "quarter",
    "month": "month",
    "mm": "month",
    "m": "month",
    "week": "week",
    "ww": "week",
    "wk": "week",
    "day": "day",
    "dd": "day",
    "d": "day",
}


def tsql_format_time_lambda(exp_class, full_format_mapping=None, default=None):
    def _format_time(args):
        return exp_class(
            this=list_get(args, 1),
            format=exp.Literal.string(
                format_time(
                    list_get(args, 0).name or (TSQL.time_format if default is True else default),
                    {**TSQL.time_mapping, **FULL_FORMAT_TIME_MAPPING} if full_format_mapping else TSQL.time_mapping,
                )
            ),
        )

    return _format_time


def parse_date_delta(exp_class):
    def inner_func(args):
        unit = DATE_DELTA_INTERVAL.get(list_get(args, 0).name.lower(), "day")
        return exp_class(this=list_get(args, 2), expression=list_get(args, 1), unit=unit)

    return inner_func


def generate_date_delta(self, e):
    func = "DATEADD" if isinstance(e, exp.DateAdd) else "DATEDIFF"
    return f"{func}({self.format_args(e.text('unit'), e.expression, e.this)})"


class TSQL(Dialect):
    null_ordering = "nulls_are_small"
    time_format = "'yyyy-mm-dd hh:mm:ss'"

    time_mapping = {
        "yyyy": "%Y",
        "yy": "%y",
        "year": "%Y",
        "qq": "%q",
        "q": "%q",
        "quarter": "%q",
        "dayofyear": "%j",
        "day": "%d",
        "dy": "%d",
        "y": "%Y",
        "week": "%W",
        "ww": "%W",
        "wk": "%W",
        "hour": "%h",
        "hh": "%I",
        "minute": "%M",
        "mi": "%M",
        "n": "%M",
        "second": "%S",
        "ss": "%S",
        "s": "%-S",
        "millisecond": "%f",
        "ms": "%f",
        "weekday": "%W",
        "dw": "%W",
        "month": "%m",
        "mm": "%M",
        "m": "%-M",
        "Y": "%Y",
        "YYYY": "%Y",
        "YY": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "h": "%-I",
        "S": "%f",
    }

    convert_format_mapping = {
        "0": "%b %d %Y %-I:%M%p",
        "1": "%m/%d/%y",
        "2": "%y.%m.%d",
        "3": "%d/%m/%y",
        "4": "%d.%m.%y",
        "5": "%d-%m-%y",
        "6": "%d %b %y",
        "7": "%b %d, %y",
        "8": "%H:%M:%S",
        "9": "%b %d %Y %-I:%M:%S:%f%p",
        "10": "mm-dd-yy",
        "11": "yy/mm/dd",
        "12": "yymmdd",
        "13": "%d %b %Y %H:%M:ss:%f",
        "14": "%H:%M:%S:%f",
        "20": "%Y-%m-%d %H:%M:%S",
        "21": "%Y-%m-%d %H:%M:%S.%f",
        "22": "%m/%d/%y %-I:%M:%S %p",
        "23": "%Y-%m-%d",
        "24": "%H:%M:%S",
        "25": "%Y-%m-%d %H:%M:%S.%f",
        "100": "%b %d %Y %-I:%M%p",
        "101": "%m/%d/%Y",
        "102": "%Y.%m.%d",
        "103": "%d/%m/%Y",
        "104": "%d.%m.%Y",
        "105": "%d-%m-%Y",
        "106": "%d %b %Y",
        "107": "%b %d, %Y",
        "108": "%H:%M:%S",
        "109": "%b %d %Y %-I:%M:%S:%f%p",
        "110": "%m-%d-%Y",
        "111": "%Y/%m/%d",
        "112": "%Y%m%d",
        "113": "%d %b %Y %H:%M:%S:%f",
        "114": "%H:%M:%S:%f",
        "120": "%Y-%m-%d %H:%M:%S",
        "121": "%Y-%m-%d %H:%M:%S.%f",
    }

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
            "NVARCHAR(MAX)": TokenType.TEXT,
            "VARCHAR(MAX)": TokenType.TEXT,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "CHARINDEX": exp.StrPosition.from_arg_list,
            "ISNULL": exp.Coalesce.from_arg_list,
            "DATEADD": parse_date_delta(exp.DateAdd),
            "DATEDIFF": parse_date_delta(exp.DateDiff),
            "DATENAME": tsql_format_time_lambda(exp.TimeToStr, full_format_mapping=True),
            "DATEPART": tsql_format_time_lambda(exp.TimeToStr),
            "GETDATE": exp.CurrentDate.from_arg_list,
            "IIF": exp.If.from_arg_list,
            "LEN": exp.Length.from_arg_list,
            "REPLICATE": exp.Repeat.from_arg_list,
            "JSON_VALUE": exp.JSONExtractScalar.from_arg_list,
        }

        VAR_LENGTH_DATATYPES = {
            DataType.Type.NVARCHAR,
            DataType.Type.VARCHAR,
            DataType.Type.CHAR,
            DataType.Type.NCHAR,
        }

        def _parse_convert(self, strict):
            to = self._parse_types()
            self._match(TokenType.COMMA)
            this = self._parse_field()

            # Retrieve length of datatype and override to default if not specified
            if list_get(to.expressions, 0) is None and to.this in self.VAR_LENGTH_DATATYPES:
                to = exp.DataType.build(to.this, expressions=[exp.Literal.number(30)], nested=False)

            # Check whether a conversion with format is applicable
            if self._match(TokenType.COMMA):
                format_val = self._parse_number().name
                if format_val not in TSQL.convert_format_mapping:
                    raise ValueError(f"CONVERT function at T-SQL does not support format style {format_val}")
                format_norm = exp.Literal.string(TSQL.convert_format_mapping[format_val])

                # Check whether the convert entails a string to date format
                if to.this == DataType.Type.DATE:
                    return self.expression(exp.StrToDate, this=this, format=format_norm)
                # Check whether the convert entails a string to datetime format
                elif to.this == DataType.Type.DATETIME:
                    return self.expression(exp.StrToTime, this=this, format=format_norm)
                # Check whether the convert entails a date to string format
                elif to.this in self.VAR_LENGTH_DATATYPES:
                    return self.expression(
                        exp.Cast if strict else exp.TryCast,
                        to=to,
                        this=self.expression(exp.TimeToStr, this=this, format=format_norm),
                    )
                elif to.this == DataType.Type.TEXT:
                    return self.expression(exp.TimeToStr, this=this, format=format_norm)

            # Entails a simple cast without any format requirement
            return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.DateAdd: lambda self, e: generate_date_delta(self, e),
            exp.DateDiff: lambda self, e: generate_date_delta(self, e),
            exp.CurrentDate: rename_func("GETDATE"),
            exp.If: rename_func("IIF"),
        }
