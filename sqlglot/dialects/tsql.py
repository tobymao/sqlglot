from __future__ import annotations

import re
import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect, parse_date_delta, rename_func
from sqlglot.expressions import DataType
from sqlglot.helper import seq_get
from sqlglot.time import format_time
from sqlglot.tokens import TokenType

FULL_FORMAT_TIME_MAPPING = {
    "weekday": "%A",
    "dw": "%A",
    "w": "%A",
    "month": "%B",
    "mm": "%B",
    "m": "%B",
}

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


DATE_FMT_RE = re.compile("([dD]{1,2})|([mM]{1,2})|([yY]{1,4})|([hH]{1,2})|([sS]{1,2})")

# N = Numeric, C=Currency
TRANSPILE_SAFE_NUMBER_FMT = {"N", "C"}


def _format_time_lambda(exp_class, full_format_mapping=None, default=None):
    def _format_time(args):
        return exp_class(
            this=seq_get(args, 1),
            format=exp.Literal.string(
                format_time(
                    seq_get(args, 0).name or (TSQL.time_format if default is True else default),
                    {**TSQL.time_mapping, **FULL_FORMAT_TIME_MAPPING}
                    if full_format_mapping
                    else TSQL.time_mapping,
                )
            ),
        )

    return _format_time


def _parse_format(args):
    fmt = seq_get(args, 1)
    number_fmt = fmt.name in TRANSPILE_SAFE_NUMBER_FMT or not DATE_FMT_RE.search(fmt.this)
    if number_fmt:
        return exp.NumberToStr(this=seq_get(args, 0), format=fmt)
    return exp.TimeToStr(
        this=seq_get(args, 0),
        format=exp.Literal.string(
            format_time(fmt.name, TSQL.format_time_mapping)
            if len(fmt.name) == 1
            else format_time(fmt.name, TSQL.time_mapping)
        ),
    )


def _parse_eomonth(args):
    date = seq_get(args, 0)
    month_lag = seq_get(args, 1)
    unit = DATE_DELTA_INTERVAL.get("month")

    if month_lag is None:
        return exp.LastDateOfMonth(this=date)

    # Remove month lag argument in parser as its compared with the number of arguments of the resulting class
    args.remove(month_lag)

    return exp.LastDateOfMonth(this=exp.DateAdd(this=date, expression=month_lag, unit=unit))


def generate_date_delta_with_unit_sql(self, e):
    func = "DATEADD" if isinstance(e, exp.DateAdd) else "DATEDIFF"
    return f"{func}({self.format_args(e.text('unit'), e.expression, e.this)})"


def _format_sql(self, e):
    fmt = (
        e.args["format"]
        if isinstance(e, exp.NumberToStr)
        else exp.Literal.string(format_time(e.text("format"), TSQL.inverse_time_mapping))
    )
    return f"FORMAT({self.format_args(e.this, fmt)})"


def _string_agg_sql(self, e):
    e = e.copy()

    this = e.this
    distinct = e.find(exp.Distinct)
    if distinct:
        # exp.Distinct can appear below an exp.Order or an exp.GroupConcat expression
        self.unsupported("T-SQL STRING_AGG doesn't support DISTINCT.")
        this = distinct.expressions[0]
        distinct.pop()

    order = ""
    if isinstance(e.this, exp.Order):
        if e.this.this:
            this = e.this.this
            e.this.this.pop()
        order = f" WITHIN GROUP ({self.sql(e.this)[1:]})"  # Order has a leading space

    separator = e.args.get("separator") or exp.Literal.string(",")
    return f"STRING_AGG({self.format_args(this, separator)}){order}"


class TSQL(Dialect):
    null_ordering = "nulls_are_small"
    time_format = "'yyyy-mm-dd hh:mm:ss'"

    time_mapping = {
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
        "yyyy": "%Y",
        "yy": "%y",
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
    # not sure if complete
    format_time_mapping = {
        "y": "%B %Y",
        "d": "%m/%d/%Y",
        "H": "%-H",
        "h": "%-I",
        "s": "%Y-%m-%d %H:%M:%S",
        "D": "%A,%B,%Y",
        "f": "%A,%B,%Y %-I:%M %p",
        "F": "%A,%B,%Y %-I:%M:%S %p",
        "g": "%m/%d/%Y %-I:%M %p",
        "G": "%m/%d/%Y %-I:%M:%S %p",
        "M": "%B %-d",
        "m": "%B %-d",
        "O": "%Y-%m-%dT%H:%M:%S",
        "u": "%Y-%M-%D %H:%M:%S%z",
        "U": "%A, %B %D, %Y %H:%M:%S%z",
        "T": "%-I:%M:%S %p",
        "t": "%-I:%M",
        "Y": "%a %Y",
    }

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]")]

        QUOTES = ["'", '"']

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BIT": TokenType.BOOLEAN,
            "DATETIME2": TokenType.DATETIME,
            "DATETIMEOFFSET": TokenType.TIMESTAMPTZ,
            "DECLARE": TokenType.COMMAND,
            "IMAGE": TokenType.IMAGE,
            "MONEY": TokenType.MONEY,
            "NTEXT": TokenType.TEXT,
            "NVARCHAR(MAX)": TokenType.TEXT,
            "PRINT": TokenType.COMMAND,
            "PROC": TokenType.PROCEDURE,
            "REAL": TokenType.FLOAT,
            "ROWVERSION": TokenType.ROWVERSION,
            "SMALLDATETIME": TokenType.DATETIME,
            "SMALLMONEY": TokenType.SMALLMONEY,
            "SQL_VARIANT": TokenType.VARIANT,
            "TIME": TokenType.TIMESTAMP,
            "TOP": TokenType.TOP,
            "UNIQUEIDENTIFIER": TokenType.UNIQUEIDENTIFIER,
            "VARCHAR(MAX)": TokenType.TEXT,
            "XML": TokenType.XML,
        }

        # TSQL allows @, # to appear as a variable/identifier prefix
        SINGLE_TOKENS = tokens.Tokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("@")
        SINGLE_TOKENS.pop("#")

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "CHARINDEX": lambda args: exp.StrPosition(
                this=seq_get(args, 1),
                substr=seq_get(args, 0),
                position=seq_get(args, 2),
            ),
            "ISNULL": exp.Coalesce.from_arg_list,
            "DATEADD": parse_date_delta(exp.DateAdd, unit_mapping=DATE_DELTA_INTERVAL),
            "DATEDIFF": parse_date_delta(exp.DateDiff, unit_mapping=DATE_DELTA_INTERVAL),
            "DATENAME": _format_time_lambda(exp.TimeToStr, full_format_mapping=True),
            "DATEPART": _format_time_lambda(exp.TimeToStr),
            "GETDATE": exp.CurrentTimestamp.from_arg_list,
            "SYSDATETIME": exp.CurrentTimestamp.from_arg_list,
            "IIF": exp.If.from_arg_list,
            "LEN": exp.Length.from_arg_list,
            "REPLICATE": exp.Repeat.from_arg_list,
            "JSON_VALUE": exp.JSONExtractScalar.from_arg_list,
            "FORMAT": _parse_format,
            "EOMONTH": _parse_eomonth,
        }

        VAR_LENGTH_DATATYPES = {
            DataType.Type.NVARCHAR,
            DataType.Type.VARCHAR,
            DataType.Type.CHAR,
            DataType.Type.NCHAR,
        }

        RETURNS_TABLE_TOKENS = parser.Parser.ID_VAR_TOKENS - {  # type: ignore
            TokenType.TABLE,
            *parser.Parser.TYPE_TOKENS,  # type: ignore
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,  # type: ignore
            TokenType.END: lambda self: self._parse_command(),
        }

        def _parse_system_time(self) -> t.Optional[exp.Expression]:
            if not self._match_text_seq("FOR", "SYSTEM_TIME"):
                return None

            if self._match_text_seq("AS", "OF"):
                system_time = self.expression(
                    exp.SystemTime, this=self._parse_bitwise(), kind="AS OF"
                )
            elif self._match_set((TokenType.FROM, TokenType.BETWEEN)):
                kind = self._prev.text
                this = self._parse_bitwise()
                self._match_texts(("TO", "AND"))
                expression = self._parse_bitwise()
                system_time = self.expression(
                    exp.SystemTime, this=this, expression=expression, kind=kind
                )
            elif self._match_text_seq("CONTAINED", "IN"):
                args = self._parse_wrapped_csv(self._parse_bitwise)
                system_time = self.expression(
                    exp.SystemTime,
                    this=seq_get(args, 0),
                    expression=seq_get(args, 1),
                    kind="CONTAINED IN",
                )
            elif self._match(TokenType.ALL):
                system_time = self.expression(exp.SystemTime, kind="ALL")
            else:
                system_time = None
                self.raise_error("Unable to parse FOR SYSTEM_TIME clause")

            return system_time

        def _parse_table_parts(self, schema: bool = False) -> exp.Expression:
            table = super()._parse_table_parts(schema=schema)
            table.set("system_time", self._parse_system_time())
            return table

        def _parse_returns(self) -> exp.Expression:
            table = self._parse_id_var(any_token=False, tokens=self.RETURNS_TABLE_TOKENS)
            returns = super()._parse_returns()
            returns.set("table", table)
            return returns

        def _parse_convert(self, strict: bool) -> t.Optional[exp.Expression]:
            to = self._parse_types()
            self._match(TokenType.COMMA)
            this = self._parse_conjunction()

            if not to or not this:
                return None

            # Retrieve length of datatype and override to default if not specified
            if seq_get(to.expressions, 0) is None and to.this in self.VAR_LENGTH_DATATYPES:
                to = exp.DataType.build(to.this, expressions=[exp.Literal.number(30)], nested=False)

            # Check whether a conversion with format is applicable
            if self._match(TokenType.COMMA):
                format_val = self._parse_number()
                format_val_name = format_val.name if format_val else ""

                if format_val_name not in TSQL.convert_format_mapping:
                    raise ValueError(
                        f"CONVERT function at T-SQL does not support format style {format_val_name}"
                    )

                format_norm = exp.Literal.string(TSQL.convert_format_mapping[format_val_name])

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

        def _parse_user_defined_function(
            self, kind: t.Optional[TokenType] = None
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_user_defined_function(kind=kind)

            if (
                kind == TokenType.FUNCTION
                or isinstance(this, exp.UserDefinedFunction)
                or self._match(TokenType.ALIAS, advance=False)
            ):
                return this

            expressions = self._parse_csv(self._parse_udf_kwarg)
            return self.expression(exp.UserDefinedFunction, this=this, expressions=expressions)

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.DateAdd: generate_date_delta_with_unit_sql,
            exp.DateDiff: generate_date_delta_with_unit_sql,
            exp.CurrentDate: rename_func("GETDATE"),
            exp.CurrentTimestamp: rename_func("GETDATE"),
            exp.If: rename_func("IIF"),
            exp.NumberToStr: _format_sql,
            exp.TimeToStr: _format_sql,
            exp.GroupConcat: _string_agg_sql,
        }

        TRANSFORMS.pop(exp.ReturnsProperty)

        def systemtime_sql(self, expression: exp.SystemTime) -> str:
            kind = expression.args["kind"]
            if kind == "ALL":
                return "FOR SYSTEM_TIME ALL"

            start = self.sql(expression, "this")
            if kind == "AS OF":
                return f"FOR SYSTEM_TIME AS OF {start}"

            end = self.sql(expression, "expression")
            if kind == "FROM":
                return f"FOR SYSTEM_TIME FROM {start} TO {end}"
            if kind == "BETWEEN":
                return f"FOR SYSTEM_TIME BETWEEN {start} AND {end}"

            return f"FOR SYSTEM_TIME CONTAINED IN ({start}, {end})"

        def returnsproperty_sql(self, expression: exp.ReturnsProperty) -> str:
            table = expression.args.get("table")
            table = f"{table} " if table else ""
            return f"RETURNS {table}{self.sql(expression, 'this')}"
