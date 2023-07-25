from __future__ import annotations

import re
import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    max_or_greatest,
    min_or_least,
    parse_date_delta,
    rename_func,
)
from sqlglot.expressions import DataType
from sqlglot.helper import seq_get
from sqlglot.time import format_time
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E

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


def _format_time_lambda(
    exp_class: t.Type[E], full_format_mapping: t.Optional[bool] = None
) -> t.Callable[[t.List], E]:
    def _format_time(args: t.List) -> E:
        assert len(args) == 2

        return exp_class(
            this=exp.cast(args[1], "datetime"),
            format=exp.Literal.string(
                format_time(
                    args[0].name.lower(),
                    {**TSQL.TIME_MAPPING, **FULL_FORMAT_TIME_MAPPING}
                    if full_format_mapping
                    else TSQL.TIME_MAPPING,
                )
            ),
        )

    return _format_time


def _parse_format(args: t.List) -> exp.Expression:
    assert len(args) == 2

    fmt = args[1]
    number_fmt = fmt.name in TRANSPILE_SAFE_NUMBER_FMT or not DATE_FMT_RE.search(fmt.name)

    if number_fmt:
        return exp.NumberToStr(this=args[0], format=fmt)

    return exp.TimeToStr(
        this=args[0],
        format=exp.Literal.string(
            format_time(fmt.name, TSQL.FORMAT_TIME_MAPPING)
            if len(fmt.name) == 1
            else format_time(fmt.name, TSQL.TIME_MAPPING)
        ),
    )


def _parse_eomonth(args: t.List) -> exp.Expression:
    date = seq_get(args, 0)
    month_lag = seq_get(args, 1)
    unit = DATE_DELTA_INTERVAL.get("month")

    if month_lag is None:
        return exp.LastDateOfMonth(this=date)

    # Remove month lag argument in parser as its compared with the number of arguments of the resulting class
    args.remove(month_lag)

    return exp.LastDateOfMonth(this=exp.DateAdd(this=date, expression=month_lag, unit=unit))


def _parse_hashbytes(args: t.List) -> exp.Expression:
    kind, data = args
    kind = kind.name.upper() if kind.is_string else ""

    if kind == "MD5":
        args.pop(0)
        return exp.MD5(this=data)
    if kind in ("SHA", "SHA1"):
        args.pop(0)
        return exp.SHA(this=data)
    if kind == "SHA2_256":
        return exp.SHA2(this=data, length=exp.Literal.number(256))
    if kind == "SHA2_512":
        return exp.SHA2(this=data, length=exp.Literal.number(512))

    return exp.func("HASHBYTES", *args)


def generate_date_delta_with_unit_sql(
    self: generator.Generator, expression: exp.DateAdd | exp.DateDiff
) -> str:
    func = "DATEADD" if isinstance(expression, exp.DateAdd) else "DATEDIFF"
    return self.func(func, expression.text("unit"), expression.expression, expression.this)


def _format_sql(self: generator.Generator, expression: exp.NumberToStr | exp.TimeToStr) -> str:
    fmt = (
        expression.args["format"]
        if isinstance(expression, exp.NumberToStr)
        else exp.Literal.string(
            format_time(
                expression.text("format"),
                t.cast(t.Dict[str, str], TSQL.INVERSE_TIME_MAPPING),
            )
        )
    )
    return self.func("FORMAT", expression.this, fmt)


def _string_agg_sql(self: generator.Generator, expression: exp.GroupConcat) -> str:
    expression = expression.copy()

    this = expression.this
    distinct = expression.find(exp.Distinct)
    if distinct:
        # exp.Distinct can appear below an exp.Order or an exp.GroupConcat expression
        self.unsupported("T-SQL STRING_AGG doesn't support DISTINCT.")
        this = distinct.pop().expressions[0]

    order = ""
    if isinstance(expression.this, exp.Order):
        if expression.this.this:
            this = expression.this.this.pop()
        order = f" WITHIN GROUP ({self.sql(expression.this)[1:]})"  # Order has a leading space

    separator = expression.args.get("separator") or exp.Literal.string(",")
    return f"STRING_AGG({self.format_args(this, separator)}){order}"


class TSQL(Dialect):
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = None
    NULL_ORDERING = "nulls_are_small"
    TIME_FORMAT = "'yyyy-mm-dd hh:mm:ss'"

    TIME_MAPPING = {
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

    CONVERT_FORMAT_MAPPING = {
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

    FORMAT_TIME_MAPPING = {
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
        HEX_STRINGS = [("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
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
            "OUTPUT": TokenType.RETURNING,
            "SYSTEM_USER": TokenType.CURRENT_USER,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "CHARINDEX": lambda args: exp.StrPosition(
                this=seq_get(args, 1),
                substr=seq_get(args, 0),
                position=seq_get(args, 2),
            ),
            "DATEADD": parse_date_delta(exp.DateAdd, unit_mapping=DATE_DELTA_INTERVAL),
            "DATEDIFF": parse_date_delta(exp.DateDiff, unit_mapping=DATE_DELTA_INTERVAL),
            "DATENAME": _format_time_lambda(exp.TimeToStr, full_format_mapping=True),
            "DATEPART": _format_time_lambda(exp.TimeToStr),
            "EOMONTH": _parse_eomonth,
            "FORMAT": _parse_format,
            "GETDATE": exp.CurrentTimestamp.from_arg_list,
            "HASHBYTES": _parse_hashbytes,
            "IIF": exp.If.from_arg_list,
            "ISNULL": exp.Coalesce.from_arg_list,
            "JSON_VALUE": exp.JSONExtractScalar.from_arg_list,
            "LEN": exp.Length.from_arg_list,
            "REPLICATE": exp.Repeat.from_arg_list,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "SYSDATETIME": exp.CurrentTimestamp.from_arg_list,
            "SUSER_NAME": exp.CurrentUser.from_arg_list,
            "SUSER_SNAME": exp.CurrentUser.from_arg_list,
            "SYSTEM_USER": exp.CurrentUser.from_arg_list,
        }

        JOIN_HINTS = {
            "LOOP",
            "HASH",
            "MERGE",
            "REMOTE",
        }

        VAR_LENGTH_DATATYPES = {
            DataType.Type.NVARCHAR,
            DataType.Type.VARCHAR,
            DataType.Type.CHAR,
            DataType.Type.NCHAR,
        }

        RETURNS_TABLE_TOKENS = parser.Parser.ID_VAR_TOKENS - {
            TokenType.TABLE,
            *parser.Parser.TYPE_TOKENS,
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.END: lambda self: self._parse_command(),
        }

        LOG_BASE_FIRST = False
        LOG_DEFAULTS_TO_LN = True

        CONCAT_NULL_OUTPUTS_STRING = True

        def _parse_commit_or_rollback(self) -> exp.Commit | exp.Rollback:
            """Applies to SQL Server and Azure SQL Database
            COMMIT [ { TRAN | TRANSACTION }
                [ transaction_name | @tran_name_variable ] ]
                [ WITH ( DELAYED_DURABILITY = { OFF | ON } ) ]

            ROLLBACK { TRAN | TRANSACTION }
                [ transaction_name | @tran_name_variable
                | savepoint_name | @savepoint_variable ]
            """
            rollback = self._prev.token_type == TokenType.ROLLBACK

            self._match_texts({"TRAN", "TRANSACTION"})
            this = self._parse_id_var()

            if rollback:
                return self.expression(exp.Rollback, this=this)

            durability = None
            if self._match_pair(TokenType.WITH, TokenType.L_PAREN):
                self._match_text_seq("DELAYED_DURABILITY")
                self._match(TokenType.EQ)

                if self._match_text_seq("OFF"):
                    durability = False
                else:
                    self._match(TokenType.ON)
                    durability = True

                self._match_r_paren()

            return self.expression(exp.Commit, this=this, durability=durability)

        def _parse_transaction(self) -> exp.Transaction | exp.Command:
            """Applies to SQL Server and Azure SQL Database
            BEGIN { TRAN | TRANSACTION }
            [ { transaction_name | @tran_name_variable }
            [ WITH MARK [ 'description' ] ]
            ]
            """
            if self._match_texts(("TRAN", "TRANSACTION")):
                transaction = self.expression(exp.Transaction, this=self._parse_id_var())
                if self._match_text_seq("WITH", "MARK"):
                    transaction.set("mark", self._parse_string())

                return transaction

            return self._parse_as_command(self._prev)

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

        def _parse_table_parts(self, schema: bool = False) -> exp.Table:
            table = super()._parse_table_parts(schema=schema)
            table.set("system_time", self._parse_system_time())
            return table

        def _parse_returns(self) -> exp.ReturnsProperty:
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

                if format_val_name not in TSQL.CONVERT_FORMAT_MAPPING:
                    raise ValueError(
                        f"CONVERT function at T-SQL does not support format style {format_val_name}"
                    )

                format_norm = exp.Literal.string(TSQL.CONVERT_FORMAT_MAPPING[format_val_name])

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

            expressions = self._parse_csv(self._parse_function_parameter)
            return self.expression(exp.UserDefinedFunction, this=this, expressions=expressions)

        def _parse_id_var(
            self,
            any_token: bool = True,
            tokens: t.Optional[t.Collection[TokenType]] = None,
        ) -> t.Optional[exp.Expression]:
            is_temporary = self._match(TokenType.HASH)
            is_global = is_temporary and self._match(TokenType.HASH)

            this = super()._parse_id_var(any_token=any_token, tokens=tokens)
            if this:
                if is_global:
                    this.set("global", True)
                elif is_temporary:
                    this.set("temporary", True)

            return this

        def _parse_create(self) -> exp.Create | exp.Command:
            create = super()._parse_create()

            if isinstance(create, exp.Create):
                table = create.this.this if isinstance(create.this, exp.Schema) else create.this
                if isinstance(table, exp.Table) and table.this.args.get("temporary"):
                    if not create.args.get("properties"):
                        create.set("properties", exp.Properties(expressions=[]))

                    create.args["properties"].append("expressions", exp.TemporaryProperty())

            return create

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        LIMIT_IS_TOP = True
        QUERY_HINTS = False
        RETURNING_END = False

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.DateAdd: generate_date_delta_with_unit_sql,
            exp.DateDiff: generate_date_delta_with_unit_sql,
            exp.CurrentDate: rename_func("GETDATE"),
            exp.CurrentTimestamp: rename_func("GETDATE"),
            exp.Extract: rename_func("DATEPART"),
            exp.GroupConcat: _string_agg_sql,
            exp.If: rename_func("IIF"),
            exp.Max: max_or_greatest,
            exp.MD5: lambda self, e: self.func("HASHBYTES", exp.Literal.string("MD5"), e.this),
            exp.Min: min_or_least,
            exp.NumberToStr: _format_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.SHA: lambda self, e: self.func("HASHBYTES", exp.Literal.string("SHA1"), e.this),
            exp.SHA2: lambda self, e: self.func(
                "HASHBYTES",
                exp.Literal.string(f"SHA2_{e.args.get('length', 256)}"),
                e.this,
            ),
            exp.TemporaryProperty: lambda self, e: "",
            exp.TimeToStr: _format_sql,
        }

        TRANSFORMS.pop(exp.ReturnsProperty)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        LIMIT_FETCH = "FETCH"

        def createable_sql(
            self,
            expression: exp.Create,
            locations: dict[exp.Properties.Location, list[exp.Property]],
        ) -> str:
            sql = self.sql(expression, "this")
            properties = expression.args.get("properties")

            if sql[:1] != "#" and any(
                isinstance(prop, exp.TemporaryProperty)
                for prop in (properties.expressions if properties else [])
            ):
                sql = f"#{sql}"

            return sql

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

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

        def returning_sql(self, expression: exp.Returning) -> str:
            into = self.sql(expression, "into")
            into = self.seg(f"INTO {into}") if into else ""
            return f"{self.seg('OUTPUT')} {self.expressions(expression, flat=True)}{into}"

        def transaction_sql(self, expression: exp.Transaction) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            mark = self.sql(expression, "mark")
            mark = f" WITH MARK {mark}" if mark else ""
            return f"BEGIN TRANSACTION{this}{mark}"

        def commit_sql(self, expression: exp.Commit) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            durability = expression.args.get("durability")
            durability = (
                f" WITH (DELAYED_DURABILITY = {'ON' if durability else 'OFF'})"
                if durability is not None
                else ""
            )
            return f"COMMIT TRANSACTION{this}{durability}"

        def rollback_sql(self, expression: exp.Rollback) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            return f"ROLLBACK TRANSACTION{this}"

        def identifier_sql(self, expression: exp.Identifier) -> str:
            identifier = super().identifier_sql(expression)

            if expression.args.get("global"):
                identifier = f"##{identifier}"
            elif expression.args.get("temporary"):
                identifier = f"#{identifier}"

            return identifier
