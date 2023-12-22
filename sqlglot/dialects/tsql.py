from __future__ import annotations

import datetime
import re
import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    any_value_to_max_sql,
    date_delta_sql,
    generatedasidentitycolumnconstraint_sql,
    max_or_greatest,
    min_or_least,
    parse_date_delta,
    rename_func,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
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

DEFAULT_START_DATE = datetime.date(1900, 1, 1)

BIT_TYPES = {exp.EQ, exp.NEQ, exp.Is, exp.In, exp.Select, exp.Alias}


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
    this = seq_get(args, 0)
    fmt = seq_get(args, 1)
    culture = seq_get(args, 2)

    number_fmt = fmt and (fmt.name in TRANSPILE_SAFE_NUMBER_FMT or not DATE_FMT_RE.search(fmt.name))

    if number_fmt:
        return exp.NumberToStr(this=this, format=fmt, culture=culture)

    if fmt:
        fmt = exp.Literal.string(
            format_time(fmt.name, TSQL.FORMAT_TIME_MAPPING)
            if len(fmt.name) == 1
            else format_time(fmt.name, TSQL.TIME_MAPPING)
        )

    return exp.TimeToStr(this=this, format=fmt, culture=culture)


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


DATEPART_ONLY_FORMATS = {"DW", "HOUR", "QUARTER"}


def _format_sql(self: TSQL.Generator, expression: exp.NumberToStr | exp.TimeToStr) -> str:
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

    # There is no format for "quarter"
    name = fmt.name.upper()
    if name in DATEPART_ONLY_FORMATS:
        return self.func("DATEPART", name, expression.this)

    return self.func("FORMAT", expression.this, fmt, expression.args.get("culture"))


def _string_agg_sql(self: TSQL.Generator, expression: exp.GroupConcat) -> str:
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


def _parse_date_delta(
    exp_class: t.Type[E], unit_mapping: t.Optional[t.Dict[str, str]] = None
) -> t.Callable[[t.List], E]:
    def inner_func(args: t.List) -> E:
        unit = seq_get(args, 0)
        if unit and unit_mapping:
            unit = exp.var(unit_mapping.get(unit.name.lower(), unit.name))

        start_date = seq_get(args, 1)
        if start_date and start_date.is_number:
            # Numeric types are valid DATETIME values
            if start_date.is_int:
                adds = DEFAULT_START_DATE + datetime.timedelta(days=int(start_date.this))
                start_date = exp.Literal.string(adds.strftime("%F"))
            else:
                # We currently don't handle float values, i.e. they're not converted to equivalent DATETIMEs.
                # This is not a problem when generating T-SQL code, it is when transpiling to other dialects.
                return exp_class(this=seq_get(args, 2), expression=start_date, unit=unit)

        return exp_class(
            this=exp.TimeStrToTime(this=seq_get(args, 2)),
            expression=exp.TimeStrToTime(this=start_date),
            unit=unit,
        )

    return inner_func


def qualify_derived_table_outputs(expression: exp.Expression) -> exp.Expression:
    """Ensures all (unnamed) output columns are aliased for CTEs and Subqueries."""
    alias = expression.args.get("alias")

    if (
        isinstance(expression, (exp.CTE, exp.Subquery))
        and isinstance(alias, exp.TableAlias)
        and not alias.columns
    ):
        from sqlglot.optimizer.qualify_columns import qualify_outputs

        # We keep track of the unaliased column projection indexes instead of the expressions
        # themselves, because the latter are going to be replaced by new nodes when the aliases
        # are added and hence we won't be able to reach these newly added Alias parents
        subqueryable = expression.this
        unaliased_column_indexes = (
            i
            for i, c in enumerate(subqueryable.selects)
            if isinstance(c, exp.Column) and not c.alias
        )

        qualify_outputs(subqueryable)

        # Preserve the quoting information of columns for newly added Alias nodes
        subqueryable_selects = subqueryable.selects
        for select_index in unaliased_column_indexes:
            alias = subqueryable_selects[select_index]
            column = alias.this
            if isinstance(column.this, exp.Identifier):
                alias.args["alias"].set("quoted", column.this.quoted)

    return expression


class TSQL(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    TIME_FORMAT = "'yyyy-mm-dd hh:mm:ss'"
    SUPPORTS_SEMI_ANTI_JOIN = False
    LOG_BASE_FIRST = False
    TYPED_DIVISION = True
    CONCAT_COALESCE = True

    TIME_MAPPING = {
        "year": "%Y",
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
        "dddd": "%A",
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
        VAR_SINGLE_TOKENS = {"@", "$", "#"}

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
            "TOP": TokenType.TOP,
            "UNIQUEIDENTIFIER": TokenType.UNIQUEIDENTIFIER,
            "UPDATE STATISTICS": TokenType.COMMAND,
            "VARCHAR(MAX)": TokenType.TEXT,
            "XML": TokenType.XML,
            "OUTPUT": TokenType.RETURNING,
            "SYSTEM_USER": TokenType.CURRENT_USER,
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
        }

    class Parser(parser.Parser):
        SET_REQUIRES_ASSIGNMENT_DELIMITER = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "CHARINDEX": lambda args: exp.StrPosition(
                this=seq_get(args, 1),
                substr=seq_get(args, 0),
                position=seq_get(args, 2),
            ),
            "DATEADD": parse_date_delta(exp.DateAdd, unit_mapping=DATE_DELTA_INTERVAL),
            "DATEDIFF": _parse_date_delta(exp.DateDiff, unit_mapping=DATE_DELTA_INTERVAL),
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

        LOG_DEFAULTS_TO_LN = True

        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False

        def _parse_projections(self) -> t.List[exp.Expression]:
            """
            T-SQL supports the syntax alias = expression in the SELECT's projection list,
            so we transform all parsed Selects to convert their EQ projections into Aliases.

            See: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-clause-transact-sql?view=sql-server-ver16#syntax
            """
            return [
                exp.alias_(projection.expression, projection.this.this, copy=False)
                if isinstance(projection, exp.EQ) and isinstance(projection.this, exp.Column)
                else projection
                for projection in super()._parse_projections()
            ]

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

            self._match_texts(("TRAN", "TRANSACTION"))
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

        def _parse_returns(self) -> exp.ReturnsProperty:
            table = self._parse_id_var(any_token=False, tokens=self.RETURNS_TABLE_TOKENS)
            returns = super()._parse_returns()
            returns.set("table", table)
            return returns

        def _parse_convert(
            self, strict: bool, safe: t.Optional[bool] = None
        ) -> t.Optional[exp.Expression]:
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
                        safe=safe,
                    )
                elif to.this == DataType.Type.TEXT:
                    return self.expression(exp.TimeToStr, this=this, format=format_norm)

            # Entails a simple cast without any format requirement
            return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to, safe=safe)

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

        def _parse_if(self) -> t.Optional[exp.Expression]:
            index = self._index

            if self._match_text_seq("OBJECT_ID"):
                self._parse_wrapped_csv(self._parse_string)
                if self._match_text_seq("IS", "NOT", "NULL") and self._match(TokenType.DROP):
                    return self._parse_drop(exists=True)
                self._retreat(index)

            return super()._parse_if()

        def _parse_unique(self) -> exp.UniqueColumnConstraint:
            if self._match_texts(("CLUSTERED", "NONCLUSTERED")):
                this = self.CONSTRAINT_PARSERS[self._prev.text.upper()](self)
            else:
                this = self._parse_schema(self._parse_id_var(any_token=False))

            return self.expression(exp.UniqueColumnConstraint, this=this)

    class Generator(generator.Generator):
        LIMIT_IS_TOP = True
        QUERY_HINTS = False
        RETURNING_END = False
        NVL2_SUPPORTED = False
        ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = False
        LIMIT_FETCH = "FETCH"
        COMPUTED_COLUMN_WITH_TYPE = False
        CTE_RECURSIVE_KEYWORD_REQUIRED = False
        ENSURE_BOOLS = True
        NULL_ORDERING_SUPPORTED = False
        SUPPORTS_SINGLE_ARG_CONCAT = False

        EXPRESSIONS_WITHOUT_NESTED_CTES = {
            exp.Delete,
            exp.Insert,
            exp.Merge,
            exp.Select,
            exp.Subquery,
            exp.Union,
            exp.Update,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.DOUBLE: "FLOAT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.TEXT: "VARCHAR(MAX)",
            exp.DataType.Type.TIMESTAMP: "DATETIME2",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIMEOFFSET",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: any_value_to_max_sql,
            exp.AutoIncrementColumnConstraint: lambda *_: "IDENTITY",
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.CTE: transforms.preprocess([qualify_derived_table_outputs]),
            exp.CurrentDate: rename_func("GETDATE"),
            exp.CurrentTimestamp: rename_func("GETDATE"),
            exp.Extract: rename_func("DATEPART"),
            exp.GeneratedAsIdentityColumnConstraint: generatedasidentitycolumnconstraint_sql,
            exp.GroupConcat: _string_agg_sql,
            exp.If: rename_func("IIF"),
            exp.Length: rename_func("LEN"),
            exp.Max: max_or_greatest,
            exp.MD5: lambda self, e: self.func("HASHBYTES", exp.Literal.string("MD5"), e.this),
            exp.Min: min_or_least,
            exp.NumberToStr: _format_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.eliminate_semi_and_anti_joins,
                    transforms.eliminate_qualify,
                ]
            ),
            exp.Subquery: transforms.preprocess([qualify_derived_table_outputs]),
            exp.SHA: lambda self, e: self.func("HASHBYTES", exp.Literal.string("SHA1"), e.this),
            exp.SHA2: lambda self, e: self.func(
                "HASHBYTES", exp.Literal.string(f"SHA2_{e.args.get('length', 256)}"), e.this
            ),
            exp.TemporaryProperty: lambda self, e: "",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToStr: _format_sql,
            exp.TsOrDsAdd: date_delta_sql("DATEADD", cast=True),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("tsql"),
        }

        TRANSFORMS.pop(exp.ReturnsProperty)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def set_operation(self, expression: exp.Union, op: str) -> str:
            limit = expression.args.get("limit")
            if limit:
                return self.sql(expression.limit(limit.pop(), copy=False))

            return super().set_operation(expression, op)

        def setitem_sql(self, expression: exp.SetItem) -> str:
            this = expression.this
            if isinstance(this, exp.EQ) and not isinstance(this.left, exp.Parameter):
                # T-SQL does not use '=' in SET command, except when the LHS is a variable.
                return f"{self.sql(this.left)} {self.sql(this.right)}"

            return super().setitem_sql(expression)

        def boolean_sql(self, expression: exp.Boolean) -> str:
            if type(expression.parent) in BIT_TYPES:
                return "1" if expression.this else "0"

            return "(1 = 1)" if expression.this else "(1 = 0)"

        def is_sql(self, expression: exp.Is) -> str:
            if isinstance(expression.expression, exp.Boolean):
                return self.binary(expression, "=")
            return self.binary(expression, "IS")

        def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
            sql = self.sql(expression, "this")
            properties = expression.args.get("properties")

            if sql[:1] != "#" and any(
                isinstance(prop, exp.TemporaryProperty)
                for prop in (properties.expressions if properties else [])
            ):
                sql = f"#{sql}"

            return sql

        def create_sql(self, expression: exp.Create) -> str:
            kind = self.sql(expression, "kind").upper()
            exists = expression.args.pop("exists", None)
            sql = super().create_sql(expression)

            table = expression.find(exp.Table)

            # Convert CTAS statement to SELECT .. INTO ..
            if kind == "TABLE" and expression.expression:
                ctas_with = expression.expression.args.get("with")
                if ctas_with:
                    ctas_with = ctas_with.pop()

                subquery = expression.expression
                if isinstance(subquery, exp.Subqueryable):
                    subquery = subquery.subquery()

                select_into = exp.select("*").from_(exp.alias_(subquery, "temp", table=True))
                select_into.set("into", exp.Into(this=table))
                select_into.set("with", ctas_with)

                sql = self.sql(select_into)

            if exists:
                identifier = self.sql(exp.Literal.string(exp.table_name(table) if table else ""))
                sql = self.sql(exp.Literal.string(sql))
                if kind == "SCHEMA":
                    sql = f"""IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = {identifier}) EXEC({sql})"""
                elif kind == "TABLE":
                    assert table
                    where = exp.and_(
                        exp.column("table_name").eq(table.name),
                        exp.column("table_schema").eq(table.db) if table.db else None,
                        exp.column("table_catalog").eq(table.catalog) if table.catalog else None,
                    )
                    sql = f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE {where}) EXEC({sql})"""
                elif kind == "INDEX":
                    index = self.sql(exp.Literal.string(expression.this.text("this")))
                    sql = f"""IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id({identifier}) AND name = {index}) EXEC({sql})"""
            elif expression.args.get("replace"):
                sql = sql.replace("CREATE OR REPLACE ", "CREATE OR ALTER ", 1)

            return self.prepend_ctes(expression, sql)

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def version_sql(self, expression: exp.Version) -> str:
            name = "SYSTEM_TIME" if expression.name == "TIMESTAMP" else expression.name
            this = f"FOR {name}"
            expr = expression.expression
            kind = expression.text("kind")
            if kind in ("FROM", "BETWEEN"):
                args = expr.expressions
                sep = "TO" if kind == "FROM" else "AND"
                expr_sql = f"{self.sql(seq_get(args, 0))} {sep} {self.sql(seq_get(args, 1))}"
            else:
                expr_sql = self.sql(expr)

            expr_sql = f" {expr_sql}" if expr_sql else ""
            return f"{this} {kind}{expr_sql}"

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

        def constraint_sql(self, expression: exp.Constraint) -> str:
            this = self.sql(expression, "this")
            expressions = self.expressions(expression, flat=True, sep=" ")
            return f"CONSTRAINT {this} {expressions}"
