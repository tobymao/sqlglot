from __future__ import annotations

import datetime
import re
import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import (
    Dialect,
    build_date_delta,
    map_date_part,
)
from sqlglot.helper import seq_get
from sqlglot.parser import build_coalesce
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

# Unsupported options:
# - OPTIMIZE FOR ( @variable_name { UNKNOWN | = <literal_constant> } [ , ...n ] )
# - TABLE HINT
OPTIONS: parser.OPTIONS_TYPE = {
    **dict.fromkeys(
        (
            "DISABLE_OPTIMIZED_PLAN_FORCING",
            "FAST",
            "IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX",
            "LABEL",
            "MAXDOP",
            "MAXRECURSION",
            "MAX_GRANT_PERCENT",
            "MIN_GRANT_PERCENT",
            "NO_PERFORMANCE_SPOOL",
            "QUERYTRACEON",
            "RECOMPILE",
        ),
        tuple(),
    ),
    "CONCAT": ("UNION",),
    "DISABLE": ("EXTERNALPUSHDOWN", "SCALEOUTEXECUTION"),
    "EXPAND": ("VIEWS",),
    "FORCE": ("EXTERNALPUSHDOWN", "ORDER", "SCALEOUTEXECUTION"),
    "HASH": ("GROUP", "JOIN", "UNION"),
    "KEEP": ("PLAN",),
    "KEEPFIXED": ("PLAN",),
    "LOOP": ("JOIN",),
    "MERGE": ("JOIN", "UNION"),
    "OPTIMIZE": (("FOR", "UNKNOWN"),),
    "ORDER": ("GROUP",),
    "PARAMETERIZATION": ("FORCED", "SIMPLE"),
    "ROBUST": ("PLAN",),
    "USE": ("PLAN",),
}


XML_OPTIONS: parser.OPTIONS_TYPE = {
    **dict.fromkeys(
        (
            "AUTO",
            "EXPLICIT",
            "TYPE",
        ),
        tuple(),
    ),
    "ELEMENTS": (
        "XSINIL",
        "ABSENT",
    ),
    "BINARY": ("BASE64",),
}


OPTIONS_THAT_REQUIRE_EQUAL = ("MAX_GRANT_PERCENT", "MIN_GRANT_PERCENT", "LABEL")


def _build_formatted_time(
    exp_class: t.Type[E], full_format_mapping: t.Optional[bool] = None
) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        fmt = seq_get(args, 0)
        if isinstance(fmt, exp.Expr):
            from sqlglot.dialects.tsql import TSQL

            fmt = exp.Literal.string(
                format_time(
                    fmt.name.lower(),
                    (
                        {**TSQL.TIME_MAPPING, **FULL_FORMAT_TIME_MAPPING}
                        if full_format_mapping
                        else TSQL.TIME_MAPPING
                    ),
                )
            )

        this = seq_get(args, 1)
        if isinstance(this, exp.Expr):
            this = exp.cast(this, exp.DType.DATETIME2)

        return exp_class(this=this, format=fmt)

    return _builder


def _build_format(args: t.List) -> exp.NumberToStr | exp.TimeToStr:
    this = seq_get(args, 0)
    fmt = seq_get(args, 1)
    culture = seq_get(args, 2)

    number_fmt = fmt and (fmt.name in TRANSPILE_SAFE_NUMBER_FMT or not DATE_FMT_RE.search(fmt.name))

    if number_fmt:
        return exp.NumberToStr(this=this, format=fmt, culture=culture)

    if fmt:
        from sqlglot.dialects.tsql import TSQL

        fmt = exp.Literal.string(
            format_time(fmt.name, TSQL.FORMAT_TIME_MAPPING)
            if len(fmt.name) == 1
            else format_time(fmt.name, TSQL.TIME_MAPPING)
        )

    return exp.TimeToStr(this=this, format=fmt, culture=culture)


def _build_eomonth(args: t.List) -> exp.LastDay:
    date = exp.TsOrDsToDate(this=seq_get(args, 0))
    month_lag = seq_get(args, 1)

    if month_lag is None:
        this: exp.Expr = date
    else:
        unit = DATE_DELTA_INTERVAL.get("month")
        this = exp.DateAdd(this=date, expression=month_lag, unit=unit and exp.var(unit))

    return exp.LastDay(this=this)


def _build_hashbytes(args: t.List) -> exp.Expr:
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


def _build_date_delta(
    exp_class: t.Type[E], unit_mapping: t.Optional[t.Dict[str, str]] = None, big_int: bool = False
) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        unit = seq_get(args, 0)
        if unit and unit_mapping:
            unit = exp.var(unit_mapping.get(unit.name.lower(), unit.name))

        start_date = seq_get(args, 1)
        if start_date and start_date.is_number:
            # Numeric types are valid DATETIME values
            if start_date.is_int:
                adds = DEFAULT_START_DATE + datetime.timedelta(days=start_date.to_py())
                start_date = exp.Literal.string(adds.strftime("%F"))
            else:
                # We currently don't handle float values, i.e. they're not converted to equivalent DATETIMEs.
                # This is not a problem when generating T-SQL code, it is when transpiling to other dialects.
                return exp_class(
                    this=seq_get(args, 2), expression=start_date, unit=unit, big_int=big_int
                )

        return exp_class(
            this=exp.TimeStrToTime(this=seq_get(args, 2)),
            expression=exp.TimeStrToTime(this=start_date),
            unit=unit,
            big_int=big_int,
        )

    return _builder


# https://learn.microsoft.com/en-us/sql/t-sql/functions/datetimefromparts-transact-sql?view=sql-server-ver16#syntax
def _build_datetimefromparts(args: t.List) -> exp.TimestampFromParts:
    return exp.TimestampFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        hour=seq_get(args, 3),
        min=seq_get(args, 4),
        sec=seq_get(args, 5),
        milli=seq_get(args, 6),
    )


# https://learn.microsoft.com/en-us/sql/t-sql/functions/timefromparts-transact-sql?view=sql-server-ver16#syntax
def _build_timefromparts(args: t.List) -> exp.TimeFromParts:
    return exp.TimeFromParts(
        hour=seq_get(args, 0),
        min=seq_get(args, 1),
        sec=seq_get(args, 2),
        fractions=seq_get(args, 3),
        precision=seq_get(args, 4),
    )


def _build_with_arg_as_text(
    klass: t.Type[exp.Expr],
) -> t.Callable[[t.List[exp.Expr]], exp.Expr]:
    def _parse(args: t.List[exp.Expr]) -> exp.Expr:
        this = seq_get(args, 0)

        if this and not this.is_string:
            this = exp.cast(this, exp.DType.TEXT)

        expression = seq_get(args, 1)
        kwargs = {"this": this}

        if expression:
            kwargs["expression"] = expression

        return klass(**kwargs)

    return _parse


# https://learn.microsoft.com/en-us/sql/t-sql/functions/parsename-transact-sql?view=sql-server-ver16
def _build_parsename(args: t.List) -> exp.SplitPart | exp.Anonymous:
    # PARSENAME(...) will be stored into exp.SplitPart if:
    # - All args are literals
    # - The part index (2nd arg) is <= 4 (max valid value, otherwise TSQL returns NULL)
    if len(args) == 2 and all(isinstance(arg, exp.Literal) for arg in args):
        this = args[0]
        part_index = args[1]
        split_count = len(this.name.split("."))
        if split_count <= 4:
            return exp.SplitPart(
                this=this,
                delimiter=exp.Literal.string("."),
                part_index=exp.Literal.number(split_count + 1 - part_index.to_py()),
            )

    return exp.Anonymous(this="PARSENAME", expressions=args)


def _build_json_query(args: t.List, dialect: Dialect) -> exp.JSONExtract:
    if len(args) == 1:
        # The default value for path is '$'. As a result, if you don't provide a
        # value for path, JSON_QUERY returns the input expression.
        args.append(exp.Literal.string("$"))

    return parser.build_extract_json_with_path(exp.JSONExtract)(args, dialect)


def _build_datetrunc(args: t.List) -> exp.TimestampTrunc:
    unit = seq_get(args, 0)
    this = seq_get(args, 1)

    if this and this.is_string:
        this = exp.cast(this, exp.DType.DATETIME2)

    return exp.TimestampTrunc(this=this, unit=unit)


class TSQLParser(parser.Parser):
    SET_REQUIRES_ASSIGNMENT_DELIMITER = False
    LOG_DEFAULTS_TO_LN = True
    STRING_ALIASES = True
    NO_PAREN_IF_COMMANDS = False

    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.SESSION_USER: exp.SessionUser,
    }

    QUERY_MODIFIER_PARSERS = {
        **parser.Parser.QUERY_MODIFIER_PARSERS,
        TokenType.OPTION: lambda self: ("options", self._parse_options()),
        TokenType.FOR: lambda self: ("for_", self._parse_for()),
    }

    # T-SQL does not allow BEGIN to be used as an identifier
    ID_VAR_TOKENS = parser.Parser.ID_VAR_TOKENS - {TokenType.BEGIN}
    ALIAS_TOKENS = parser.Parser.ALIAS_TOKENS - {TokenType.BEGIN}
    TABLE_ALIAS_TOKENS = (parser.Parser.TABLE_ALIAS_TOKENS | {TokenType.ANTI, TokenType.SEMI}) - {
        TokenType.BEGIN
    }
    COMMENT_TABLE_ALIAS_TOKENS = parser.Parser.COMMENT_TABLE_ALIAS_TOKENS - {TokenType.BEGIN}
    UPDATE_ALIAS_TOKENS = parser.Parser.UPDATE_ALIAS_TOKENS - {TokenType.BEGIN}

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "ATN2": exp.Atan2.from_arg_list,
        "CHARINDEX": lambda args: exp.StrPosition(
            this=seq_get(args, 1),
            substr=seq_get(args, 0),
            position=seq_get(args, 2),
        ),
        "COUNT": lambda args: exp.Count(this=seq_get(args, 0), expressions=args[1:], big_int=False),
        "COUNT_BIG": lambda args: exp.Count(
            this=seq_get(args, 0), expressions=args[1:], big_int=True
        ),
        "DATEADD": build_date_delta(exp.DateAdd, unit_mapping=DATE_DELTA_INTERVAL),
        "DATEDIFF": _build_date_delta(exp.DateDiff, unit_mapping=DATE_DELTA_INTERVAL),
        "DATEDIFF_BIG": _build_date_delta(
            exp.DateDiff, unit_mapping=DATE_DELTA_INTERVAL, big_int=True
        ),
        "DATENAME": _build_formatted_time(exp.TimeToStr, full_format_mapping=True),
        "DATETIMEFROMPARTS": _build_datetimefromparts,
        "EOMONTH": _build_eomonth,
        "FORMAT": _build_format,
        "GETDATE": exp.CurrentTimestamp.from_arg_list,
        "HASHBYTES": _build_hashbytes,
        "ISNULL": lambda args: build_coalesce(args=args, is_null=True),
        "JSON_QUERY": _build_json_query,
        "JSON_VALUE": parser.build_extract_json_with_path(exp.JSONExtractScalar),
        "LEN": _build_with_arg_as_text(exp.Length),
        "LEFT": _build_with_arg_as_text(exp.Left),
        "NEWID": exp.Uuid.from_arg_list,
        "RIGHT": _build_with_arg_as_text(exp.Right),
        "PARSENAME": _build_parsename,
        "REPLICATE": exp.Repeat.from_arg_list,
        "SCHEMA_NAME": exp.CurrentSchema.from_arg_list,
        "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
        "SYSDATETIME": exp.CurrentTimestamp.from_arg_list,
        "SUSER_NAME": exp.CurrentUser.from_arg_list,
        "SUSER_SNAME": exp.CurrentUser.from_arg_list,
        "SYSDATETIMEOFFSET": exp.CurrentTimestampLTZ.from_arg_list,
        "SYSTEM_USER": exp.CurrentUser.from_arg_list,
        "TIMEFROMPARTS": _build_timefromparts,
        "DATETRUNC": _build_datetrunc,
    }

    JOIN_HINTS = {"LOOP", "HASH", "MERGE", "REMOTE"}

    PROCEDURE_OPTIONS = dict.fromkeys(
        ("ENCRYPTION", "RECOMPILE", "SCHEMABINDING", "NATIVE_COMPILATION", "EXECUTE"), tuple()
    )

    COLUMN_DEFINITION_MODES = {"OUT", "OUTPUT", "READONLY"}

    RETURNS_TABLE_TOKENS = parser.Parser.ID_VAR_TOKENS - {
        TokenType.TABLE,
        *parser.Parser.TYPE_TOKENS,
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.DECLARE: lambda self: self._parse_declare(),
        TokenType.EXECUTE: lambda self: self._parse_execute(),
    }

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.DCOLON: lambda self, this: self.expression(
            exp.ScopeResolution(
                this=this, expression=self._parse_function() or self._parse_var(any_token=True)
            )
        ),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
        "NEXT": lambda self: self._parse_next_value_for(),
    }

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        "JSON_ARRAYAGG": lambda self: self.expression(
            exp.JSONArrayAgg(
                this=self._parse_bitwise(),
                order=self._parse_order(),
                null_handling=self._parse_on_handling("NULL", "NULL", "ABSENT"),
            )
        ),
        "DATEPART": lambda self: self._parse_datepart(),
    }

    # The DCOLON (::) operator serves as a scope resolution (exp.ScopeResolution) operator in T-SQL
    COLUMN_OPERATORS = {
        **parser.Parser.COLUMN_OPERATORS,
        TokenType.DCOLON: lambda self, this, to: (
            self.expression(exp.Cast(this=this, to=to))
            if isinstance(to, exp.DataType) and to.this != exp.DType.USERDEFINED
            else self.expression(exp.ScopeResolution(this=this, expression=to))
        ),
    }

    SET_OP_MODIFIERS = {"offset"}

    ODBC_DATETIME_LITERALS = {
        "d": exp.Date,
        "t": exp.Time,
        "ts": exp.Timestamp,
    }

    def _parse_execute(self) -> exp.Execute:
        execute = self.expression(
            exp.Execute(
                this=self._parse_table(schema=True),
                expressions=self._parse_csv(self._parse_expression),
            )
        )

        if execute.name.lower() == "sp_executesql":
            execute = self.expression(exp.ExecuteSql(**execute.args))

        return execute

    def _parse_datepart(self) -> exp.Extract:
        this = self._parse_var(tokens=[TokenType.IDENTIFIER])
        expression = self._match(TokenType.COMMA) and self._parse_bitwise()
        name = map_date_part(this, self.dialect)

        return self.expression(exp.Extract(this=name, expression=expression))

    def _parse_alter_table_set(self) -> exp.AlterSet:
        return self._parse_wrapped(super()._parse_alter_table_set)

    def _parse_wrapped_select(self, table: bool = False) -> t.Optional[exp.Expr]:
        if self._match(TokenType.MERGE):
            comments = self._prev_comments
            merge = self._parse_merge()
            merge.add_comments(comments, prepend=True)
            return merge

        return super()._parse_wrapped_select(table=table)

    def _parse_dcolon(self) -> t.Optional[exp.Expr]:
        # We want to use _parse_types() if the first token after :: is a known type,
        # otherwise we could parse something like x::varchar(max) into a function
        if self._match_set(self.TYPE_TOKENS, advance=False):
            return self._parse_types()

        return self._parse_function() or self._parse_types()

    def _parse_options(self) -> t.Optional[t.List[exp.Expr]]:
        if not self._match(TokenType.OPTION):
            return None

        def _parse_option() -> t.Optional[exp.Expr]:
            option = self._parse_var_from_options(OPTIONS)
            if not option:
                return None

            self._match(TokenType.EQ)
            return self.expression(
                exp.QueryOption(this=option, expression=self._parse_primary_or_var())
            )

        return self._parse_wrapped_csv(_parse_option)

    def _parse_xml_key_value_option(self) -> exp.XMLKeyValueOption:
        this = self._parse_primary_or_var()
        if self._match(TokenType.L_PAREN, advance=False):
            expression = self._parse_wrapped(self._parse_string)
        else:
            expression = None

        return exp.XMLKeyValueOption(this=this, expression=expression)

    def _parse_for(self) -> t.Optional[t.List[exp.Expr]]:
        if not self._match_pair(TokenType.FOR, TokenType.XML):
            return None

        def _parse_for_xml() -> t.Optional[exp.Expr]:
            return self.expression(
                exp.QueryOption(
                    this=self._parse_var_from_options(XML_OPTIONS, raise_unmatched=False)
                    or self._parse_xml_key_value_option()
                )
            )

        return self._parse_csv(_parse_for_xml)

    def _parse_projections(
        self,
    ) -> t.Tuple[t.List[exp.Expr], t.Optional[t.List[exp.Expr]]]:
        """
        T-SQL supports the syntax alias = expression in the SELECT's projection list,
        so we transform all parsed Selects to convert their EQ projections into Aliases.

        See: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-clause-transact-sql?view=sql-server-ver16#syntax
        """
        projections, _ = super()._parse_projections()
        return [
            (
                exp.alias_(projection.expression, projection.this.this, copy=False)
                if isinstance(projection, exp.EQ) and isinstance(projection.this, exp.Column)
                else projection
            )
            for projection in projections
        ], None

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
            return self.expression(exp.Rollback(this=this))

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

        return self.expression(exp.Commit(this=this, durability=durability))

    def _parse_transaction(self) -> exp.Transaction | exp.Command:
        """Applies to SQL Server and Azure SQL Database
        BEGIN { TRAN | TRANSACTION }
        [ { transaction_name | @tran_name_variable }
        [ WITH MARK [ 'description' ] ]
        ]
        """
        if self._match_texts(("TRAN", "TRANSACTION")):
            transaction = self.expression(exp.Transaction(this=self._parse_id_var()))
            if self._match_text_seq("WITH", "MARK"):
                transaction.set("mark", self._parse_string())

            return transaction

        return self._parse_as_command(self._prev)

    def _parse_returns(self) -> exp.ReturnsProperty:
        table = self._parse_id_var(any_token=False, tokens=self.RETURNS_TABLE_TOKENS)
        returns = super()._parse_returns()
        returns.set("table", table)
        return returns

    def _parse_convert(self, strict: bool, safe: t.Optional[bool] = None) -> t.Optional[exp.Expr]:
        this = self._parse_types()
        self._match(TokenType.COMMA)
        args = [this, *self._parse_csv(self._parse_assignment)]
        convert = exp.Convert.from_arg_list(args)
        convert.set("safe", safe)
        return convert

    def _parse_column_def(
        self, this: t.Optional[exp.Expr], computed_column: bool = True
    ) -> t.Optional[exp.Expr]:
        this = super()._parse_column_def(this=this, computed_column=computed_column)
        if not this:
            return None
        if self._match(TokenType.EQ):
            this.set("default", self._parse_disjunction())
        if self._match_texts(self.COLUMN_DEFINITION_MODES):
            this.set("output", self._prev.text)
        return this

    def _parse_user_defined_function(
        self, kind: t.Optional[TokenType] = None
    ) -> t.Optional[exp.Expr]:
        this = super()._parse_user_defined_function(kind=kind)

        if kind == TokenType.FUNCTION or isinstance(this, exp.UserDefinedFunction):
            return this

        if kind == TokenType.PROCEDURE and this:
            expressions = this.expressions
            if not (
                expressions or self._match_set((TokenType.ALIAS, TokenType.WITH), advance=False)
            ):
                expressions = self._parse_csv(self._parse_function_parameter)

            return self.expression(
                exp.StoredProcedure(
                    this=this if isinstance(this, exp.Table) else this.this,
                    expressions=expressions,
                    wrapped=this.args.get("wrapped"),
                )
            )

        return self.expression(exp.UserDefinedFunction(this=this))

    def _parse_into(self) -> t.Optional[exp.Into]:
        into = super()._parse_into()

        table = isinstance(into, exp.Into) and into.find(exp.Table)
        if isinstance(table, exp.Table):
            table_identifier = table.this
            if table_identifier.args.get("temporary"):
                # Promote the temporary property from the Identifier to the Into expression
                t.cast(exp.Into, into).set("temporary", True)

        return into

    def _parse_id_var(
        self,
        any_token: bool = True,
        tokens: t.Optional[t.Collection[TokenType]] = None,
    ) -> t.Optional[exp.Expr]:
        is_temporary = self._match(TokenType.HASH)
        is_global = is_temporary and self._match(TokenType.HASH)

        this = super()._parse_id_var(any_token=any_token, tokens=tokens)
        if this:
            if is_global:
                this.set("global_", True)
            elif is_temporary:
                this.set("temporary", True)

        return this

    def _parse_create(self) -> exp.Create | exp.Command:
        create = super()._parse_create()

        if isinstance(create, exp.Create):
            table = create.this.this if isinstance(create.this, exp.Schema) else create.this
            if isinstance(table, exp.Table) and table.this and table.this.args.get("temporary"):
                if not create.args.get("properties"):
                    create.set("properties", exp.Properties(expressions=[]))

                create.args["properties"].append("expressions", exp.TemporaryProperty())

        return create

    def _parse_if(self) -> t.Optional[exp.Expr]:
        this = self._parse_condition()
        true = self._parse_block()

        false = self._match(TokenType.ELSE) and self._parse_block()

        return self.expression(exp.IfBlock(this=this, true=true, false=false))

    def _parse_unique(self) -> exp.UniqueColumnConstraint:
        if self._match_texts(("CLUSTERED", "NONCLUSTERED")):
            this = self.CONSTRAINT_PARSERS[self._prev.text.upper()](self)
        else:
            this = self._parse_schema(self._parse_id_var(any_token=False))

        return self.expression(exp.UniqueColumnConstraint(this=this))

    def _parse_update(self) -> exp.Update:
        expression = super()._parse_update()
        expression.set("options", self._parse_options())
        return expression

    def _parse_partition(self) -> t.Optional[exp.Partition]:
        if not self._match_text_seq("WITH", "(", "PARTITIONS"):
            return None

        def parse_range():
            low = self._parse_bitwise()
            high = self._parse_bitwise() if self._match_text_seq("TO") else None

            return self.expression(exp.PartitionRange(this=low, expression=high)) if high else low

        partition = self.expression(exp.Partition(expressions=self._parse_wrapped_csv(parse_range)))

        self._match_r_paren()

        return partition

    def _parse_alter_table_alter(self) -> t.Optional[exp.Expr]:
        expression = super()._parse_alter_table_alter()

        if expression is not None:
            collation = expression.args.get("collate")
            if isinstance(collation, exp.Column) and isinstance(collation.this, exp.Identifier):
                identifier = collation.this
                collation.set("this", exp.Var(this=identifier.name))

        return expression

    def _parse_primary_key_part(self) -> t.Optional[exp.Expr]:
        return self._parse_ordered()
