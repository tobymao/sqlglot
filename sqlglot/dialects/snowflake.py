from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    binary_from_function,
    date_delta_sql,
    date_trunc_to_time,
    datestrtodate_sql,
    build_formatted_time,
    if_sql,
    inline_array_sql,
    max_or_greatest,
    min_or_least,
    rename_func,
    timestamptrunc_sql,
    timestrtotime_sql,
    var_map_sql,
)
from sqlglot.expressions import Literal
from sqlglot.helper import flatten, is_int, seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


# from https://docs.snowflake.com/en/sql-reference/functions/to_timestamp.html
def _build_to_timestamp(args: t.List) -> t.Union[exp.StrToTime, exp.UnixToTime, exp.TimeStrToTime]:
    if len(args) == 2:
        first_arg, second_arg = args
        if second_arg.is_string:
            # case: <string_expr> [ , <format> ]
            return build_formatted_time(exp.StrToTime, "snowflake")(args)
        return exp.UnixToTime(this=first_arg, scale=second_arg)

    from sqlglot.optimizer.simplify import simplify_literals

    # The first argument might be an expression like 40 * 365 * 86400, so we try to
    # reduce it using `simplify_literals` first and then check if it's a Literal.
    first_arg = seq_get(args, 0)
    if not isinstance(simplify_literals(first_arg, root=True), Literal):
        # case: <variant_expr> or other expressions such as columns
        return exp.TimeStrToTime.from_arg_list(args)

    if first_arg.is_string:
        if is_int(first_arg.this):
            # case: <integer>
            return exp.UnixToTime.from_arg_list(args)

        # case: <date_expr>
        return build_formatted_time(exp.StrToTime, "snowflake", default=True)(args)

    # case: <numeric_expr>
    return exp.UnixToTime.from_arg_list(args)


def _build_object_construct(args: t.List) -> t.Union[exp.StarMap, exp.Struct]:
    expression = parser.build_var_map(args)

    if isinstance(expression, exp.StarMap):
        return expression

    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=k, expression=v) for k, v in zip(expression.keys, expression.values)
        ]
    )


def _build_datediff(args: t.List) -> exp.DateDiff:
    return exp.DateDiff(
        this=seq_get(args, 2), expression=seq_get(args, 1), unit=_map_date_part(seq_get(args, 0))
    )


# https://docs.snowflake.com/en/sql-reference/functions/div0
def _build_if_from_div0(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 1), expression=exp.Literal.number(0))
    true = exp.Literal.number(0)
    false = exp.Div(this=seq_get(args, 0), expression=seq_get(args, 1))
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


def _datatype_sql(self: Snowflake.Generator, expression: exp.DataType) -> str:
    if expression.is_type("array"):
        return "ARRAY"
    elif expression.is_type("map"):
        return "OBJECT"
    return self.datatype_sql(expression)


def _regexpilike_sql(self: Snowflake.Generator, expression: exp.RegexpILike) -> str:
    flag = expression.text("flag")

    if "i" not in flag:
        flag += "i"

    return self.func(
        "REGEXP_LIKE", expression.this, expression.expression, exp.Literal.string(flag)
    )


def _build_convert_timezone(args: t.List) -> t.Union[exp.Anonymous, exp.AtTimeZone]:
    if len(args) == 3:
        return exp.Anonymous(this="CONVERT_TIMEZONE", expressions=args)
    return exp.AtTimeZone(this=seq_get(args, 1), zone=seq_get(args, 0))


def _build_regexp_replace(args: t.List) -> exp.RegexpReplace:
    regexp_replace = exp.RegexpReplace.from_arg_list(args)

    if not regexp_replace.args.get("replacement"):
        regexp_replace.set("replacement", exp.Literal.string(""))

    return regexp_replace


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[Snowflake.Parser], exp.Show]:
    def _parse(self: Snowflake.Parser) -> exp.Show:
        return self._parse_show_snowflake(*args, **kwargs)

    return _parse


DATE_PART_MAPPING = {
    "Y": "YEAR",
    "YY": "YEAR",
    "YYY": "YEAR",
    "YYYY": "YEAR",
    "YR": "YEAR",
    "YEARS": "YEAR",
    "YRS": "YEAR",
    "MM": "MONTH",
    "MON": "MONTH",
    "MONS": "MONTH",
    "MONTHS": "MONTH",
    "D": "DAY",
    "DD": "DAY",
    "DAYS": "DAY",
    "DAYOFMONTH": "DAY",
    "WEEKDAY": "DAYOFWEEK",
    "DOW": "DAYOFWEEK",
    "DW": "DAYOFWEEK",
    "WEEKDAY_ISO": "DAYOFWEEKISO",
    "DOW_ISO": "DAYOFWEEKISO",
    "DW_ISO": "DAYOFWEEKISO",
    "YEARDAY": "DAYOFYEAR",
    "DOY": "DAYOFYEAR",
    "DY": "DAYOFYEAR",
    "W": "WEEK",
    "WK": "WEEK",
    "WEEKOFYEAR": "WEEK",
    "WOY": "WEEK",
    "WY": "WEEK",
    "WEEK_ISO": "WEEKISO",
    "WEEKOFYEARISO": "WEEKISO",
    "WEEKOFYEAR_ISO": "WEEKISO",
    "Q": "QUARTER",
    "QTR": "QUARTER",
    "QTRS": "QUARTER",
    "QUARTERS": "QUARTER",
    "H": "HOUR",
    "HH": "HOUR",
    "HR": "HOUR",
    "HOURS": "HOUR",
    "HRS": "HOUR",
    "M": "MINUTE",
    "MI": "MINUTE",
    "MIN": "MINUTE",
    "MINUTES": "MINUTE",
    "MINS": "MINUTE",
    "S": "SECOND",
    "SEC": "SECOND",
    "SECONDS": "SECOND",
    "SECS": "SECOND",
    "MS": "MILLISECOND",
    "MSEC": "MILLISECOND",
    "MILLISECONDS": "MILLISECOND",
    "US": "MICROSECOND",
    "USEC": "MICROSECOND",
    "MICROSECONDS": "MICROSECOND",
    "NS": "NANOSECOND",
    "NSEC": "NANOSECOND",
    "NANOSEC": "NANOSECOND",
    "NSECOND": "NANOSECOND",
    "NSECONDS": "NANOSECOND",
    "NANOSECS": "NANOSECOND",
    "EPOCH": "EPOCH_SECOND",
    "EPOCH_SECONDS": "EPOCH_SECOND",
    "EPOCH_MILLISECONDS": "EPOCH_MILLISECOND",
    "EPOCH_MICROSECONDS": "EPOCH_MICROSECOND",
    "EPOCH_NANOSECONDS": "EPOCH_NANOSECOND",
    "TZH": "TIMEZONE_HOUR",
    "TZM": "TIMEZONE_MINUTE",
}


@t.overload
def _map_date_part(part: exp.Expression) -> exp.Var:
    pass


@t.overload
def _map_date_part(part: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
    pass


def _map_date_part(part):
    mapped = DATE_PART_MAPPING.get(part.name.upper()) if part else None
    return exp.var(mapped) if mapped else part


def _date_trunc_to_time(args: t.List) -> exp.DateTrunc | exp.TimestampTrunc:
    trunc = date_trunc_to_time(args)
    trunc.set("unit", _map_date_part(trunc.args["unit"]))
    return trunc


def _build_timestamp_from_parts(args: t.List) -> exp.Func:
    if len(args) == 2:
        # Other dialects don't have the TIMESTAMP_FROM_PARTS(date, time) concept,
        # so we parse this into Anonymous for now instead of introducing complexity
        return exp.Anonymous(this="TIMESTAMP_FROM_PARTS", expressions=args)

    return exp.TimestampFromParts.from_arg_list(args)


def _unqualify_unpivot_columns(expression: exp.Expression) -> exp.Expression:
    """
    Snowflake doesn't allow columns referenced in UNPIVOT to be qualified,
    so we need to unqualify them.

    Example:
        >>> from sqlglot import parse_one
        >>> expr = parse_one("SELECT * FROM m_sales UNPIVOT(sales FOR month IN (m_sales.jan, feb, mar, april))")
        >>> print(_unqualify_unpivot_columns(expr).sql(dialect="snowflake"))
        SELECT * FROM m_sales UNPIVOT(sales FOR month IN (jan, feb, mar, april))
    """
    if isinstance(expression, exp.Pivot) and expression.unpivot:
        expression = transforms.unqualify_columns(expression)

    return expression


class Snowflake(Dialect):
    # https://docs.snowflake.com/en/sql-reference/identifiers-syntax
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False
    PREFER_CTE_ALIAS_COLUMN = True
    TABLESAMPLE_SIZE_IS_PERCENT = True

    TIME_MAPPING = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "mmmm": "%B",
        "MON": "%b",
        "mon": "%b",
        "MM": "%m",
        "mm": "%m",
        "DD": "%d",
        "dd": "%-d",
        "DY": "%a",
        "dy": "%w",
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FF": "%f",
        "ff": "%f",
        "FF6": "%f",
        "ff6": "%f",
    }

    def quote_identifier(self, expression: E, identify: bool = True) -> E:
        # This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
        # unquoted DUAL keyword in a special way and does not map it to a user-defined table
        if (
            isinstance(expression, exp.Identifier)
            and isinstance(expression.parent, exp.Table)
            and expression.name.lower() == "dual"
        ):
            return expression  # type: ignore

        return super().quote_identifier(expression, identify=identify)

    class Parser(parser.Parser):
        IDENTIFY_PIVOT_STRINGS = True

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {TokenType.WINDOW}

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAYAGG": exp.ArrayAgg.from_arg_list,
            "ARRAY_CONSTRUCT": exp.Array.from_arg_list,
            "ARRAY_CONTAINS": lambda args: exp.ArrayContains(
                this=seq_get(args, 1), expression=seq_get(args, 0)
            ),
            "ARRAY_GENERATE_RANGE": lambda args: exp.GenerateSeries(
                # ARRAY_GENERATE_RANGE has an exlusive end; we normalize it to be inclusive
                start=seq_get(args, 0),
                end=exp.Sub(this=seq_get(args, 1), expression=exp.Literal.number(1)),
                step=seq_get(args, 2),
            ),
            "BITXOR": binary_from_function(exp.BitwiseXor),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BOOLXOR": binary_from_function(exp.Xor),
            "CONVERT_TIMEZONE": _build_convert_timezone,
            "DATE_TRUNC": _date_trunc_to_time,
            "DATEADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=_map_date_part(seq_get(args, 0)),
            ),
            "DATEDIFF": _build_datediff,
            "DIV0": _build_if_from_div0,
            "FLATTEN": exp.Explode.from_arg_list,
            "GET_PATH": lambda args, dialect: exp.JSONExtract(
                this=seq_get(args, 0), expression=dialect.to_json_path(seq_get(args, 1))
            ),
            "IFF": exp.If.from_arg_list,
            "LAST_DAY": lambda args: exp.LastDay(
                this=seq_get(args, 0), unit=_map_date_part(seq_get(args, 1))
            ),
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "NULLIFZERO": _build_if_from_nullifzero,
            "OBJECT_CONSTRUCT": _build_object_construct,
            "REGEXP_REPLACE": _build_regexp_replace,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "TIMEDIFF": _build_datediff,
            "TIMESTAMPDIFF": _build_datediff,
            "TIMESTAMPFROMPARTS": _build_timestamp_from_parts,
            "TIMESTAMP_FROM_PARTS": _build_timestamp_from_parts,
            "TO_NUMBER": lambda args: exp.ToNumber(
                this=seq_get(args, 0),
                format=seq_get(args, 1),
                precision=seq_get(args, 2),
                scale=seq_get(args, 3),
            ),
            "TO_TIMESTAMP": _build_to_timestamp,
            "TO_VARCHAR": exp.ToChar.from_arg_list,
            "ZEROIFNULL": _build_if_from_zeroifnull,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "DATE_PART": lambda self: self._parse_date_part(),
            "OBJECT_CONSTRUCT_KEEP_NULL": lambda self: self._parse_json_object(),
        }
        FUNCTION_PARSERS.pop("TRIM")

        TIMESTAMPS = parser.Parser.TIMESTAMPS - {TokenType.TIME}

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.LIKE_ANY: parser.binary_range_parser(exp.LikeAny),
            TokenType.ILIKE_ANY: parser.binary_range_parser(exp.ILikeAny),
        }

        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,
            "SET": lambda self: self._parse_set(tag=self._match_text_seq("TAG")),
            "UNSET": lambda self: self.expression(
                exp.Set,
                tag=self._match_text_seq("TAG"),
                expressions=self._parse_csv(self._parse_id_var),
                unset=True,
            ),
            "SWAP": lambda self: self._parse_alter_table_swap(),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "LOCATION": lambda self: self._parse_location(),
        }

        SHOW_PARSERS = {
            "SCHEMAS": _show_parser("SCHEMAS"),
            "TERSE SCHEMAS": _show_parser("SCHEMAS"),
            "OBJECTS": _show_parser("OBJECTS"),
            "TERSE OBJECTS": _show_parser("OBJECTS"),
            "TABLES": _show_parser("TABLES"),
            "TERSE TABLES": _show_parser("TABLES"),
            "VIEWS": _show_parser("VIEWS"),
            "TERSE VIEWS": _show_parser("VIEWS"),
            "PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
            "TERSE PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
            "IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
            "TERSE IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
            "UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
            "TERSE UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
            "SEQUENCES": _show_parser("SEQUENCES"),
            "TERSE SEQUENCES": _show_parser("SEQUENCES"),
            "COLUMNS": _show_parser("COLUMNS"),
            "USERS": _show_parser("USERS"),
            "TERSE USERS": _show_parser("USERS"),
        }

        STAGED_FILE_SINGLE_TOKENS = {
            TokenType.DOT,
            TokenType.MOD,
            TokenType.SLASH,
        }

        FLATTEN_COLUMNS = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]

        SCHEMA_KINDS = {"OBJECTS", "TABLES", "VIEWS", "SEQUENCES", "UNIQUE KEYS", "IMPORTED KEYS"}

        def _parse_column_ops(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
            this = super()._parse_column_ops(this)

            casts = []
            json_path = []

            while self._match(TokenType.COLON):
                path = super()._parse_column_ops(self._parse_field(any_token=True))

                # The cast :: operator has a lower precedence than the extraction operator :, so
                # we rearrange the AST appropriately to avoid casting the 2nd argument of GET_PATH
                while isinstance(path, exp.Cast):
                    casts.append(path.to)
                    path = path.this

                if path:
                    json_path.append(path.sql(dialect="snowflake", copy=False))

            if json_path:
                this = self.expression(
                    exp.JSONExtract,
                    this=this,
                    expression=self.dialect.to_json_path(exp.Literal.string(".".join(json_path))),
                )

                while casts:
                    this = self.expression(exp.Cast, this=this, to=casts.pop())

            return this

        # https://docs.snowflake.com/en/sql-reference/functions/date_part.html
        # https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
        def _parse_date_part(self: Snowflake.Parser) -> t.Optional[exp.Expression]:
            this = self._parse_var() or self._parse_type()

            if not this:
                return None

            self._match(TokenType.COMMA)
            expression = self._parse_bitwise()
            this = _map_date_part(this)
            name = this.name.upper()

            if name.startswith("EPOCH"):
                if name == "EPOCH_MILLISECOND":
                    scale = 10**3
                elif name == "EPOCH_MICROSECOND":
                    scale = 10**6
                elif name == "EPOCH_NANOSECOND":
                    scale = 10**9
                else:
                    scale = None

                ts = self.expression(exp.Cast, this=expression, to=exp.DataType.build("TIMESTAMP"))
                to_unix: exp.Expression = self.expression(exp.TimeToUnix, this=ts)

                if scale:
                    to_unix = exp.Mul(this=to_unix, expression=exp.Literal.number(scale))

                return to_unix

            return self.expression(exp.Extract, this=this, expression=expression)

        def _parse_bracket_key_value(self, is_map: bool = False) -> t.Optional[exp.Expression]:
            if is_map:
                # Keys are strings in Snowflake's objects, see also:
                # - https://docs.snowflake.com/en/sql-reference/data-types-semistructured
                # - https://docs.snowflake.com/en/sql-reference/functions/object_construct
                return self._parse_slice(self._parse_string())

            return self._parse_slice(self._parse_alias(self._parse_conjunction(), explicit=True))

        def _parse_lateral(self) -> t.Optional[exp.Lateral]:
            lateral = super()._parse_lateral()
            if not lateral:
                return lateral

            if isinstance(lateral.this, exp.Explode):
                table_alias = lateral.args.get("alias")
                columns = [exp.to_identifier(col) for col in self.FLATTEN_COLUMNS]
                if table_alias and not table_alias.args.get("columns"):
                    table_alias.set("columns", columns)
                elif not table_alias:
                    exp.alias_(lateral, "_flattened", table=columns, copy=False)

            return lateral

        def _parse_at_before(self, table: exp.Table) -> exp.Table:
            # https://docs.snowflake.com/en/sql-reference/constructs/at-before
            index = self._index
            if self._match_texts(("AT", "BEFORE")):
                this = self._prev.text.upper()
                kind = (
                    self._match(TokenType.L_PAREN)
                    and self._match_texts(self.HISTORICAL_DATA_KIND)
                    and self._prev.text.upper()
                )
                expression = self._match(TokenType.FARROW) and self._parse_bitwise()

                if expression:
                    self._match_r_paren()
                    when = self.expression(
                        exp.HistoricalData, this=this, kind=kind, expression=expression
                    )
                    table.set("when", when)
                else:
                    self._retreat(index)

            return table

        def _parse_table_parts(
            self, schema: bool = False, is_db_reference: bool = False, wildcard: bool = False
        ) -> exp.Table:
            # https://docs.snowflake.com/en/user-guide/querying-stage
            if self._match(TokenType.STRING, advance=False):
                table = self._parse_string()
            elif self._match_text_seq("@", advance=False):
                table = self._parse_location_path()
            else:
                table = None

            if table:
                file_format = None
                pattern = None

                self._match(TokenType.L_PAREN)
                while self._curr and not self._match(TokenType.R_PAREN):
                    if self._match_text_seq("FILE_FORMAT", "=>"):
                        file_format = self._parse_string() or super()._parse_table_parts(
                            is_db_reference=is_db_reference
                        )
                    elif self._match_text_seq("PATTERN", "=>"):
                        pattern = self._parse_string()
                    else:
                        break

                    self._match(TokenType.COMMA)

                table = self.expression(exp.Table, this=table, format=file_format, pattern=pattern)
            else:
                table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference)

            return self._parse_at_before(table)

        def _parse_id_var(
            self,
            any_token: bool = True,
            tokens: t.Optional[t.Collection[TokenType]] = None,
        ) -> t.Optional[exp.Expression]:
            if self._match_text_seq("IDENTIFIER", "("):
                identifier = (
                    super()._parse_id_var(any_token=any_token, tokens=tokens)
                    or self._parse_string()
                )
                self._match_r_paren()
                return self.expression(exp.Anonymous, this="IDENTIFIER", expressions=[identifier])

            return super()._parse_id_var(any_token=any_token, tokens=tokens)

        def _parse_show_snowflake(self, this: str) -> exp.Show:
            scope = None
            scope_kind = None

            # will identity SHOW TERSE SCHEMAS but not SHOW TERSE PRIMARY KEYS
            # which is syntactically valid but has no effect on the output
            terse = self._tokens[self._index - 2].text.upper() == "TERSE"

            history = self._match_text_seq("HISTORY")

            like = self._parse_string() if self._match(TokenType.LIKE) else None

            if self._match(TokenType.IN):
                if self._match_text_seq("ACCOUNT"):
                    scope_kind = "ACCOUNT"
                elif self._match_set(self.DB_CREATABLES):
                    scope_kind = self._prev.text.upper()
                    if self._curr:
                        scope = self._parse_table_parts()
                elif self._curr:
                    scope_kind = "SCHEMA" if this in self.SCHEMA_KINDS else "TABLE"
                    scope = self._parse_table_parts()

            return self.expression(
                exp.Show,
                **{
                    "terse": terse,
                    "this": this,
                    "history": history,
                    "like": like,
                    "scope": scope,
                    "scope_kind": scope_kind,
                    "starts_with": self._match_text_seq("STARTS", "WITH") and self._parse_string(),
                    "limit": self._parse_limit(),
                    "from": self._parse_string() if self._match(TokenType.FROM) else None,
                },
            )

        def _parse_alter_table_swap(self) -> exp.SwapTable:
            self._match_text_seq("WITH")
            return self.expression(exp.SwapTable, this=self._parse_table(schema=True))

        def _parse_location(self) -> exp.LocationProperty:
            self._match(TokenType.EQ)
            return self.expression(exp.LocationProperty, this=self._parse_location_path())

        def _parse_location_path(self) -> exp.Var:
            parts = [self._advance_any(ignore_reserved=True)]

            # We avoid consuming a comma token because external tables like @foo and @bar
            # can be joined in a query with a comma separator.
            while self._is_connected() and not self._match(TokenType.COMMA, advance=False):
                parts.append(self._advance_any(ignore_reserved=True))

            return exp.var("".join(part.text for part in parts if part))

    class Tokenizer(tokens.Tokenizer):
        STRING_ESCAPES = ["\\", "'"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        RAW_STRINGS = ["$$"]
        COMMENTS = ["--", "//", ("/*", "*/")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BYTEINT": TokenType.INT,
            "CHAR VARYING": TokenType.VARCHAR,
            "CHARACTER VARYING": TokenType.VARCHAR,
            "EXCLUDE": TokenType.EXCEPT,
            "ILIKE ANY": TokenType.ILIKE_ANY,
            "LIKE ANY": TokenType.LIKE_ANY,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NCHAR VARYING": TokenType.VARCHAR,
            "PUT": TokenType.COMMAND,
            "REMOVE": TokenType.COMMAND,
            "RENAME": TokenType.REPLACE,
            "RM": TokenType.COMMAND,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "SQL_DOUBLE": TokenType.DOUBLE,
            "SQL_VARCHAR": TokenType.VARCHAR,
            "STORAGE INTEGRATION": TokenType.STORAGE_INTEGRATION,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP_NTZ": TokenType.TIMESTAMP,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TIMESTAMPNTZ": TokenType.TIMESTAMP,
            "TOP": TokenType.TOP,
        }

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        VAR_SINGLE_TOKENS = {"$"}

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Generator(generator.Generator):
        PARAMETER_TOKEN = "$"
        MATCHED_BY_SOURCE = False
        SINGLE_STRING_INTERVAL = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        AGGREGATE_FILTER_SUPPORTED = False
        SUPPORTS_TABLE_COPY = False
        COLLATE_IS_FUNC = True
        LIMIT_ONLY_LITERALS = True
        JSON_KEY_VALUE_PAIR_SEP = ","
        INSERT_OVERWRITE = " OVERWRITE INTO"

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.Array: inline_array_sql,
            exp.ArrayConcat: rename_func("ARRAY_CAT"),
            exp.ArrayContains: lambda self, e: self.func("ARRAY_CONTAINS", e.expression, e.this),
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), e.this
            ),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DataType: _datatype_sql,
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.Explode: rename_func("FLATTEN"),
            exp.Extract: rename_func("DATE_PART"),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), "'UTC'", e.this
            ),
            exp.GenerateSeries: lambda self, e: self.func(
                "ARRAY_GENERATE_RANGE", e.args["start"], e.args["end"] + 1, e.args.get("step")
            ),
            exp.GroupConcat: rename_func("LISTAGG"),
            exp.If: if_sql(name="IFF", false_value="NULL"),
            exp.JSONExtract: lambda self, e: self.func("GET_PATH", e.this, e.expression),
            exp.JSONExtractScalar: lambda self, e: self.func(
                "JSON_EXTRACT_PATH_TEXT", e.this, e.expression
            ),
            exp.JSONObject: lambda self, e: self.func("OBJECT_CONSTRUCT_KEEP_NULL", *e.expressions),
            exp.JSONPathRoot: lambda *_: "",
            exp.LogicalAnd: rename_func("BOOLAND_AGG"),
            exp.LogicalOr: rename_func("BOOLOR_AGG"),
            exp.Map: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.PercentileCont: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.PercentileDisc: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.Pivot: transforms.preprocess([_unqualify_unpivot_columns]),
            exp.RegexpILike: _regexpilike_sql,
            exp.Rand: rename_func("RANDOM"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.explode_to_unnest(),
                    transforms.eliminate_semi_and_anti_joins,
                ]
            ),
            exp.SHA: rename_func("SHA1"),
            exp.StarMap: rename_func("OBJECT_CONSTRUCT"),
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.StrPosition: lambda self, e: self.func(
                "POSITION", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.Stuff: rename_func("INSERT"),
            exp.TimestampDiff: lambda self, e: self.func(
                "TIMESTAMPDIFF", e.unit, e.expression, e.this
            ),
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToStr: lambda self, e: self.func(
                "TO_CHAR", exp.cast(e.this, "timestamp"), self.format_time(e)
            ),
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.ToArray: rename_func("TO_ARRAY"),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.Trim: lambda self, e: self.func("TRIM", e.this, e.expression),
            exp.TsOrDsAdd: date_delta_sql("DATEADD", cast=True),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
            exp.UnixToTime: rename_func("TO_TIMESTAMP"),
            exp.VarMap: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.Xor: rename_func("BOOLXOR"),
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TIMESTAMP: "TIMESTAMPNTZ",
        }

        STAR_MAPPING = {
            "except": "EXCLUDE",
            "replace": "RENAME",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.SetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def tonumber_sql(self, expression: exp.ToNumber) -> str:
            return self.func(
                "TO_NUMBER",
                expression.this,
                expression.args.get("format"),
                expression.args.get("precision"),
                expression.args.get("scale"),
            )

        def timestampfromparts_sql(self, expression: exp.TimestampFromParts) -> str:
            milli = expression.args.get("milli")
            if milli is not None:
                milli_to_nano = milli.pop() * exp.Literal.number(1000000)
                expression.set("nano", milli_to_nano)

            return rename_func("TIMESTAMP_FROM_PARTS")(self, expression)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            value = expression.this

            if value.type is None:
                from sqlglot.optimizer.annotate_types import annotate_types

                value = annotate_types(value)

            if value.is_type(*exp.DataType.TEXT_TYPES, exp.DataType.Type.UNKNOWN):
                return super().trycast_sql(expression)

            # TRY_CAST only works for string values in Snowflake
            return self.cast_sql(expression)

        def log_sql(self, expression: exp.Log) -> str:
            if not expression.expression:
                return self.func("LN", expression.this)

            return super().log_sql(expression)

        def unnest_sql(self, expression: exp.Unnest) -> str:
            unnest_alias = expression.args.get("alias")
            offset = expression.args.get("offset")

            columns = [
                exp.to_identifier("seq"),
                exp.to_identifier("key"),
                exp.to_identifier("path"),
                offset.pop() if isinstance(offset, exp.Expression) else exp.to_identifier("index"),
                seq_get(unnest_alias.columns if unnest_alias else [], 0)
                or exp.to_identifier("value"),
                exp.to_identifier("this"),
            ]

            if unnest_alias:
                unnest_alias.set("columns", columns)
            else:
                unnest_alias = exp.TableAlias(this="_u", columns=columns)

            explode = f"TABLE(FLATTEN(INPUT => {self.sql(expression.expressions[0])}))"
            alias = self.sql(unnest_alias)
            alias = f" AS {alias}" if alias else ""
            return f"{explode}{alias}"

        def show_sql(self, expression: exp.Show) -> str:
            terse = "TERSE " if expression.args.get("terse") else ""
            history = " HISTORY" if expression.args.get("history") else ""
            like = self.sql(expression, "like")
            like = f" LIKE {like}" if like else ""

            scope = self.sql(expression, "scope")
            scope = f" {scope}" if scope else ""

            scope_kind = self.sql(expression, "scope_kind")
            if scope_kind:
                scope_kind = f" IN {scope_kind}"

            starts_with = self.sql(expression, "starts_with")
            if starts_with:
                starts_with = f" STARTS WITH {starts_with}"

            limit = self.sql(expression, "limit")

            from_ = self.sql(expression, "from")
            if from_:
                from_ = f" FROM {from_}"

            return f"SHOW {terse}{expression.name}{history}{like}{scope_kind}{scope}{starts_with}{limit}{from_}"

        def regexpextract_sql(self, expression: exp.RegexpExtract) -> str:
            # Other dialects don't support all of the following parameters, so we need to
            # generate default values as necessary to ensure the transpilation is correct
            group = expression.args.get("group")
            parameters = expression.args.get("parameters") or (group and exp.Literal.string("c"))
            occurrence = expression.args.get("occurrence") or (parameters and exp.Literal.number(1))
            position = expression.args.get("position") or (occurrence and exp.Literal.number(1))

            return self.func(
                "REGEXP_SUBSTR",
                expression.this,
                expression.expression,
                position,
                occurrence,
                parameters,
                group,
            )

        def except_op(self, expression: exp.Except) -> str:
            if not expression.args.get("distinct"):
                self.unsupported("EXCEPT with All is not supported in Snowflake")
            return super().except_op(expression)

        def intersect_op(self, expression: exp.Intersect) -> str:
            if not expression.args.get("distinct"):
                self.unsupported("INTERSECT with All is not supported in Snowflake")
            return super().intersect_op(expression)

        def describe_sql(self, expression: exp.Describe) -> str:
            # Default to table if kind is unknown
            kind_value = expression.args.get("kind") or "TABLE"
            kind = f" {kind_value}" if kind_value else ""
            this = f" {self.sql(expression, 'this')}"
            expressions = self.expressions(expression, flat=True)
            expressions = f" {expressions}" if expressions else ""
            return f"DESCRIBE{kind}{this}{expressions}"

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            start = expression.args.get("start")
            start = f" START {start}" if start else ""
            increment = expression.args.get("increment")
            increment = f" INCREMENT {increment}" if increment else ""
            return f"AUTOINCREMENT{start}{increment}"

        def swaptable_sql(self, expression: exp.SwapTable) -> str:
            this = self.sql(expression, "this")
            return f"SWAP WITH {this}"

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(properties, wrapped=False, prefix=self.seg(""), sep=" ")

        def cluster_sql(self, expression: exp.Cluster) -> str:
            return f"CLUSTER BY ({self.expressions(expression, flat=True)})"

        def struct_sql(self, expression: exp.Struct) -> str:
            keys = []
            values = []

            for i, e in enumerate(expression.expressions):
                if isinstance(e, exp.PropertyEQ):
                    keys.append(
                        exp.Literal.string(e.name) if isinstance(e.this, exp.Identifier) else e.this
                    )
                    values.append(e.expression)
                else:
                    keys.append(exp.Literal.string(f"_{i}"))
                    values.append(e)

            return self.func("OBJECT_CONSTRUCT", *flatten(zip(keys, values)))
