from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    date_trunc_to_time,
    datestrtodate_sql,
    format_time_lambda,
    if_sql,
    inline_array_sql,
    max_or_greatest,
    min_or_least,
    rename_func,
    timestamptrunc_sql,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
    var_map_sql,
)
from sqlglot.expressions import Literal
from sqlglot.helper import seq_get
from sqlglot.parser import binary_range_parser
from sqlglot.tokens import TokenType


def _check_int(s: str) -> bool:
    if s[0] in ("-", "+"):
        return s[1:].isdigit()
    return s.isdigit()


# from https://docs.snowflake.com/en/sql-reference/functions/to_timestamp.html
def _parse_to_timestamp(args: t.List) -> t.Union[exp.StrToTime, exp.UnixToTime]:
    if len(args) == 2:
        first_arg, second_arg = args
        if second_arg.is_string:
            # case: <string_expr> [ , <format> ]
            return format_time_lambda(exp.StrToTime, "snowflake")(args)

        # case: <numeric_expr> [ , <scale> ]
        if second_arg.name not in ["0", "3", "9"]:
            raise ValueError(
                f"Scale for snowflake numeric timestamp is {second_arg}, but should be 0, 3, or 9"
            )

        if second_arg.name == "0":
            timescale = exp.UnixToTime.SECONDS
        elif second_arg.name == "3":
            timescale = exp.UnixToTime.MILLIS
        elif second_arg.name == "9":
            timescale = exp.UnixToTime.MICROS

        return exp.UnixToTime(this=first_arg, scale=timescale)

    from sqlglot.optimizer.simplify import simplify_literals

    # The first argument might be an expression like 40 * 365 * 86400, so we try to
    # reduce it using `simplify_literals` first and then check if it's a Literal.
    first_arg = seq_get(args, 0)
    if not isinstance(simplify_literals(first_arg, root=True), Literal):
        # case: <variant_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)(args)

    if first_arg.is_string:
        if _check_int(first_arg.this):
            # case: <integer>
            return exp.UnixToTime.from_arg_list(args)

        # case: <date_expr>
        return format_time_lambda(exp.StrToTime, "snowflake", default=True)(args)

    # case: <numeric_expr>
    return exp.UnixToTime.from_arg_list(args)


def _parse_object_construct(args: t.List) -> t.Union[exp.StarMap, exp.Struct]:
    expression = parser.parse_var_map(args)

    if isinstance(expression, exp.StarMap):
        return expression

    return exp.Struct(
        expressions=[
            t.cast(exp.Condition, k).eq(v) for k, v in zip(expression.keys, expression.values)
        ]
    )


def _parse_datediff(args: t.List) -> exp.DateDiff:
    return exp.DateDiff(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))


def _unix_to_time_sql(self: Snowflake.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale in [None, exp.UnixToTime.SECONDS]:
        return f"TO_TIMESTAMP({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TO_TIMESTAMP({timestamp}, 3)"
    if scale == exp.UnixToTime.MICROS:
        return f"TO_TIMESTAMP({timestamp}, 9)"

    raise ValueError("Improper scale for timestamp")


# https://docs.snowflake.com/en/sql-reference/functions/date_part.html
# https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
def _parse_date_part(self: Snowflake.Parser) -> t.Optional[exp.Expression]:
    this = self._parse_var() or self._parse_type()

    if not this:
        return None

    self._match(TokenType.COMMA)
    expression = self._parse_bitwise()

    name = this.name.upper()
    if name.startswith("EPOCH"):
        if name.startswith("EPOCH_MILLISECOND"):
            scale = 10**3
        elif name.startswith("EPOCH_MICROSECOND"):
            scale = 10**6
        elif name.startswith("EPOCH_NANOSECOND"):
            scale = 10**9
        else:
            scale = None

        ts = self.expression(exp.Cast, this=expression, to=exp.DataType.build("TIMESTAMP"))
        to_unix: exp.Expression = self.expression(exp.TimeToUnix, this=ts)

        if scale:
            to_unix = exp.Mul(this=to_unix, expression=exp.Literal.number(scale))

        return to_unix

    return self.expression(exp.Extract, this=this, expression=expression)


# https://docs.snowflake.com/en/sql-reference/functions/div0
def _div0_to_if(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 1), expression=exp.Literal.number(0))
    true = exp.Literal.number(0)
    false = exp.Div(this=seq_get(args, 0), expression=seq_get(args, 1))
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _zeroifnull_to_if(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _nullifzero_to_if(args: t.List) -> exp.If:
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


def _parse_convert_timezone(args: t.List) -> t.Union[exp.Anonymous, exp.AtTimeZone]:
    if len(args) == 3:
        return exp.Anonymous(this="CONVERT_TIMEZONE", expressions=args)
    return exp.AtTimeZone(this=seq_get(args, 1), zone=seq_get(args, 0))


def _parse_regexp_replace(args: t.List) -> exp.RegexpReplace:
    regexp_replace = exp.RegexpReplace.from_arg_list(args)

    if not regexp_replace.args.get("replacement"):
        regexp_replace.set("replacement", exp.Literal.string(""))

    return regexp_replace


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[Snowflake.Parser], exp.Show]:
    def _parse(self: Snowflake.Parser) -> exp.Show:
        return self._parse_show_snowflake(*args, **kwargs)

    return _parse


class Snowflake(Dialect):
    # https://docs.snowflake.com/en/sql-reference/identifiers-syntax
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = True
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False

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

    class Parser(parser.Parser):
        IDENTIFY_PIVOT_STRINGS = True

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {TokenType.WINDOW}

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAYAGG": exp.ArrayAgg.from_arg_list,
            "ARRAY_CONSTRUCT": exp.Array.from_arg_list,
            "ARRAY_GENERATE_RANGE": lambda args: exp.GenerateSeries(
                # ARRAY_GENERATE_RANGE has an exlusive end; we normalize it to be inclusive
                start=seq_get(args, 0),
                end=exp.Sub(this=seq_get(args, 1), expression=exp.Literal.number(1)),
                step=seq_get(args, 2),
            ),
            "ARRAY_TO_STRING": exp.ArrayJoin.from_arg_list,
            "BITXOR": binary_from_function(exp.BitwiseXor),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BOOLXOR": binary_from_function(exp.Xor),
            "CONVERT_TIMEZONE": _parse_convert_timezone,
            "DATE_TRUNC": date_trunc_to_time,
            "DATEADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATEDIFF": _parse_datediff,
            "DIV0": _div0_to_if,
            "FLATTEN": exp.Explode.from_arg_list,
            "IFF": exp.If.from_arg_list,
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "NULLIFZERO": _nullifzero_to_if,
            "OBJECT_CONSTRUCT": _parse_object_construct,
            "REGEXP_REPLACE": _parse_regexp_replace,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "TIMEDIFF": _parse_datediff,
            "TIMESTAMPDIFF": _parse_datediff,
            "TO_ARRAY": exp.Array.from_arg_list,
            "TO_TIMESTAMP": _parse_to_timestamp,
            "TO_VARCHAR": exp.ToChar.from_arg_list,
            "ZEROIFNULL": _zeroifnull_to_if,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "DATE_PART": _parse_date_part,
        }
        FUNCTION_PARSERS.pop("TRIM")

        COLUMN_OPERATORS = {
            **parser.Parser.COLUMN_OPERATORS,
            TokenType.COLON: lambda self, this, path: self.expression(
                exp.Bracket, this=this, expressions=[path]
            ),
        }

        TIMESTAMPS = parser.Parser.TIMESTAMPS - {TokenType.TIME}

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.LIKE_ANY: binary_range_parser(exp.LikeAny),
            TokenType.ILIKE_ANY: binary_range_parser(exp.ILikeAny),
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
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        SHOW_PARSERS = {
            "PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
            "TERSE PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
        }

        STAGED_FILE_SINGLE_TOKENS = {
            TokenType.DOT,
            TokenType.MOD,
            TokenType.SLASH,
        }
        FLATTEN_COLUMNS = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]

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

        def _parse_table_parts(self, schema: bool = False) -> exp.Table:
            # https://docs.snowflake.com/en/user-guide/querying-stage
            table: t.Optional[exp.Expression] = None
            if self._match_text_seq("@"):
                table_name = "@"
                while True:
                    self._advance()
                    table_name += self._prev.text
                    if not self._match_set(self.STAGED_FILE_SINGLE_TOKENS, advance=False):
                        break
                    while self._match_set(self.STAGED_FILE_SINGLE_TOKENS):
                        table_name += self._prev.text

                table = exp.var(table_name)
            elif self._match(TokenType.STRING, advance=False):
                table = self._parse_string()

            if table:
                file_format = None
                pattern = None

                if self._match_text_seq("(", "FILE_FORMAT", "=>"):
                    file_format = self._parse_string() or super()._parse_table_parts()
                    if self._match_text_seq(",", "PATTERN", "=>"):
                        pattern = self._parse_string()
                    self._match_r_paren()

                return self.expression(exp.Table, this=table, format=file_format, pattern=pattern)

            return super()._parse_table_parts(schema=schema)

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

            if self._match(TokenType.IN):
                if self._match_text_seq("ACCOUNT"):
                    scope_kind = "ACCOUNT"
                elif self._match_set(self.DB_CREATABLES):
                    scope_kind = self._prev.text
                    if self._curr:
                        scope = self._parse_table()
                elif self._curr:
                    scope_kind = "TABLE"
                    scope = self._parse_table()

            return self.expression(exp.Show, this=this, scope=scope, scope_kind=scope_kind)

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
            "RENAME": TokenType.REPLACE,
            "SAMPLE": TokenType.TABLE_SAMPLE,
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

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.ArrayConcat: rename_func("ARRAY_CAT"),
            exp.ArrayJoin: rename_func("ARRAY_TO_STRING"),
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), e.this
            ),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.DateAdd: lambda self, e: self.func("DATEADD", e.text("unit"), e.expression, e.this),
            exp.DateDiff: lambda self, e: self.func(
                "DATEDIFF", e.text("unit"), e.expression, e.this
            ),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DataType: _datatype_sql,
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.Explode: rename_func("FLATTEN"),
            exp.Extract: rename_func("DATE_PART"),
            exp.GenerateSeries: lambda self, e: self.func(
                "ARRAY_GENERATE_RANGE", e.args["start"], e.args["end"] + 1, e.args.get("step")
            ),
            exp.GroupConcat: rename_func("LISTAGG"),
            exp.If: if_sql(name="IFF", false_value="NULL"),
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
            exp.RegexpILike: _regexpilike_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.explode_to_unnest(0),
                    transforms.eliminate_semi_and_anti_joins,
                ]
            ),
            exp.StarMap: rename_func("OBJECT_CONSTRUCT"),
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.StrPosition: lambda self, e: self.func(
                "POSITION", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Struct: lambda self, e: self.func(
                "OBJECT_CONSTRUCT",
                *(arg for expression in e.expressions for arg in expression.flatten()),
            ),
            exp.Stuff: rename_func("INSERT"),
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToStr: lambda self, e: self.func(
                "TO_CHAR", exp.cast(e.this, "timestamp"), self.format_time(e)
            ),
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.Trim: lambda self, e: self.func("TRIM", e.this, e.expression),
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("snowflake"),
            exp.UnixToTime: _unix_to_time_sql,
            exp.VarMap: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.Xor: rename_func("BOOLXOR"),
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

        def unnest_sql(self, expression: exp.Unnest) -> str:
            selects = ["value"]
            unnest_alias = expression.args.get("alias")

            offset = expression.args.get("offset")
            if offset:
                if unnest_alias:
                    expression = expression.copy()
                    unnest_alias.append("columns", offset.pop())

                selects.append("index")

            subquery = exp.Subquery(
                this=exp.select(*selects).from_(
                    f"TABLE(FLATTEN(INPUT => {self.sql(expression.expressions[0])}))"
                ),
            )
            alias = self.sql(unnest_alias)
            alias = f" AS {alias}" if alias else ""
            return f"{self.sql(subquery)}{alias}"

        def show_sql(self, expression: exp.Show) -> str:
            scope = self.sql(expression, "scope")
            scope = f" {scope}" if scope else ""

            scope_kind = self.sql(expression, "scope_kind")
            if scope_kind:
                scope_kind = f" IN {scope_kind}"

            return f"SHOW {expression.name}{scope_kind}{scope}"

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
            if not expression.args.get("distinct", False):
                self.unsupported("EXCEPT with All is not supported in Snowflake")
            return super().except_op(expression)

        def intersect_op(self, expression: exp.Intersect) -> str:
            if not expression.args.get("distinct", False):
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
