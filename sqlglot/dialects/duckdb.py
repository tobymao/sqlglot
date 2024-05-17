from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    JSON_EXTRACT_TYPE,
    NormalizationStrategy,
    approx_count_distinct_sql,
    arg_max_or_min_no_count,
    arrow_json_extract_sql,
    binary_from_function,
    bool_xor_sql,
    build_default_decimal_type,
    date_trunc_to_time,
    datestrtodate_sql,
    encode_decode_sql,
    build_formatted_time,
    inline_array_unless_query,
    no_comment_column_constraint_sql,
    no_safe_divide_sql,
    no_timestamp_sql,
    pivot_column_names,
    regexp_extract_sql,
    rename_func,
    str_position_sql,
    str_to_time_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    unit_to_var,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _ts_or_ds_add_sql(self: DuckDB.Generator, expression: exp.TsOrDsAdd) -> str:
    this = self.sql(expression, "this")
    interval = self.sql(exp.Interval(this=expression.expression, unit=unit_to_var(expression)))
    return f"CAST({this} AS {self.sql(expression.return_type)}) + {interval}"


def _date_delta_sql(
    self: DuckDB.Generator, expression: exp.DateAdd | exp.DateSub | exp.TimeAdd
) -> str:
    this = self.sql(expression, "this")
    unit = unit_to_var(expression)
    op = "+" if isinstance(expression, (exp.DateAdd, exp.TimeAdd)) else "-"
    return f"{this} {op} {self.sql(exp.Interval(this=expression.expression, unit=unit))}"


# BigQuery -> DuckDB conversion for the DATE function
def _date_sql(self: DuckDB.Generator, expression: exp.Date) -> str:
    result = f"CAST({self.sql(expression, 'this')} AS DATE)"
    zone = self.sql(expression, "zone")

    if zone:
        date_str = self.func("STRFTIME", result, "'%d/%m/%Y'")
        date_str = f"{date_str} || ' ' || {zone}"

        # This will create a TIMESTAMP with time zone information
        result = self.func("STRPTIME", date_str, "'%d/%m/%Y %Z'")

    return result


def _array_sort_sql(self: DuckDB.Generator, expression: exp.ArraySort) -> str:
    if expression.expression:
        self.unsupported("DuckDB ARRAY_SORT does not support a comparator")
    return self.func("ARRAY_SORT", expression.this)


def _sort_array_sql(self: DuckDB.Generator, expression: exp.SortArray) -> str:
    name = "ARRAY_REVERSE_SORT" if expression.args.get("asc") == exp.false() else "ARRAY_SORT"
    return self.func(name, expression.this)


def _build_sort_array_desc(args: t.List) -> exp.Expression:
    return exp.SortArray(this=seq_get(args, 0), asc=exp.false())


def _build_date_diff(args: t.List) -> exp.Expression:
    return exp.DateDiff(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))


def _build_generate_series(end_exclusive: bool = False) -> t.Callable[[t.List], exp.GenerateSeries]:
    def _builder(args: t.List) -> exp.GenerateSeries:
        # Check https://duckdb.org/docs/sql/functions/nested.html#range-functions
        if len(args) == 1:
            # DuckDB uses 0 as a default for the series' start when it's omitted
            args.insert(0, exp.Literal.number("0"))

        gen_series = exp.GenerateSeries.from_arg_list(args)
        gen_series.set("is_end_exclusive", end_exclusive)

        return gen_series

    return _builder


def _build_make_timestamp(args: t.List) -> exp.Expression:
    if len(args) == 1:
        return exp.UnixToTime(this=seq_get(args, 0), scale=exp.UnixToTime.MICROS)

    return exp.TimestampFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        hour=seq_get(args, 3),
        min=seq_get(args, 4),
        sec=seq_get(args, 5),
    )


def _struct_sql(self: DuckDB.Generator, expression: exp.Struct) -> str:
    args: t.List[str] = []
    for i, expr in enumerate(expression.expressions):
        if isinstance(expr, exp.PropertyEQ):
            key = expr.name
            value = expr.expression
        else:
            key = f"_{i}"
            value = expr

        args.append(f"{self.sql(exp.Literal.string(key))}: {self.sql(value)}")

    return f"{{{', '.join(args)}}}"


def _datatype_sql(self: DuckDB.Generator, expression: exp.DataType) -> str:
    if expression.is_type("array"):
        return f"{self.expressions(expression, flat=True)}[]"

    # Type TIMESTAMP / TIME WITH TIME ZONE does not support any modifiers
    if expression.is_type("timestamptz", "timetz"):
        return expression.this.value

    return self.datatype_sql(expression)


def _json_format_sql(self: DuckDB.Generator, expression: exp.JSONFormat) -> str:
    sql = self.func("TO_JSON", expression.this, expression.args.get("options"))
    return f"CAST({sql} AS TEXT)"


def _unix_to_time_sql(self: DuckDB.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("TO_TIMESTAMP", timestamp)
    if scale == exp.UnixToTime.MILLIS:
        return self.func("EPOCH_MS", timestamp)
    if scale == exp.UnixToTime.MICROS:
        return self.func("MAKE_TIMESTAMP", timestamp)

    return self.func("TO_TIMESTAMP", exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)))


def _arrow_json_extract_sql(self: DuckDB.Generator, expression: JSON_EXTRACT_TYPE) -> str:
    arrow_sql = arrow_json_extract_sql(self, expression)
    if not expression.same_parent and isinstance(expression.parent, exp.Binary):
        arrow_sql = self.wrap(arrow_sql)
    return arrow_sql


class DuckDB(Dialect):
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    INDEX_OFFSET = 1
    CONCAT_COALESCE = True

    # https://duckdb.org/docs/sql/introduction.html#creating-a-new-table
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    def to_json_path(self, path: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if isinstance(path, exp.Literal):
            # DuckDB also supports the JSON pointer syntax, where every path starts with a `/`.
            # Additionally, it allows accessing the back of lists using the `[#-i]` syntax.
            # This check ensures we'll avoid trying to parse these as JSON paths, which can
            # either result in a noisy warning or in an invalid representation of the path.
            path_text = path.name
            if path_text.startswith("/") or "[#" in path_text:
                return path

        return super().to_json_path(path)

    class Tokenizer(tokens.Tokenizer):
        HEREDOC_STRINGS = ["$"]

        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "//": TokenType.DIV,
            "ATTACH": TokenType.COMMAND,
            "BINARY": TokenType.VARBINARY,
            "BITSTRING": TokenType.BIT,
            "BPCHAR": TokenType.TEXT,
            "CHAR": TokenType.TEXT,
            "CHARACTER VARYING": TokenType.TEXT,
            "EXCLUDE": TokenType.EXCEPT,
            "LOGICAL": TokenType.BOOLEAN,
            "ONLY": TokenType.ONLY,
            "PIVOT_WIDER": TokenType.PIVOT,
            "POSITIONAL": TokenType.POSITIONAL,
            "SIGNED": TokenType.INT,
            "STRING": TokenType.VARCHAR,
            "UBIGINT": TokenType.UBIGINT,
            "UINTEGER": TokenType.UINT,
            "USMALLINT": TokenType.USMALLINT,
            "UTINYINT": TokenType.UTINYINT,
            "TIMESTAMP_S": TokenType.TIMESTAMP_S,
            "TIMESTAMP_MS": TokenType.TIMESTAMP_MS,
            "TIMESTAMP_NS": TokenType.TIMESTAMP_NS,
            "TIMESTAMP_US": TokenType.TIMESTAMP,
        }

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

    class Parser(parser.Parser):
        BITWISE = {
            **parser.Parser.BITWISE,
            TokenType.TILDA: exp.RegexpLike,
        }

        FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "STRUCT_PACK"}

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAY_HAS": exp.ArrayContains.from_arg_list,
            "ARRAY_REVERSE_SORT": _build_sort_array_desc,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "DATEDIFF": _build_date_diff,
            "DATE_DIFF": _build_date_diff,
            "DATE_TRUNC": date_trunc_to_time,
            "DATETRUNC": date_trunc_to_time,
            "DECODE": lambda args: exp.Decode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "ENCODE": lambda args: exp.Encode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "EPOCH": exp.TimeToUnix.from_arg_list,
            "EPOCH_MS": lambda args: exp.UnixToTime(
                this=seq_get(args, 0), scale=exp.UnixToTime.MILLIS
            ),
            "JSON": exp.ParseJSON.from_arg_list,
            "JSON_EXTRACT_PATH": parser.build_extract_json_with_path(exp.JSONExtract),
            "JSON_EXTRACT_STRING": parser.build_extract_json_with_path(exp.JSONExtractScalar),
            "LIST_HAS": exp.ArrayContains.from_arg_list,
            "LIST_REVERSE_SORT": _build_sort_array_desc,
            "LIST_SORT": exp.SortArray.from_arg_list,
            "LIST_VALUE": exp.Array.from_arg_list,
            "MAKE_TIME": exp.TimeFromParts.from_arg_list,
            "MAKE_TIMESTAMP": _build_make_timestamp,
            "MEDIAN": lambda args: exp.PercentileCont(
                this=seq_get(args, 0), expression=exp.Literal.number(0.5)
            ),
            "QUANTILE_CONT": exp.PercentileCont.from_arg_list,
            "QUANTILE_DISC": exp.PercentileDisc.from_arg_list,
            "REGEXP_EXTRACT": lambda args: exp.RegexpExtract(
                this=seq_get(args, 0), expression=seq_get(args, 1), group=seq_get(args, 2)
            ),
            "REGEXP_MATCHES": exp.RegexpLike.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                modifiers=seq_get(args, 3),
            ),
            "STRFTIME": build_formatted_time(exp.TimeToStr, "duckdb"),
            "STRING_SPLIT": exp.Split.from_arg_list,
            "STRING_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "STRING_TO_ARRAY": exp.Split.from_arg_list,
            "STRPTIME": build_formatted_time(exp.StrToTime, "duckdb"),
            "STRUCT_PACK": exp.Struct.from_arg_list,
            "STR_SPLIT": exp.Split.from_arg_list,
            "STR_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
            "XOR": binary_from_function(exp.BitwiseXor),
            "GENERATE_SERIES": _build_generate_series(),
            "RANGE": _build_generate_series(end_exclusive=True),
        }

        FUNCTIONS.pop("DATE_SUB")

        FUNCTION_PARSERS = parser.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("DECODE")

        NO_PAREN_FUNCTION_PARSERS = {
            **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
            "MAP": lambda self: self._parse_map(),
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.SEMI,
            TokenType.ANTI,
        }

        PLACEHOLDER_PARSERS = {
            **parser.Parser.PLACEHOLDER_PARSERS,
            TokenType.PARAMETER: lambda self: (
                self.expression(exp.Placeholder, this=self._prev.text)
                if self._match(TokenType.NUMBER) or self._match_set(self.ID_VAR_TOKENS)
                else None
            ),
        }

        TYPE_CONVERTER = {
            # https://duckdb.org/docs/sql/data_types/numeric
            exp.DataType.Type.DECIMAL: build_default_decimal_type(precision=18, scale=3),
        }

        def _parse_table_sample(self, as_modifier: bool = False) -> t.Optional[exp.TableSample]:
            # https://duckdb.org/docs/sql/samples.html
            sample = super()._parse_table_sample(as_modifier=as_modifier)
            if sample and not sample.args.get("method"):
                if sample.args.get("size"):
                    sample.set("method", exp.var("RESERVOIR"))
                else:
                    sample.set("method", exp.var("SYSTEM"))

            return sample

        def _parse_bracket(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            bracket = super()._parse_bracket(this)
            if isinstance(bracket, exp.Bracket):
                bracket.set("returns_list_for_maps", True)

            return bracket

        def _parse_map(self) -> exp.ToMap | exp.Map:
            if self._match(TokenType.L_BRACE, advance=False):
                return self.expression(exp.ToMap, this=self._parse_bracket())

            args = self._parse_wrapped_csv(self._parse_conjunction)
            return self.expression(exp.Map, keys=seq_get(args, 0), values=seq_get(args, 1))

        def _parse_struct_types(self, type_required: bool = False) -> t.Optional[exp.Expression]:
            return self._parse_field_def()

        def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
            if len(aggregations) == 1:
                return super()._pivot_column_names(aggregations)
            return pivot_column_names(aggregations, dialect="duckdb")

    class Generator(generator.Generator):
        PARAMETER_TOKEN = "$"
        NAMED_PLACEHOLDER_TOKEN = "$"
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        LIMIT_FETCH = "LIMIT"
        STRUCT_DELIMITER = ("(", ")")
        RENAME_TABLE_WITH_DB = False
        NVL2_SUPPORTED = False
        SEMI_ANTI_JOIN_WITH_SIDE = False
        TABLESAMPLE_KEYWORDS = "USING SAMPLE"
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        IGNORE_NULLS_IN_FUNC = True
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False
        MULTI_ARG_DISTINCT = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_TO_NUMBER = False
        COPY_HAS_INTO_KEYWORD = False
        STAR_EXCEPT = "EXCLUDE"

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.Array: inline_array_unless_query,
            exp.ArrayFilter: rename_func("LIST_FILTER"),
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.ArgMax: arg_max_or_min_no_count("ARG_MAX"),
            exp.ArgMin: arg_max_or_min_no_count("ARG_MIN"),
            exp.ArraySort: _array_sort_sql,
            exp.ArraySum: rename_func("LIST_SUM"),
            exp.BitwiseXor: rename_func("XOR"),
            exp.CommentColumnConstraint: no_comment_column_constraint_sql,
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTime: lambda *_: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.DataType: _datatype_sql,
            exp.Date: _date_sql,
            exp.DateAdd: _date_delta_sql,
            exp.DateFromParts: rename_func("MAKE_DATE"),
            exp.DateSub: _date_delta_sql,
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", f"'{e.args.get('unit') or 'DAY'}'", e.expression, e.this
            ),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateToDi: lambda self,
            e: f"CAST(STRFTIME({self.sql(e, 'this')}, {DuckDB.DATEINT_FORMAT}) AS INT)",
            exp.Decode: lambda self, e: encode_decode_sql(self, e, "DECODE", replace=False),
            exp.DiToDate: lambda self,
            e: f"CAST(STRPTIME(CAST({self.sql(e, 'this')} AS TEXT), {DuckDB.DATEINT_FORMAT}) AS DATE)",
            exp.Encode: lambda self, e: encode_decode_sql(self, e, "ENCODE", replace=False),
            exp.Explode: rename_func("UNNEST"),
            exp.IntDiv: lambda self, e: self.binary(e, "//"),
            exp.IsInf: rename_func("ISINF"),
            exp.IsNan: rename_func("ISNAN"),
            exp.JSONExtract: _arrow_json_extract_sql,
            exp.JSONExtractScalar: _arrow_json_extract_sql,
            exp.JSONFormat: _json_format_sql,
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.MD5Digest: lambda self, e: self.func("UNHEX", self.func("MD5", e.this)),
            exp.MonthsBetween: lambda self, e: self.func(
                "DATEDIFF",
                "'month'",
                exp.cast(e.expression, exp.DataType.Type.TIMESTAMP, copy=True),
                exp.cast(e.this, exp.DataType.Type.TIMESTAMP, copy=True),
            ),
            exp.ParseJSON: rename_func("JSON"),
            exp.PercentileCont: rename_func("QUANTILE_CONT"),
            exp.PercentileDisc: rename_func("QUANTILE_DISC"),
            # DuckDB doesn't allow qualified columns inside of PIVOT expressions.
            # See: https://github.com/duckdb/duckdb/blob/671faf92411182f81dce42ac43de8bfb05d9909e/src/planner/binder/tableref/bind_pivot.cpp#L61-L62
            exp.Pivot: transforms.preprocess([transforms.unqualify_columns]),
            exp.RegexpExtract: regexp_extract_sql,
            exp.RegexpReplace: lambda self, e: self.func(
                "REGEXP_REPLACE",
                e.this,
                e.expression,
                e.args.get("replacement"),
                e.args.get("modifiers"),
            ),
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.RegexpSplit: rename_func("STR_SPLIT_REGEX"),
            exp.Rand: rename_func("RANDOM"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.Split: rename_func("STR_SPLIT"),
            exp.SortArray: _sort_array_sql,
            exp.StrPosition: str_position_sql,
            exp.StrToDate: lambda self, e: f"CAST({str_to_time_sql(self, e)} AS DATE)",
            exp.StrToTime: str_to_time_sql,
            exp.StrToUnix: lambda self, e: self.func(
                "EPOCH", self.func("STRPTIME", e.this, self.format_time(e))
            ),
            exp.Struct: _struct_sql,
            exp.TimeAdd: _date_delta_sql,
            exp.Timestamp: no_timestamp_sql,
            exp.TimestampDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.unit), e.expression, e.this
            ),
            exp.TimestampTrunc: timestamptrunc_sql(),
            exp.TimeStrToDate: lambda self, e: self.sql(exp.cast(e.this, exp.DataType.Type.DATE)),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: self.func(
                "EPOCH", exp.cast(e.this, exp.DataType.Type.TIMESTAMP)
            ),
            exp.TimeToStr: lambda self, e: self.func("STRFTIME", e.this, self.format_time(e)),
            exp.TimeToUnix: rename_func("EPOCH"),
            exp.TsOrDiToDi: lambda self,
            e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS TEXT), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsDiff: lambda self, e: self.func(
                "DATE_DIFF",
                f"'{e.args.get('unit') or 'DAY'}'",
                exp.cast(e.expression, exp.DataType.Type.TIMESTAMP),
                exp.cast(e.this, exp.DataType.Type.TIMESTAMP),
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "STRFTIME", self.func("TO_TIMESTAMP", e.this), self.format_time(e)
            ),
            exp.UnixToTime: _unix_to_time_sql,
            exp.UnixToTimeStr: lambda self, e: f"CAST(TO_TIMESTAMP({self.sql(e, 'this')}) AS TEXT)",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.Xor: bool_xor_sql,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
            exp.JSONPathWildcard,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.NCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
            exp.DataType.Type.UINT: "UINTEGER",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.ROWVERSION: "BLOB",
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.TIMESTAMP_S: "TIMESTAMP_S",
            exp.DataType.Type.TIMESTAMP_MS: "TIMESTAMP_MS",
            exp.DataType.Type.TIMESTAMP_NS: "TIMESTAMP_NS",
        }

        UNWRAPPED_INTERVAL_VALUES = (exp.Literal, exp.Paren)

        # DuckDB doesn't generally support CREATE TABLE .. properties
        # https://duckdb.org/docs/sql/statements/create_table.html
        PROPERTIES_LOCATION = {
            prop: exp.Properties.Location.UNSUPPORTED
            for prop in generator.Generator.PROPERTIES_LOCATION
        }

        # There are a few exceptions (e.g. temporary tables) which are supported or
        # can be transpiled to DuckDB, so we explicitly override them accordingly
        PROPERTIES_LOCATION[exp.LikeProperty] = exp.Properties.Location.POST_SCHEMA
        PROPERTIES_LOCATION[exp.TemporaryProperty] = exp.Properties.Location.POST_CREATE

        def timefromparts_sql(self, expression: exp.TimeFromParts) -> str:
            nano = expression.args.get("nano")
            if nano is not None:
                expression.set(
                    "sec", expression.args["sec"] + nano.pop() / exp.Literal.number(1000000000.0)
                )

            return rename_func("MAKE_TIME")(self, expression)

        def timestampfromparts_sql(self, expression: exp.TimestampFromParts) -> str:
            sec = expression.args["sec"]

            milli = expression.args.get("milli")
            if milli is not None:
                sec += milli.pop() / exp.Literal.number(1000.0)

            nano = expression.args.get("nano")
            if nano is not None:
                sec += nano.pop() / exp.Literal.number(1000000000.0)

            if milli or nano:
                expression.set("sec", sec)

            return rename_func("MAKE_TIMESTAMP")(self, expression)

        def tablesample_sql(
            self,
            expression: exp.TableSample,
            sep: str = " AS ",
            tablesample_keyword: t.Optional[str] = None,
        ) -> str:
            if not isinstance(expression.parent, exp.Select):
                # This sample clause only applies to a single source, not the entire resulting relation
                tablesample_keyword = "TABLESAMPLE"

            if expression.args.get("size"):
                method = expression.args.get("method")
                if method and method.name.upper() != "RESERVOIR":
                    self.unsupported(
                        f"Sampling method {method} is not supported with a discrete sample count, "
                        "defaulting to reservoir sampling"
                    )
                    expression.set("method", exp.var("RESERVOIR"))

            return super().tablesample_sql(
                expression, sep=sep, tablesample_keyword=tablesample_keyword
            )

        def interval_sql(self, expression: exp.Interval) -> str:
            multiplier: t.Optional[int] = None
            unit = expression.text("unit").lower()

            if unit.startswith("week"):
                multiplier = 7
            if unit.startswith("quarter"):
                multiplier = 90

            if multiplier:
                return f"({multiplier} * {super().interval_sql(exp.Interval(this=expression.this, unit=exp.var('DAY')))})"

            return super().interval_sql(expression)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            if isinstance(expression.parent, exp.UserDefinedFunction):
                return self.sql(expression, "this")
            return super().columndef_sql(expression, sep)

        def join_sql(self, expression: exp.Join) -> str:
            if (
                expression.side == "LEFT"
                and not expression.args.get("on")
                and isinstance(expression.this, exp.Unnest)
            ):
                # Some dialects support `LEFT JOIN UNNEST(...)` without an explicit ON clause
                # DuckDB doesn't, but we can just add a dummy ON clause that is always true
                return super().join_sql(expression.on(exp.true()))

            return super().join_sql(expression)

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            # GENERATE_SERIES(a, b) -> [a, b], RANGE(a, b) -> [a, b)
            if expression.args.get("is_end_exclusive"):
                expression.set("is_end_exclusive", None)
                return rename_func("RANGE")(self, expression)

            return super().generateseries_sql(expression)

        def bracket_sql(self, expression: exp.Bracket) -> str:
            this = expression.this
            if isinstance(this, exp.Array):
                this.replace(exp.paren(this))

            bracket = super().bracket_sql(expression)

            if not expression.args.get("returns_list_for_maps"):
                if not this.type:
                    from sqlglot.optimizer.annotate_types import annotate_types

                    this = annotate_types(this)

                if this.is_type(exp.DataType.Type.MAP):
                    bracket = f"({bracket})[1]"

            return bracket

        def withingroup_sql(self, expression: exp.WithinGroup) -> str:
            expression_sql = self.sql(expression, "expression")

            func = expression.this
            if isinstance(func, exp.PERCENTILES):
                # Make the order key the first arg and slide the fraction to the right
                # https://duckdb.org/docs/sql/aggregates#ordered-set-aggregate-functions
                order_col = expression.find(exp.Ordered)
                if order_col:
                    func.set("expression", func.this)
                    func.set("this", order_col.this)

            this = self.sql(expression, "this").rstrip(")")

            return f"{this}{expression_sql})"
