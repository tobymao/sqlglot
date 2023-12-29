from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    approx_count_distinct_sql,
    arg_max_or_min_no_count,
    arrow_json_extract_scalar_sql,
    arrow_json_extract_sql,
    binary_from_function,
    bool_xor_sql,
    date_trunc_to_time,
    datestrtodate_sql,
    encode_decode_sql,
    format_time_lambda,
    inline_array_sql,
    no_comment_column_constraint_sql,
    no_properties_sql,
    no_safe_divide_sql,
    no_timestamp_sql,
    pivot_column_names,
    regexp_extract_sql,
    rename_func,
    str_position_sql,
    str_to_time_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    ts_or_ds_to_date_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _ts_or_ds_add_sql(self: DuckDB.Generator, expression: exp.TsOrDsAdd) -> str:
    this = self.sql(expression, "this")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    interval = self.sql(exp.Interval(this=expression.expression, unit=unit))
    return f"CAST({this} AS {self.sql(expression.return_type)}) + {interval}"


def _date_delta_sql(self: DuckDB.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
    this = self.sql(expression, "this")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    op = "+" if isinstance(expression, exp.DateAdd) else "-"
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
        self.unsupported("DUCKDB ARRAY_SORT does not support a comparator")
    return f"ARRAY_SORT({self.sql(expression, 'this')})"


def _sort_array_sql(self: DuckDB.Generator, expression: exp.SortArray) -> str:
    this = self.sql(expression, "this")
    if expression.args.get("asc") == exp.false():
        return f"ARRAY_REVERSE_SORT({this})"
    return f"ARRAY_SORT({this})"


def _sort_array_reverse(args: t.List) -> exp.Expression:
    return exp.SortArray(this=seq_get(args, 0), asc=exp.false())


def _parse_date_diff(args: t.List) -> exp.Expression:
    return exp.DateDiff(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))


def _parse_make_timestamp(args: t.List) -> exp.Expression:
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
    for expr in expression.expressions:
        if isinstance(expr, exp.Alias):
            key = expr.alias
            value = expr.this
        else:
            key = expr.name or expr.this.name
            if isinstance(expr, exp.Bracket):
                value = expr.expressions[0]
            else:
                value = expr.expression

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
    timestamp = self.sql(expression, "this")
    if scale in (None, exp.UnixToTime.SECONDS):
        return f"TO_TIMESTAMP({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"EPOCH_MS({timestamp})"
    if scale == exp.UnixToTime.MICROS:
        return f"MAKE_TIMESTAMP({timestamp})"
    if scale == exp.UnixToTime.NANOS:
        return f"TO_TIMESTAMP({timestamp} / 1000000000)"

    self.unsupported(f"Unsupported scale for timestamp: {scale}.")
    return ""


class DuckDB(Dialect):
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    INDEX_OFFSET = 1
    CONCAT_COALESCE = True

    # https://duckdb.org/docs/sql/introduction.html#creating-a-new-table
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    class Tokenizer(tokens.Tokenizer):
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
            "PIVOT_WIDER": TokenType.PIVOT,
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

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARRAY_HAS": exp.ArrayContains.from_arg_list,
            "ARRAY_LENGTH": exp.ArraySize.from_arg_list,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "ARRAY_REVERSE_SORT": _sort_array_reverse,
            "DATEDIFF": _parse_date_diff,
            "DATE_DIFF": _parse_date_diff,
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
            "LIST_HAS": exp.ArrayContains.from_arg_list,
            "LIST_REVERSE_SORT": _sort_array_reverse,
            "LIST_SORT": exp.SortArray.from_arg_list,
            "LIST_VALUE": exp.Array.from_arg_list,
            "MAKE_TIMESTAMP": _parse_make_timestamp,
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
            "STRFTIME": format_time_lambda(exp.TimeToStr, "duckdb"),
            "STRING_SPLIT": exp.Split.from_arg_list,
            "STRING_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "STRING_TO_ARRAY": exp.Split.from_arg_list,
            "STRPTIME": format_time_lambda(exp.StrToTime, "duckdb"),
            "STRUCT_PACK": exp.Struct.from_arg_list,
            "STR_SPLIT": exp.Split.from_arg_list,
            "STR_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
            "XOR": binary_from_function(exp.BitwiseXor),
        }

        FUNCTION_PARSERS = parser.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("DECODE", None)

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.SEMI,
            TokenType.ANTI,
        }

        PLACEHOLDER_PARSERS = {
            **parser.Parser.PLACEHOLDER_PARSERS,
            TokenType.PARAMETER: lambda self: self.expression(exp.Placeholder, this=self._prev.text)
            if self._match(TokenType.NUMBER) or self._match_set(self.ID_VAR_TOKENS)
            else None,
        }

        def _parse_types(
            self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_types(
                check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
            )

            # DuckDB treats NUMERIC and DECIMAL without precision as DECIMAL(18, 3)
            # See: https://duckdb.org/docs/sql/data_types/numeric
            if (
                isinstance(this, exp.DataType)
                and this.is_type("numeric", "decimal")
                and not this.expressions
            ):
                return exp.DataType.build("DECIMAL(18, 3)")

            return this

        def _parse_struct_types(self) -> t.Optional[exp.Expression]:
            return self._parse_field_def()

        def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
            if len(aggregations) == 1:
                return super()._pivot_column_names(aggregations)
            return pivot_column_names(aggregations, dialect="duckdb")

    class Generator(generator.Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        LIMIT_FETCH = "LIMIT"
        STRUCT_DELIMITER = ("(", ")")
        RENAME_TABLE_WITH_DB = False
        NVL2_SUPPORTED = False
        SEMI_ANTI_JOIN_WITH_SIDE = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.Array: lambda self, e: self.func("ARRAY", e.expressions[0])
            if e.expressions and e.expressions[0].find(exp.Select)
            else inline_array_sql(self, e),
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.ArgMax: arg_max_or_min_no_count("ARG_MAX"),
            exp.ArgMin: arg_max_or_min_no_count("ARG_MIN"),
            exp.ArraySort: _array_sort_sql,
            exp.ArraySum: rename_func("LIST_SUM"),
            exp.BitwiseXor: rename_func("XOR"),
            exp.CommentColumnConstraint: no_comment_column_constraint_sql,
            exp.CurrentDate: lambda self, e: "CURRENT_DATE",
            exp.CurrentTime: lambda self, e: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda self, e: "CURRENT_TIMESTAMP",
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
            exp.DateToDi: lambda self, e: f"CAST(STRFTIME({self.sql(e, 'this')}, {DuckDB.DATEINT_FORMAT}) AS INT)",
            exp.Decode: lambda self, e: encode_decode_sql(self, e, "DECODE", replace=False),
            exp.DiToDate: lambda self, e: f"CAST(STRPTIME(CAST({self.sql(e, 'this')} AS TEXT), {DuckDB.DATEINT_FORMAT}) AS DATE)",
            exp.Encode: lambda self, e: encode_decode_sql(self, e, "ENCODE", replace=False),
            exp.Explode: rename_func("UNNEST"),
            exp.IntDiv: lambda self, e: self.binary(e, "//"),
            exp.IsInf: rename_func("ISINF"),
            exp.IsNan: rename_func("ISNAN"),
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONFormat: _json_format_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.MonthsBetween: lambda self, e: self.func(
                "DATEDIFF",
                "'month'",
                exp.cast(e.expression, "timestamp", copy=True),
                exp.cast(e.this, "timestamp", copy=True),
            ),
            exp.ParseJSON: rename_func("JSON"),
            exp.PercentileCont: rename_func("QUANTILE_CONT"),
            exp.PercentileDisc: rename_func("QUANTILE_DISC"),
            # DuckDB doesn't allow qualified columns inside of PIVOT expressions.
            # See: https://github.com/duckdb/duckdb/blob/671faf92411182f81dce42ac43de8bfb05d9909e/src/planner/binder/tableref/bind_pivot.cpp#L61-L62
            exp.Pivot: transforms.preprocess([transforms.unqualify_columns]),
            exp.Properties: no_properties_sql,
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
            exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.Struct: _struct_sql,
            exp.Timestamp: no_timestamp_sql,
            exp.TimestampDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.unit), e.expression, e.this
            ),
            exp.TimestampFromParts: rename_func("MAKE_TIMESTAMP"),
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: f"EPOCH(CAST({self.sql(e, 'this')} AS TIMESTAMP))",
            exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("EPOCH"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS TEXT), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsDiff: lambda self, e: self.func(
                "DATE_DIFF",
                f"'{e.args.get('unit') or 'DAY'}'",
                exp.cast(e.expression, "TIMESTAMP"),
                exp.cast(e.this, "TIMESTAMP"),
            ),
            exp.TsOrDsToDate: ts_or_ds_to_date_sql("duckdb"),
            exp.UnixToStr: lambda self, e: f"STRFTIME(TO_TIMESTAMP({self.sql(e, 'this')}), {self.format_time(e)})",
            exp.UnixToTime: _unix_to_time_sql,
            exp.UnixToTimeStr: lambda self, e: f"CAST(TO_TIMESTAMP({self.sql(e, 'this')}) AS TEXT)",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.Xor: bool_xor_sql,
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
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.TIMESTAMP_S: "TIMESTAMP_S",
            exp.DataType.Type.TIMESTAMP_MS: "TIMESTAMP_MS",
            exp.DataType.Type.TIMESTAMP_NS: "TIMESTAMP_NS",
        }

        STAR_MAPPING = {**generator.Generator.STAR_MAPPING, "except": "EXCLUDE"}

        UNWRAPPED_INTERVAL_VALUES = (exp.Column, exp.Literal, exp.Paren)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

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

        def tablesample_sql(
            self, expression: exp.TableSample, seed_prefix: str = "SEED", sep: str = " AS "
        ) -> str:
            return super().tablesample_sql(expression, seed_prefix="REPEATABLE", sep=sep)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            if isinstance(expression.parent, exp.UserDefinedFunction):
                return self.sql(expression, "this")
            return super().columndef_sql(expression, sep)

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"${expression.name}" if expression.name else "?"
