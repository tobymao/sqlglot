from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    bool_xor_sql,
    date_trunc_to_time,
    encode_decode_sql,
    format_time_lambda,
    if_sql,
    left_to_substring_sql,
    no_ilike_sql,
    no_pivot_sql,
    no_safe_divide_sql,
    no_timestamp_sql,
    regexp_extract_sql,
    rename_func,
    right_to_substring_sql,
    struct_extract_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import apply_index_offset, seq_get
from sqlglot.tokens import TokenType


def _approx_distinct_sql(self: Presto.Generator, expression: exp.ApproxDistinct) -> str:
    accuracy = expression.args.get("accuracy")
    accuracy = ", " + self.sql(accuracy) if accuracy else ""
    return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"


def _explode_to_unnest_sql(self: Presto.Generator, expression: exp.Lateral) -> str:
    if isinstance(expression.this, exp.Explode):
        return self.sql(
            exp.Join(
                this=exp.Unnest(
                    expressions=[expression.this.this],
                    alias=expression.args.get("alias"),
                    offset=isinstance(expression.this, exp.Posexplode),
                ),
                kind="cross",
            )
        )
    return self.lateral_sql(expression)


def _initcap_sql(self: Presto.Generator, expression: exp.Initcap) -> str:
    regex = r"(\w)(\w*)"
    return f"REGEXP_REPLACE({self.sql(expression, 'this')}, '{regex}', x -> UPPER(x[1]) || LOWER(x[2]))"


def _no_sort_array(self: Presto.Generator, expression: exp.SortArray) -> str:
    if expression.args.get("asc") == exp.false():
        comparator = "(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END"
    else:
        comparator = None
    return self.func("ARRAY_SORT", expression.this, comparator)


def _schema_sql(self: Presto.Generator, expression: exp.Schema) -> str:
    if isinstance(expression.parent, exp.Property):
        columns = ", ".join(f"'{c.name}'" for c in expression.expressions)
        return f"ARRAY[{columns}]"

    if expression.parent:
        for schema in expression.parent.find_all(exp.Schema):
            column_defs = schema.find_all(exp.ColumnDef)
            if column_defs and isinstance(schema.parent, exp.Property):
                expression.expressions.extend(column_defs)

    return self.schema_sql(expression)


def _quantile_sql(self: Presto.Generator, expression: exp.Quantile) -> str:
    self.unsupported("Presto does not support exact quantiles")
    return f"APPROX_PERCENTILE({self.sql(expression, 'this')}, {self.sql(expression, 'quantile')})"


def _str_to_time_sql(
    self: Presto.Generator, expression: exp.StrToDate | exp.StrToTime | exp.TsOrDsToDate
) -> str:
    return f"DATE_PARSE({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_to_date_sql(self: Presto.Generator, expression: exp.TsOrDsToDate) -> str:
    time_format = self.format_time(expression)
    if time_format and time_format not in (Presto.TIME_FORMAT, Presto.DATE_FORMAT):
        return exp.cast(_str_to_time_sql(self, expression), "DATE").sql(dialect="presto")
    return exp.cast(exp.cast(expression.this, "TIMESTAMP", copy=True), "DATE").sql(dialect="presto")


def _ts_or_ds_add_sql(self: Presto.Generator, expression: exp.TsOrDsAdd) -> str:
    this = expression.this

    if not isinstance(this, exp.CurrentDate):
        this = exp.cast(exp.cast(expression.this, "TIMESTAMP", copy=True), "DATE")

    return self.func(
        "DATE_ADD",
        exp.Literal.string(expression.text("unit") or "day"),
        expression.expression,
        this,
    )


def _approx_percentile(args: t.List) -> exp.Expression:
    if len(args) == 4:
        return exp.ApproxQuantile(
            this=seq_get(args, 0),
            weight=seq_get(args, 1),
            quantile=seq_get(args, 2),
            accuracy=seq_get(args, 3),
        )
    if len(args) == 3:
        return exp.ApproxQuantile(
            this=seq_get(args, 0), quantile=seq_get(args, 1), accuracy=seq_get(args, 2)
        )
    return exp.ApproxQuantile.from_arg_list(args)


def _from_unixtime(args: t.List) -> exp.Expression:
    if len(args) == 3:
        return exp.UnixToTime(
            this=seq_get(args, 0),
            hours=seq_get(args, 1),
            minutes=seq_get(args, 2),
        )
    if len(args) == 2:
        return exp.UnixToTime(this=seq_get(args, 0), zone=seq_get(args, 1))

    return exp.UnixToTime.from_arg_list(args)


def _parse_element_at(args: t.List) -> exp.SafeBracket:
    this = seq_get(args, 0)
    index = seq_get(args, 1)
    assert isinstance(this, exp.Expression) and isinstance(index, exp.Expression)
    return exp.SafeBracket(this=this, expressions=apply_index_offset(this, [index], -1))


def _unnest_sequence(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Table):
        if isinstance(expression.this, exp.GenerateSeries):
            unnest = exp.Unnest(expressions=[expression.this])

            if expression.alias:
                return exp.alias_(unnest, alias="_u", table=[expression.alias], copy=False)
            return unnest
    return expression


def _first_last_sql(self: Presto.Generator, expression: exp.First | exp.Last) -> str:
    """
    Trino doesn't support FIRST / LAST as functions, but they're valid in the context
    of MATCH_RECOGNIZE, so we need to preserve them in that case. In all other cases
    they're converted into an ARBITRARY call.

    Reference: https://trino.io/docs/current/sql/match-recognize.html#logical-navigation-functions
    """
    if isinstance(expression.find_ancestor(exp.MatchRecognize, exp.Select), exp.MatchRecognize):
        return self.function_fallback_sql(expression)

    return rename_func("ARBITRARY")(self, expression)


class Presto(Dialect):
    INDEX_OFFSET = 1
    NULL_ORDERING = "nulls_are_last"
    TIME_FORMAT = MySQL.TIME_FORMAT
    TIME_MAPPING = MySQL.TIME_MAPPING
    STRICT_STRING_CONCAT = True
    SUPPORTS_SEMI_ANTI_JOIN = False

    # https://github.com/trinodb/trino/issues/17
    # https://github.com/trinodb/trino/issues/12289
    # https://github.com/prestodb/presto/issues/2863
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = None

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "START": TokenType.BEGIN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "ROW": TokenType.STRUCT,
            "IPADDRESS": TokenType.IPADDRESS,
            "IPPREFIX": TokenType.IPPREFIX,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARBITRARY": exp.AnyValue.from_arg_list,
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "APPROX_PERCENTILE": _approx_percentile,
            "BITWISE_AND": binary_from_function(exp.BitwiseAnd),
            "BITWISE_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BITWISE_OR": binary_from_function(exp.BitwiseOr),
            "BITWISE_XOR": binary_from_function(exp.BitwiseXor),
            "CARDINALITY": exp.ArraySize.from_arg_list,
            "CONTAINS": exp.ArrayContains.from_arg_list,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "presto"),
            "DATE_PARSE": format_time_lambda(exp.StrToTime, "presto"),
            "DATE_TRUNC": date_trunc_to_time,
            "ELEMENT_AT": _parse_element_at,
            "FROM_HEX": exp.Unhex.from_arg_list,
            "FROM_UNIXTIME": _from_unixtime,
            "FROM_UTF8": lambda args: exp.Decode(
                this=seq_get(args, 0), replace=seq_get(args, 1), charset=exp.Literal.string("utf-8")
            ),
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "REGEXP_EXTRACT": lambda args: exp.RegexpExtract(
                this=seq_get(args, 0), expression=seq_get(args, 1), group=seq_get(args, 2)
            ),
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2) or exp.Literal.string(""),
            ),
            "ROW": exp.Struct.from_arg_list,
            "SEQUENCE": exp.GenerateSeries.from_arg_list,
            "SPLIT_TO_MAP": exp.StrToMap.from_arg_list,
            "STRPOS": lambda args: exp.StrPosition(
                this=seq_get(args, 0), substr=seq_get(args, 1), instance=seq_get(args, 2)
            ),
            "TO_UNIXTIME": exp.TimeToUnix.from_arg_list,
            "TO_HEX": exp.Hex.from_arg_list,
            "TO_UTF8": lambda args: exp.Encode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
        }

        FUNCTION_PARSERS = parser.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("TRIM")

    class Generator(generator.Generator):
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        IS_BOOL_ALLOWED = False
        TZ_TO_WITH_TIME_ZONE = True
        NVL2_SUPPORTED = False
        STRUCT_DELIMITER = ("(", ")")
        LIMIT_ONLY_LITERALS = True

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.LocationProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.STRUCT: "ROW",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.DATETIME64: "TIMESTAMP",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("ARBITRARY"),
            exp.ApproxDistinct: _approx_distinct_sql,
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.Array: lambda self, e: f"ARRAY[{self.expressions(e, flat=True)}]",
            exp.ArrayConcat: rename_func("CONCAT"),
            exp.ArrayContains: rename_func("CONTAINS"),
            exp.ArraySize: rename_func("CARDINALITY"),
            exp.BitwiseAnd: lambda self, e: f"BITWISE_AND({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseLeftShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_LEFT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseNot: lambda self, e: f"BITWISE_NOT({self.sql(e, 'this')})",
            exp.BitwiseOr: lambda self, e: f"BITWISE_OR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseRightShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_RIGHT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseXor: lambda self, e: f"BITWISE_XOR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Cast: transforms.preprocess([transforms.epoch_cast_to_ts]),
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DateAdd: lambda self, e: self.func(
                "DATE_ADD", exp.Literal.string(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateStrToDate: lambda self, e: f"CAST(DATE_PARSE({self.sql(e, 'this')}, {Presto.DATE_FORMAT}) AS DATE)",
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Presto.DATEINT_FORMAT}) AS INT)",
            exp.DateSub: lambda self, e: self.func(
                "DATE_ADD",
                exp.Literal.string(e.text("unit") or "day"),
                e.expression * -1,
                e.this,
            ),
            exp.Decode: lambda self, e: encode_decode_sql(self, e, "FROM_UTF8"),
            exp.DiToDate: lambda self, e: f"CAST(DATE_PARSE(CAST({self.sql(e, 'this')} AS VARCHAR), {Presto.DATEINT_FORMAT}) AS DATE)",
            exp.Encode: lambda self, e: encode_decode_sql(self, e, "TO_UTF8"),
            exp.FileFormatProperty: lambda self, e: f"FORMAT='{e.name.upper()}'",
            exp.First: _first_last_sql,
            exp.Group: transforms.preprocess([transforms.unalias_group]),
            exp.GroupConcat: lambda self, e: self.func(
                "ARRAY_JOIN", self.func("ARRAY_AGG", e.this), e.args.get("separator")
            ),
            exp.Hex: rename_func("TO_HEX"),
            exp.If: if_sql(),
            exp.ILike: no_ilike_sql,
            exp.Initcap: _initcap_sql,
            exp.ParseJSON: rename_func("JSON_PARSE"),
            exp.Last: _first_last_sql,
            exp.Lateral: _explode_to_unnest_sql,
            exp.Left: left_to_substring_sql,
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Pivot: no_pivot_sql,
            exp.Quantile: _quantile_sql,
            exp.RegexpExtract: regexp_extract_sql,
            exp.Right: right_to_substring_sql,
            exp.SafeBracket: lambda self, e: self.func(
                "ELEMENT_AT", e.this, seq_get(apply_index_offset(e.this, e.expressions, 1), 0)
            ),
            exp.SafeDivide: no_safe_divide_sql,
            exp.Schema: _schema_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.eliminate_distinct_on,
                    transforms.explode_to_unnest(1),
                    transforms.eliminate_semi_and_anti_joins,
                ]
            ),
            exp.SortArray: _no_sort_array,
            exp.StrPosition: rename_func("STRPOS"),
            exp.StrToDate: lambda self, e: f"CAST({_str_to_time_sql(self, e)} AS DATE)",
            exp.StrToMap: rename_func("SPLIT_TO_MAP"),
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.StructExtract: struct_extract_sql,
            exp.Table: transforms.preprocess([_unnest_sequence]),
            exp.Timestamp: no_timestamp_sql,
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToDate: timestrtotime_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {Presto.TIME_FORMAT}))",
            exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("TO_UNIXTIME"),
            exp.TryCast: transforms.preprocess([transforms.epoch_cast_to_ts]),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
            exp.Unhex: rename_func("FROM_HEX"),
            exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: lambda self, e: f"CAST(FROM_UNIXTIME({self.sql(e, 'this')}) AS VARCHAR)",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.With: transforms.preprocess([transforms.add_recursive_cte_column_names]),
            exp.WithinGroup: transforms.preprocess(
                [transforms.remove_within_group_for_percentiles]
            ),
            exp.Xor: bool_xor_sql,
        }

        def struct_sql(self, expression: exp.Struct) -> str:
            if any(isinstance(arg, (exp.EQ, exp.Slice)) for arg in expression.expressions):
                self.unsupported("Struct with key-value definitions is unsupported.")
                return self.function_fallback_sql(expression)

            return rename_func("ROW")(self, expression)

        def interval_sql(self, expression: exp.Interval) -> str:
            unit = self.sql(expression, "unit")
            if expression.this and unit.lower().startswith("week"):
                return f"({expression.this.name} * INTERVAL '7' day)"
            return super().interval_sql(expression)

        def transaction_sql(self, expression: exp.Transaction) -> str:
            modes = expression.args.get("modes")
            modes = f" {', '.join(modes)}" if modes else ""
            return f"START TRANSACTION{modes}"

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            start = expression.args["start"]
            end = expression.args["end"]
            step = expression.args.get("step")

            if isinstance(start, exp.Cast):
                target_type = start.to
            elif isinstance(end, exp.Cast):
                target_type = end.to
            else:
                target_type = None

            if target_type and target_type.is_type("timestamp"):
                if target_type is start.to:
                    end = exp.cast(end, target_type)
                else:
                    start = exp.cast(start, target_type)

            return self.func("SEQUENCE", start, end, step)

        def offset_limit_modifiers(
            self, expression: exp.Expression, fetch: bool, limit: t.Optional[exp.Fetch | exp.Limit]
        ) -> t.List[str]:
            return [
                self.sql(expression, "offset"),
                self.sql(limit),
            ]

        def create_sql(self, expression: exp.Create) -> str:
            """
            Presto doesn't support CREATE VIEW with expressions (ex: `CREATE VIEW x (cola)` then `(cola)` is the expression),
            so we need to remove them
            """
            kind = expression.args["kind"]
            schema = expression.this
            if kind == "VIEW" and schema.expressions:
                expression.this.set("expressions", None)
            return super().create_sql(expression)
