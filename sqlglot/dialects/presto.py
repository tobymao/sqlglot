from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    date_trunc_to_time,
    format_time_lambda,
    if_sql,
    no_ilike_sql,
    no_pivot_sql,
    no_safe_divide_sql,
    rename_func,
    struct_extract_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.errors import UnsupportedError
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _approx_distinct_sql(self: generator.Generator, expression: exp.ApproxDistinct) -> str:
    accuracy = expression.args.get("accuracy")
    accuracy = ", " + self.sql(accuracy) if accuracy else ""
    return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"


def _datatype_sql(self: generator.Generator, expression: exp.DataType) -> str:
    sql = self.datatype_sql(expression)
    if expression.this == exp.DataType.Type.TIMESTAMPTZ:
        sql = f"{sql} WITH TIME ZONE"
    return sql


def _explode_to_unnest_sql(self: generator.Generator, expression: exp.Lateral) -> str:
    if isinstance(expression.this, (exp.Explode, exp.Posexplode)):
        return self.sql(
            exp.Join(
                this=exp.Unnest(
                    expressions=[expression.this.this],
                    alias=expression.args.get("alias"),
                    ordinality=isinstance(expression.this, exp.Posexplode),
                ),
                kind="cross",
            )
        )
    return self.lateral_sql(expression)


def _initcap_sql(self: generator.Generator, expression: exp.Initcap) -> str:
    regex = r"(\w)(\w*)"
    return f"REGEXP_REPLACE({self.sql(expression, 'this')}, '{regex}', x -> UPPER(x[1]) || LOWER(x[2]))"


def _decode_sql(self: generator.Generator, expression: exp.Decode) -> str:
    _ensure_utf8(expression.args["charset"])
    return self.func("FROM_UTF8", expression.this, expression.args.get("replace"))


def _encode_sql(self: generator.Generator, expression: exp.Encode) -> str:
    _ensure_utf8(expression.args["charset"])
    return f"TO_UTF8({self.sql(expression, 'this')})"


def _no_sort_array(self: generator.Generator, expression: exp.SortArray) -> str:
    if expression.args.get("asc") == exp.false():
        comparator = "(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END"
    else:
        comparator = None
    return self.func("ARRAY_SORT", expression.this, comparator)


def _schema_sql(self: generator.Generator, expression: exp.Schema) -> str:
    if isinstance(expression.parent, exp.Property):
        columns = ", ".join(f"'{c.name}'" for c in expression.expressions)
        return f"ARRAY[{columns}]"

    if expression.parent:
        for schema in expression.parent.find_all(exp.Schema):
            if isinstance(schema.parent, exp.Property):
                expression = expression.copy()
                expression.expressions.extend(schema.expressions)

    return self.schema_sql(expression)


def _quantile_sql(self: generator.Generator, expression: exp.Quantile) -> str:
    self.unsupported("Presto does not support exact quantiles")
    return f"APPROX_PERCENTILE({self.sql(expression, 'this')}, {self.sql(expression, 'quantile')})"


def _str_to_time_sql(
    self: generator.Generator, expression: exp.StrToDate | exp.StrToTime | exp.TsOrDsToDate
) -> str:
    return f"DATE_PARSE({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_to_date_sql(self: generator.Generator, expression: exp.TsOrDsToDate) -> str:
    time_format = self.format_time(expression)
    if time_format and time_format not in (Presto.time_format, Presto.date_format):
        return f"CAST({_str_to_time_sql(self, expression)} AS DATE)"
    return f"CAST(SUBSTR(CAST({self.sql(expression, 'this')} AS VARCHAR), 1, 10) AS DATE)"


def _ts_or_ds_add_sql(self: generator.Generator, expression: exp.TsOrDsAdd) -> str:
    this = expression.this

    if not isinstance(this, exp.CurrentDate):
        this = self.func(
            "DATE_PARSE",
            self.func(
                "SUBSTR",
                this if this.is_string else exp.cast(this, "VARCHAR"),
                exp.Literal.number(1),
                exp.Literal.number(10),
            ),
            Presto.date_format,
        )

    return self.func(
        "DATE_ADD",
        exp.Literal.string(expression.text("unit") or "day"),
        expression.expression,
        this,
    )


def _ensure_utf8(charset: exp.Literal) -> None:
    if charset.name.lower() != "utf-8":
        raise UnsupportedError(f"Unsupported charset {charset}")


def _approx_percentile(args: t.Sequence) -> exp.Expression:
    if len(args) == 4:
        return exp.ApproxQuantile(
            this=seq_get(args, 0),
            weight=seq_get(args, 1),
            quantile=seq_get(args, 2),
            accuracy=seq_get(args, 3),
        )
    if len(args) == 3:
        return exp.ApproxQuantile(
            this=seq_get(args, 0),
            quantile=seq_get(args, 1),
            accuracy=seq_get(args, 2),
        )
    return exp.ApproxQuantile.from_arg_list(args)


def _from_unixtime(args: t.Sequence) -> exp.Expression:
    if len(args) == 3:
        return exp.UnixToTime(
            this=seq_get(args, 0),
            hours=seq_get(args, 1),
            minutes=seq_get(args, 2),
        )
    if len(args) == 2:
        return exp.UnixToTime(
            this=seq_get(args, 0),
            zone=seq_get(args, 1),
        )
    return exp.UnixToTime.from_arg_list(args)


def _unnest_sequence(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Table):
        if isinstance(expression.this, exp.GenerateSeries):
            unnest = exp.Unnest(expressions=[expression.this])

            if expression.alias:
                return exp.alias_(
                    unnest,
                    alias="_u",
                    table=[expression.alias],
                    copy=False,
                )
            return unnest
    return expression


class Presto(Dialect):
    index_offset = 1
    null_ordering = "nulls_are_last"
    time_format = MySQL.time_format  # type: ignore
    time_mapping = MySQL.time_mapping  # type: ignore

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "START": TokenType.BEGIN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "ROW": TokenType.STRUCT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "APPROX_PERCENTILE": _approx_percentile,
            "CARDINALITY": exp.ArraySize.from_arg_list,
            "CONTAINS": exp.ArrayContains.from_arg_list,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "presto"),
            "DATE_PARSE": format_time_lambda(exp.StrToTime, "presto"),
            "DATE_TRUNC": date_trunc_to_time,
            "FROM_HEX": exp.Unhex.from_arg_list,
            "FROM_UNIXTIME": _from_unixtime,
            "FROM_UTF8": lambda args: exp.Decode(
                this=seq_get(args, 0), replace=seq_get(args, 1), charset=exp.Literal.string("utf-8")
            ),
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "SEQUENCE": exp.GenerateSeries.from_arg_list,
            "STRPOS": lambda args: exp.StrPosition(
                this=seq_get(args, 0),
                substr=seq_get(args, 1),
                instance=seq_get(args, 2),
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
        STRUCT_DELIMITER = ("(", ")")

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.LocationProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.STRUCT: "ROW",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.ApproxDistinct: _approx_distinct_sql,
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
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
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DataType: _datatype_sql,
            exp.DateAdd: lambda self, e: self.func(
                "DATE_ADD", exp.Literal.string(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateStrToDate: lambda self, e: f"CAST(DATE_PARSE({self.sql(e, 'this')}, {Presto.date_format}) AS DATE)",
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Presto.dateint_format}) AS INT)",
            exp.Decode: _decode_sql,
            exp.DiToDate: lambda self, e: f"CAST(DATE_PARSE(CAST({self.sql(e, 'this')} AS VARCHAR), {Presto.dateint_format}) AS DATE)",
            exp.Encode: _encode_sql,
            exp.FileFormatProperty: lambda self, e: f"FORMAT='{e.name.upper()}'",
            exp.Group: transforms.preprocess([transforms.unalias_group]),
            exp.Hex: rename_func("TO_HEX"),
            exp.If: if_sql,
            exp.ILike: no_ilike_sql,
            exp.Initcap: _initcap_sql,
            exp.Lateral: _explode_to_unnest_sql,
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Pivot: no_pivot_sql,
            exp.Quantile: _quantile_sql,
            exp.SafeDivide: no_safe_divide_sql,
            exp.Schema: _schema_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.eliminate_distinct_on,
                    transforms.explode_to_unnest,
                ]
            ),
            exp.SortArray: _no_sort_array,
            exp.StrPosition: rename_func("STRPOS"),
            exp.StrToDate: lambda self, e: f"CAST({_str_to_time_sql(self, e)} AS DATE)",
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.StructExtract: struct_extract_sql,
            exp.Table: transforms.preprocess([_unnest_sequence]),
            exp.TimestampTrunc: timestamptrunc_sql,
            exp.TimeStrToDate: timestrtotime_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {Presto.time_format}))",
            exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("TO_UNIXTIME"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
            exp.Unhex: rename_func("FROM_HEX"),
            exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: lambda self, e: f"CAST(FROM_UNIXTIME({self.sql(e, 'this')}) AS VARCHAR)",
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WithinGroup: transforms.preprocess(
                [transforms.remove_within_group_for_percentiles]
            ),
        }

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

            if target_type and target_type.is_type(exp.DataType.Type.TIMESTAMP):
                to = target_type.copy()

                if target_type is start.to:
                    end = exp.Cast(this=end, to=to)
                else:
                    start = exp.Cast(this=start, to=to)

            return self.func("SEQUENCE", start, end, step)
