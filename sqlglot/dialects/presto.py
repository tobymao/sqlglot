from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    date_trunc_to_time,
    format_time_lambda,
    if_sql,
    no_ilike_sql,
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


def _approx_distinct_sql(self, expression):
    accuracy = expression.args.get("accuracy")
    accuracy = ", " + self.sql(accuracy) if accuracy else ""
    return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"


def _datatype_sql(self, expression):
    sql = self.datatype_sql(expression)
    if expression.this == exp.DataType.Type.TIMESTAMPTZ:
        sql = f"{sql} WITH TIME ZONE"
    return sql


def _explode_to_unnest_sql(self, expression):
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


def _initcap_sql(self, expression):
    regex = r"(\w)(\w*)"
    return f"REGEXP_REPLACE({self.sql(expression, 'this')}, '{regex}', x -> UPPER(x[1]) || LOWER(x[2]))"


def _decode_sql(self, expression):
    _ensure_utf8(expression.args.get("charset"))
    return self.func("FROM_UTF8", expression.this, expression.args.get("replace"))


def _encode_sql(self, expression):
    _ensure_utf8(expression.args.get("charset"))
    return f"TO_UTF8({self.sql(expression, 'this')})"


def _no_sort_array(self, expression):
    if expression.args.get("asc") == exp.false():
        comparator = "(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END"
    else:
        comparator = None
    return self.func("ARRAY_SORT", expression.this, comparator)


def _schema_sql(self, expression):
    if isinstance(expression.parent, exp.Property):
        columns = ", ".join(f"'{c.name}'" for c in expression.expressions)
        return f"ARRAY[{columns}]"

    for schema in expression.parent.find_all(exp.Schema):
        if isinstance(schema.parent, exp.Property):
            expression = expression.copy()
            expression.expressions.extend(schema.expressions)

    return self.schema_sql(expression)


def _quantile_sql(self, expression):
    self.unsupported("Presto does not support exact quantiles")
    return f"APPROX_PERCENTILE({self.sql(expression, 'this')}, {self.sql(expression, 'quantile')})"


def _str_to_time_sql(self, expression):
    return f"DATE_PARSE({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_to_date_sql(self, expression):
    time_format = self.format_time(expression)
    if time_format and time_format not in (Presto.time_format, Presto.date_format):
        return f"CAST({_str_to_time_sql(self, expression)} AS DATE)"
    return f"CAST(SUBSTR(CAST({self.sql(expression, 'this')} AS VARCHAR), 1, 10) AS DATE)"


def _ts_or_ds_add_sql(self, expression):
    return self.func(
        "DATE_ADD",
        exp.Literal.string(expression.text("unit") or "day"),
        expression.expression,
        self.func(
            "DATE_PARSE",
            self.func("SUBSTR", expression.this, exp.Literal.number(1), exp.Literal.number(10)),
            Presto.date_format,
        ),
    )


def _sequence_sql(self, expression):
    start = expression.args["start"]
    end = expression.args["end"]
    step = expression.args.get("step", 1)  # Postgres defaults to 1 for generate_series

    target_type = None

    if isinstance(start, exp.Cast):
        target_type = start.to
    elif isinstance(end, exp.Cast):
        target_type = end.to

    if target_type and target_type.this == exp.DataType.Type.TIMESTAMP:
        to = target_type.copy()

        if target_type is start.to:
            end = exp.Cast(this=end, to=to)
        else:
            start = exp.Cast(this=start, to=to)

    return self.func("SEQUENCE", start, end, step)


def _ensure_utf8(charset):
    if charset.name.lower() != "utf-8":
        raise UnsupportedError(f"Unsupported charset {charset}")


def _approx_percentile(args):
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


def _from_unixtime(args):
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


class Presto(Dialect):
    index_offset = 1
    null_ordering = "nulls_are_last"
    time_format = MySQL.time_format  # type: ignore
    time_mapping = MySQL.time_mapping  # type: ignore

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "START": TokenType.BEGIN,
            "ROW": TokenType.STRUCT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
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
            "FROM_UNIXTIME": _from_unixtime,
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "STRPOS": lambda args: exp.StrPosition(
                this=seq_get(args, 0),
                substr=seq_get(args, 1),
                instance=seq_get(args, 2),
            ),
            "TO_UNIXTIME": exp.TimeToUnix.from_arg_list,
            "APPROX_PERCENTILE": _approx_percentile,
            "FROM_HEX": exp.Unhex.from_arg_list,
            "TO_HEX": exp.Hex.from_arg_list,
            "TO_UTF8": lambda args: exp.Encode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "FROM_UTF8": lambda args: exp.Decode(
                this=seq_get(args, 0), replace=seq_get(args, 1), charset=exp.Literal.string("utf-8")
            ),
        }
        FUNCTION_PARSERS = parser.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("TRIM")

    class Generator(generator.Generator):
        STRUCT_DELIMITER = ("(", ")")

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.LocationProperty: exp.Properties.Location.UNSUPPORTED,
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
            **transforms.UNALIAS_GROUP,  # type: ignore
            **transforms.ELIMINATE_QUALIFY,  # type: ignore
            exp.ApproxDistinct: _approx_distinct_sql,
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
            exp.GenerateSeries: _sequence_sql,
            exp.Hex: rename_func("TO_HEX"),
            exp.If: if_sql,
            exp.ILike: no_ilike_sql,
            exp.Initcap: _initcap_sql,
            exp.Lateral: _explode_to_unnest_sql,
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.Quantile: _quantile_sql,
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.Schema: _schema_sql,
            exp.SortArray: _no_sort_array,
            exp.StrPosition: rename_func("STRPOS"),
            exp.StrToDate: lambda self, e: f"CAST({_str_to_time_sql(self, e)} AS DATE)",
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.StructExtract: struct_extract_sql,
            exp.TableFormatProperty: lambda self, e: f"TABLE_FORMAT='{e.name.upper()}'",
            exp.FileFormatProperty: lambda self, e: f"FORMAT='{e.name.upper()}'",
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
        }

        def transaction_sql(self, expression):
            modes = expression.args.get("modes")
            modes = f" {', '.join(modes)}" if modes else ""
            return f"START TRANSACTION{modes}"
