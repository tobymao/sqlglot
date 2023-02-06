from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import create_with_partitions_sql, rename_func, trim_sql
from sqlglot.dialects.hive import Hive
from sqlglot.helper import seq_get


def _create_sql(self, e):
    kind = e.args.get("kind")
    temporary = e.args.get("temporary")

    if kind.upper() == "TABLE" and temporary is True:
        return f"CREATE TEMPORARY VIEW {self.sql(e, 'this')} AS {self.sql(e, 'expression')}"
    return create_with_partitions_sql(self, e)


def _map_sql(self, expression):
    keys = self.sql(expression.args["keys"])
    values = self.sql(expression.args["values"])
    return f"MAP_FROM_ARRAYS({keys}, {values})"


def _str_to_date(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Hive.date_format:
        return f"TO_DATE({this})"
    return f"TO_DATE({this}, {time_format})"


def _unix_to_time(self, expression):
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale is None:
        return f"FROM_UNIXTIME({timestamp})"
    if scale == exp.UnixToTime.SECONDS:
        return f"TIMESTAMP_SECONDS({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TIMESTAMP_MILLIS({timestamp})"
    if scale == exp.UnixToTime.MICROS:
        return f"TIMESTAMP_MICROS({timestamp})"

    raise ValueError("Improper scale for timestamp")


class Spark(Hive):
    class Parser(Hive.Parser):
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,  # type: ignore
            "MAP_FROM_ARRAYS": exp.Map.from_arg_list,
            "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
            "LEFT": lambda args: exp.Substring(
                this=seq_get(args, 0),
                start=exp.Literal.number(1),
                length=seq_get(args, 1),
            ),
            "SHIFTLEFT": lambda args: exp.BitwiseLeftShift(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "SHIFTRIGHT": lambda args: exp.BitwiseRightShift(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "RIGHT": lambda args: exp.Substring(
                this=seq_get(args, 0),
                start=exp.Sub(
                    this=exp.Length(this=seq_get(args, 0)),
                    expression=exp.Add(this=seq_get(args, 1), expression=exp.Literal.number(1)),
                ),
                length=seq_get(args, 1),
            ),
            "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
            "IIF": exp.If.from_arg_list,
            "AGGREGATE": exp.Reduce.from_arg_list,
            "DAYOFWEEK": lambda args: exp.DayOfWeek(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            ),
            "DAYOFMONTH": lambda args: exp.DayOfMonth(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            ),
            "DAYOFYEAR": lambda args: exp.DayOfYear(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            ),
            "WEEKOFYEAR": lambda args: exp.WeekOfYear(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            ),
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,  # type: ignore
            "BROADCAST": lambda self: self._parse_join_hint("BROADCAST"),
            "BROADCASTJOIN": lambda self: self._parse_join_hint("BROADCASTJOIN"),
            "MAPJOIN": lambda self: self._parse_join_hint("MAPJOIN"),
            "MERGE": lambda self: self._parse_join_hint("MERGE"),
            "SHUFFLEMERGE": lambda self: self._parse_join_hint("SHUFFLEMERGE"),
            "MERGEJOIN": lambda self: self._parse_join_hint("MERGEJOIN"),
            "SHUFFLE_HASH": lambda self: self._parse_join_hint("SHUFFLE_HASH"),
            "SHUFFLE_REPLICATE_NL": lambda self: self._parse_join_hint("SHUFFLE_REPLICATE_NL"),
        }

        def _parse_add_column(self):
            return self._match_text_seq("ADD", "COLUMNS") and self._parse_schema()

        def _parse_drop_column(self):
            return self._match_text_seq("DROP", "COLUMNS") and self.expression(
                exp.Drop,
                this=self._parse_schema(),
                kind="COLUMNS",
            )

    class Generator(Hive.Generator):
        TYPE_MAPPING = {
            **Hive.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TINYINT: "BYTE",
            exp.DataType.Type.SMALLINT: "SHORT",
            exp.DataType.Type.BIGINT: "LONG",
        }

        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.EngineProperty: exp.Properties.Location.UNSUPPORTED,
            exp.AutoIncrementProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CharacterSetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CollateProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TRANSFORMS = {
            **Hive.Generator.TRANSFORMS,  # type: ignore
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.FileFormatProperty: lambda self, e: f"USING {e.name.upper()}",
            exp.ArraySum: lambda self, e: f"AGGREGATE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.BitwiseLeftShift: rename_func("SHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("SHIFTRIGHT"),
            exp.DateTrunc: rename_func("TRUNC"),
            exp.Hint: lambda self, e: f" /*+ {self.expressions(e).strip()} */",
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: _unix_to_time,
            exp.Create: _create_sql,
            exp.Map: _map_sql,
            exp.Reduce: rename_func("AGGREGATE"),
            exp.StructKwarg: lambda self, e: f"{self.sql(e, 'this')}: {self.sql(e, 'expression')}",
            exp.TimestampTrunc: lambda self, e: f"DATE_TRUNC({self.sql(e, 'unit')}, {self.sql(e, 'this')})",
            exp.Trim: trim_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.DateFromParts: rename_func("MAKE_DATE"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.AtTimeZone: lambda self, e: f"FROM_UTC_TIMESTAMP({self.sql(e, 'this')}, {self.sql(e, 'zone')})",
        }
        TRANSFORMS.pop(exp.ArraySort)
        TRANSFORMS.pop(exp.ILike)

        WRAP_DERIVED_VALUES = False

        def cast_sql(self, expression: exp.Cast) -> str:
            if isinstance(expression.this, exp.Cast) and expression.this.is_type(
                exp.DataType.Type.JSON
            ):
                schema = f"'{self.sql(expression, 'to')}'"
                return f"FROM_JSON({self.format_args(self.sql(expression.this, 'this'), schema)})"
            if expression.to.is_type(exp.DataType.Type.JSON):
                return f"TO_JSON({self.sql(expression, 'this')})"

            return super(Spark.Generator, self).cast_sql(expression)

    class Tokenizer(Hive.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
