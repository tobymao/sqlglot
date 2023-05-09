from __future__ import annotations

import typing as t

from sqlglot import exp, parser, transforms
from sqlglot.dialects.dialect import create_with_partitions_sql, rename_func, trim_sql
from sqlglot.dialects.hive import Hive
from sqlglot.helper import seq_get


def _create_sql(self: Hive.Generator, e: exp.Create) -> str:
    kind = e.args["kind"]
    properties = e.args.get("properties")

    if kind.upper() == "TABLE" and any(
        isinstance(prop, exp.TemporaryProperty)
        for prop in (properties.expressions if properties else [])
    ):
        return f"CREATE TEMPORARY VIEW {self.sql(e, 'this')} AS {self.sql(e, 'expression')}"
    return create_with_partitions_sql(self, e)


def _map_sql(self: Hive.Generator, expression: exp.Map) -> str:
    keys = self.sql(expression.args["keys"])
    values = self.sql(expression.args["values"])
    return f"MAP_FROM_ARRAYS({keys}, {values})"


def _parse_as_cast(to_type: str) -> t.Callable[[t.Sequence], exp.Expression]:
    return lambda args: exp.Cast(this=seq_get(args, 0), to=exp.DataType.build(to_type))


def _str_to_date(self: Hive.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Hive.date_format:
        return f"TO_DATE({this})"
    return f"TO_DATE({this}, {time_format})"


def _unix_to_time_sql(self: Hive.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale is None:
        return f"CAST(FROM_UNIXTIME({timestamp}) AS TIMESTAMP)"
    if scale == exp.UnixToTime.SECONDS:
        return f"TIMESTAMP_SECONDS({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TIMESTAMP_MILLIS({timestamp})"
    if scale == exp.UnixToTime.MICROS:
        return f"TIMESTAMP_MICROS({timestamp})"

    raise ValueError("Improper scale for timestamp")


class Spark2(Hive):
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
            "DATE": lambda args: exp.Cast(this=seq_get(args, 0), to=exp.DataType.build("date")),
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1),
                unit=exp.var(seq_get(args, 0)),
            ),
            "TRUNC": lambda args: exp.DateTrunc(unit=seq_get(args, 1), this=seq_get(args, 0)),
            "BOOLEAN": _parse_as_cast("boolean"),
            "DOUBLE": _parse_as_cast("double"),
            "FLOAT": _parse_as_cast("float"),
            "INT": _parse_as_cast("int"),
            "STRING": _parse_as_cast("string"),
            "TIMESTAMP": _parse_as_cast("timestamp"),
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

        def _parse_add_column(self) -> t.Optional[exp.Expression]:
            return self._match_text_seq("ADD", "COLUMNS") and self._parse_schema()

        def _parse_drop_column(self) -> t.Optional[exp.Expression]:
            return self._match_text_seq("DROP", "COLUMNS") and self.expression(
                exp.Drop,
                this=self._parse_schema(),
                kind="COLUMNS",
            )

        def _pivot_column_names(self, pivot_columns: t.List[exp.Expression]) -> t.List[str]:
            # Spark doesn't add a suffix to the pivot columns when there's a single aggregation
            if len(pivot_columns) == 1:
                return [""]

            names = []
            for agg in pivot_columns:
                if isinstance(agg, exp.Alias):
                    names.append(agg.alias)
                else:
                    """
                    This case corresponds to aggregations without aliases being used as suffixes
                    (e.g. col_avg(foo)). We need to unquote identifiers because they're going to
                    be quoted in the base parser's `_parse_pivot` method, due to `to_identifier`.
                    Otherwise, we'd end up with `col_avg(`foo`)` (notice the double quotes).

                    Moreover, function names are lowercased in order to mimic Spark's naming scheme.
                    """
                    agg_all_unquoted = agg.transform(
                        lambda node: exp.Identifier(this=node.name, quoted=False)
                        if isinstance(node, exp.Identifier)
                        else node
                    )
                    names.append(agg_all_unquoted.sql(dialect="spark", normalize_functions="lower"))

            return names

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
            exp.ArraySum: lambda self, e: f"AGGREGATE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.AtTimeZone: lambda self, e: f"FROM_UTC_TIMESTAMP({self.sql(e, 'this')}, {self.sql(e, 'zone')})",
            exp.BitwiseLeftShift: rename_func("SHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("SHIFTRIGHT"),
            exp.Create: _create_sql,
            exp.DateFromParts: rename_func("MAKE_DATE"),
            exp.DateTrunc: lambda self, e: self.func("TRUNC", e.this, e.args.get("unit")),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.FileFormatProperty: lambda self, e: f"USING {e.name.upper()}",
            exp.Hint: lambda self, e: f" /*+ {self.expressions(e).strip()} */",
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Map: _map_sql,
            exp.Pivot: transforms.preprocess([transforms.unqualify_pivot_columns]),
            exp.Reduce: rename_func("AGGREGATE"),
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", exp.Literal.string(e.text("unit")), e.this
            ),
            exp.Trim: trim_sql,
            exp.UnixToTime: _unix_to_time_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.WithinGroup: transforms.preprocess(
                [transforms.remove_within_group_for_percentiles]
            ),
        }
        TRANSFORMS.pop(exp.ArrayJoin)
        TRANSFORMS.pop(exp.ArraySort)
        TRANSFORMS.pop(exp.ILike)

        WRAP_DERIVED_VALUES = False
        CREATE_FUNCTION_RETURN_AS = False

        def cast_sql(self, expression: exp.Cast) -> str:
            if isinstance(expression.this, exp.Cast) and expression.this.is_type(
                exp.DataType.Type.JSON
            ):
                schema = f"'{self.sql(expression, 'to')}'"
                return self.func("FROM_JSON", expression.this.this, schema)
            if expression.to.is_type(exp.DataType.Type.JSON):
                return self.func("TO_JSON", expression.this)

            return super(Hive.Generator, self).cast_sql(expression)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            return super().columndef_sql(
                expression,
                sep=": "
                if isinstance(expression.parent, exp.DataType)
                and expression.parent.is_type(exp.DataType.Type.STRUCT)
                else sep,
            )

    class Tokenizer(Hive.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
