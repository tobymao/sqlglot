from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import (
    binary_from_function,
    build_formatted_time,
    pivot_column_names,
)
from sqlglot.helper import ensure_list, mypyc_attr, seq_get
from sqlglot.parsers.hive import HiveParser
from sqlglot.parser import build_trim


def build_as_cast(to_type: str) -> t.Callable[[t.List], exp.Expr]:
    return lambda args: exp.Cast(this=seq_get(args, 0), to=exp.DataType.build(to_type))


@mypyc_attr(allow_interpreted_subclasses=True)
class Spark2Parser(HiveParser):
    TRIM_PATTERN_FIRST = True
    CHANGE_COLUMN_ALTER_SYNTAX = True

    FUNCTIONS = {
        **HiveParser.FUNCTIONS,
        "AGGREGATE": exp.Reduce.from_arg_list,
        "BOOLEAN": build_as_cast("boolean"),
        "DATE": build_as_cast("date"),
        "DATE_TRUNC": lambda args: exp.TimestampTrunc(
            this=seq_get(args, 1), unit=exp.var(seq_get(args, 0))
        ),
        "DAYOFMONTH": lambda args: exp.DayOfMonth(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DAYOFWEEK": lambda args: exp.DayOfWeek(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DAYOFYEAR": lambda args: exp.DayOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DOUBLE": build_as_cast("double"),
        "ELEMENT_AT": lambda args: exp.Bracket(
            this=seq_get(args, 0),
            expressions=ensure_list(seq_get(args, 1)),
            offset=1,
            safe=False,
        ),
        "FLOAT": build_as_cast("float"),
        "FORMAT_STRING": exp.Format.from_arg_list,
        "FROM_UTC_TIMESTAMP": lambda args, dialect: exp.AtTimeZone(
            this=exp.cast(
                seq_get(args, 0) or exp.Var(this=""),
                exp.DType.TIMESTAMP,
                dialect=dialect,
            ),
            zone=seq_get(args, 1),
        ),
        "LTRIM": lambda args: build_trim(args, reverse_args=True),
        "INT": build_as_cast("int"),
        "MAP_FROM_ARRAYS": exp.Map.from_arg_list,
        "RLIKE": exp.RegexpLike.from_arg_list,
        "RTRIM": lambda args: build_trim(args, is_left=False, reverse_args=True),
        "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),
        "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
        "STRING": build_as_cast("string"),
        "SLICE": exp.ArraySlice.from_arg_list,
        "TIMESTAMP": build_as_cast("timestamp"),
        "TO_TIMESTAMP": lambda args: (
            build_as_cast("timestamp")(args)
            if len(args) == 1
            else build_formatted_time(exp.StrToTime, "spark")(args)
        ),
        "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
        "TO_UTC_TIMESTAMP": lambda args, dialect: exp.FromTimeZone(
            this=exp.cast(
                seq_get(args, 0) or exp.Var(this=""),
                exp.DType.TIMESTAMP,
                dialect=dialect,
            ),
            zone=seq_get(args, 1),
        ),
        "TRUNC": lambda args: exp.DateTrunc(unit=seq_get(args, 1), this=seq_get(args, 0)),
        "WEEKOFYEAR": lambda args: exp.WeekOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
    }

    FUNCTION_PARSERS = {
        **HiveParser.FUNCTION_PARSERS,
        "APPROX_PERCENTILE": lambda self: self._parse_quantile_function(exp.ApproxQuantile),
        "BROADCAST": lambda self: self._parse_join_hint("BROADCAST"),
        "BROADCASTJOIN": lambda self: self._parse_join_hint("BROADCASTJOIN"),
        "MAPJOIN": lambda self: self._parse_join_hint("MAPJOIN"),
        "MERGE": lambda self: self._parse_join_hint("MERGE"),
        "SHUFFLEMERGE": lambda self: self._parse_join_hint("SHUFFLEMERGE"),
        "MERGEJOIN": lambda self: self._parse_join_hint("MERGEJOIN"),
        "SHUFFLE_HASH": lambda self: self._parse_join_hint("SHUFFLE_HASH"),
        "SHUFFLE_REPLICATE_NL": lambda self: self._parse_join_hint("SHUFFLE_REPLICATE_NL"),
    }

    def _parse_drop_column(self) -> t.Optional[exp.Drop | exp.Command]:
        return (
            self.expression(exp.Drop(this=self._parse_schema(), kind="COLUMNS"))
            if self._match_text_seq("DROP", "COLUMNS")
            else None
        )

    def _pivot_column_names(self, aggregations: t.List[exp.Expr]) -> t.List[str]:
        if len(aggregations) == 1:
            return []
        return pivot_column_names(aggregations, dialect="spark")
