"""sqlglot expressions - date, time, and timestamp functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    Expression,
    ExpressionBase,
    Func,
    TimeUnit,
    IntervalOp,
    Literal,
    Neg,
    Column,
    TIMESTAMP_PARTS,
)
from sqlglot.expressions.datatypes import DataType, DType


# Current date/time


class CurrentDate(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentDatetime(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentTime(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentTimestamp(ExpressionBase, Func):
    arg_types = {"this": False, "sysdate": False}


class CurrentTimestampLTZ(ExpressionBase, Func):
    arg_types = {}


class CurrentTimezone(ExpressionBase, Func):
    arg_types = {}


class Localtime(ExpressionBase, Func):
    arg_types = {"this": False}


class Localtimestamp(ExpressionBase, Func):
    arg_types = {"this": False}


class Systimestamp(ExpressionBase, Func):
    arg_types = {"this": False}


class UtcDate(ExpressionBase, Func):
    arg_types = {}


class UtcTime(ExpressionBase, Func):
    arg_types = {"this": False}


class UtcTimestamp(ExpressionBase, Func):
    arg_types = {"this": False}


# Date arithmetic


class AddMonths(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "preserve_end_of_month": False}


class DateAdd(ExpressionBase, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateBin(ExpressionBase, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False, "zone": False, "origin": False}


class DateDiff(ExpressionBase, Func, TimeUnit):
    _sql_names = ["DATEDIFF", "DATE_DIFF"]
    arg_types = {
        "this": True,
        "expression": True,
        "unit": False,
        "zone": False,
        "big_int": False,
        "date_part_boundary": False,
    }


class DateSub(ExpressionBase, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeAdd(ExpressionBase, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(ExpressionBase, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class MonthsBetween(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "roundoff": False}


class TimeAdd(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeDiff(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeSub(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampAdd(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampDiff(ExpressionBase, Func, TimeUnit):
    _sql_names = ["TIMESTAMPDIFF", "TIMESTAMP_DIFF"]
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampSub(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsAdd(ExpressionBase, Func, TimeUnit):
    # return_type is used to correctly cast the arguments of this expression when transpiling it
    arg_types = {"this": True, "expression": True, "unit": False, "return_type": False}

    @property
    def return_type(self) -> DataType:
        return DataType.build(self.args.get("return_type") or DType.DATE)


class TsOrDsDiff(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


# Truncation


class DatetimeTrunc(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DateTrunc(ExpressionBase, Func):
    arg_types = {"unit": True, "this": True, "zone": False, "input_type_preserved": False}

    def __init__(self, **args):
        # Across most dialects it's safe to unabbreviate the unit (e.g. 'Q' -> 'QUARTER') except Oracle
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROUND-and-TRUNC-Date-Functions.html
        unabbreviate = args.pop("unabbreviate", True)

        unit = args.get("unit")
        if isinstance(unit, TimeUnit.VAR_LIKE) and not (
            isinstance(unit, Column) and len(unit.parts) != 1
        ):
            unit_name = unit.name.upper()
            if unabbreviate and unit_name in TimeUnit.UNABBREVIATED_UNIT_NAME:
                unit_name = TimeUnit.UNABBREVIATED_UNIT_NAME[unit_name]

            args["unit"] = Literal.string(unit_name)

        super().__init__(**args)

    @property
    def unit(self) -> Expression:
        return self.args["unit"]


class TimestampTrunc(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False, "input_type_preserved": False}


class TimeSlice(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": True, "kind": False}


class TimeTrunc(ExpressionBase, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


# Date/time extraction


class Day(ExpressionBase, Func):
    pass


class DayOfMonth(ExpressionBase, Func):
    _sql_names = ["DAY_OF_MONTH", "DAYOFMONTH"]


class DayOfWeek(ExpressionBase, Func):
    _sql_names = ["DAY_OF_WEEK", "DAYOFWEEK"]


class DayOfWeekIso(ExpressionBase, Func):
    _sql_names = ["DAYOFWEEK_ISO", "ISODOW"]


class DayOfYear(ExpressionBase, Func):
    _sql_names = ["DAY_OF_YEAR", "DAYOFYEAR"]


class Dayname(ExpressionBase, Func):
    arg_types = {"this": True, "abbreviated": False}


class Extract(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class GetExtract(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Hour(ExpressionBase, Func):
    pass


class Minute(ExpressionBase, Func):
    pass


class Month(ExpressionBase, Func):
    pass


class Monthname(ExpressionBase, Func):
    arg_types = {"this": True, "abbreviated": False}


class Quarter(ExpressionBase, Func):
    pass


class Second(ExpressionBase, Func):
    pass


class ToDays(ExpressionBase, Func):
    pass


class Week(ExpressionBase, Func):
    arg_types = {"this": True, "mode": False}


class WeekOfYear(ExpressionBase, Func):
    _sql_names = ["WEEK_OF_YEAR", "WEEKOFYEAR"]


class Year(ExpressionBase, Func):
    pass


class YearOfWeek(ExpressionBase, Func):
    _sql_names = ["YEAR_OF_WEEK", "YEAROFWEEK"]


class YearOfWeekIso(ExpressionBase, Func):
    _sql_names = ["YEAR_OF_WEEK_ISO", "YEAROFWEEKISO"]


# Date/time construction


class Date(ExpressionBase, Func):
    arg_types = {"this": False, "zone": False, "expressions": False}
    is_var_len_args = True


class DateFromParts(ExpressionBase, Func):
    _sql_names = ["DATE_FROM_PARTS", "DATEFROMPARTS"]
    arg_types = {"year": True, "month": False, "day": False, "allow_overflow": False}


class DateFromUnixDate(ExpressionBase, Func):
    pass


class Datetime(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class GapFill(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "ts_column": True,
        "bucket_width": True,
        "partitioning_columns": False,
        "value_columns": False,
        "origin": False,
        "ignore_nulls": False,
    }


class GenerateDateArray(ExpressionBase, Func):
    arg_types = {"start": True, "end": True, "step": False}


class GenerateTimestampArray(ExpressionBase, Func):
    arg_types = {"start": True, "end": True, "step": True}


class JustifyDays(ExpressionBase, Func):
    pass


class JustifyHours(ExpressionBase, Func):
    pass


class JustifyInterval(ExpressionBase, Func):
    pass


class LastDay(ExpressionBase, Func, TimeUnit):
    _sql_names = ["LAST_DAY", "LAST_DAY_OF_MONTH"]
    arg_types = {"this": True, "unit": False}


class MakeInterval(ExpressionBase, Func):
    arg_types = {
        "year": False,
        "month": False,
        "week": False,
        "day": False,
        "hour": False,
        "minute": False,
        "second": False,
    }


class NextDay(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class PreviousDay(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Time(ExpressionBase, Func):
    arg_types = {"this": False, "zone": False}


class TimeFromParts(ExpressionBase, Func):
    _sql_names = ["TIME_FROM_PARTS", "TIMEFROMPARTS"]
    arg_types = {
        "hour": True,
        "min": True,
        "sec": True,
        "nano": False,
        "fractions": False,
        "precision": False,
        "overflow": False,
    }


class Timestamp(ExpressionBase, Func):
    arg_types = {"this": False, "zone": False, "with_tz": False}


class TimestampFromParts(ExpressionBase, Func):
    _sql_names = ["TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
        "milli": False,
        "this": False,
        "expression": False,
    }


class TimestampLtzFromParts(ExpressionBase, Func):
    _sql_names = ["TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS"]
    arg_types = TIMESTAMP_PARTS.copy()


class TimestampTzFromParts(ExpressionBase, Func):
    _sql_names = ["TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
    }


# Date/time conversion


class ConvertTimezone(ExpressionBase, Func):
    arg_types = {
        "source_tz": False,
        "target_tz": True,
        "timestamp": True,
        "options": False,
    }


class DateStrToDate(ExpressionBase, Func):
    pass


class DateToDateStr(ExpressionBase, Func):
    pass


class DateToDi(ExpressionBase, Func):
    pass


class DiToDate(ExpressionBase, Func):
    pass


class FromISO8601Timestamp(ExpressionBase, Func):
    _sql_names = ["FROM_ISO8601_TIMESTAMP"]


class ParseDatetime(ExpressionBase, Func):
    arg_types = {"this": True, "format": False, "zone": False}


class ParseTime(ExpressionBase, Func):
    arg_types = {"this": True, "format": True}


class StrToDate(ExpressionBase, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class StrToTime(ExpressionBase, Func):
    arg_types = {"this": True, "format": True, "zone": False, "safe": False, "target_type": False}


class StrToUnix(ExpressionBase, Func):
    arg_types = {"this": False, "format": False}


class TimeStrToDate(ExpressionBase, Func):
    pass


class TimeStrToTime(ExpressionBase, Func):
    arg_types = {"this": True, "zone": False}


class TimeStrToUnix(ExpressionBase, Func):
    pass


class TimeToStr(ExpressionBase, Func):
    arg_types = {"this": True, "format": True, "culture": False, "zone": False}


class TimeToTimeStr(ExpressionBase, Func):
    pass


class TimeToUnix(ExpressionBase, Func):
    pass


class TsOrDiToDi(ExpressionBase, Func):
    pass


class TsOrDsToDate(ExpressionBase, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToDateStr(ExpressionBase, Func):
    pass


class TsOrDsToDatetime(ExpressionBase, Func):
    pass


class TsOrDsToTime(ExpressionBase, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToTimestamp(ExpressionBase, Func):
    pass


class UnixDate(ExpressionBase, Func):
    pass


class UnixMicros(ExpressionBase, Func):
    pass


class UnixMillis(ExpressionBase, Func):
    pass


class UnixSeconds(ExpressionBase, Func):
    pass


class UnixToStr(ExpressionBase, Func):
    arg_types = {"this": True, "format": False}


class UnixToTime(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "scale": False,
        "zone": False,
        "hours": False,
        "minutes": False,
        "format": False,
        "target_type": False,
    }

    SECONDS: t.ClassVar[Literal | Neg] = Literal.number(0)
    DECIS: t.ClassVar[Literal | Neg] = Literal.number(1)
    CENTIS: t.ClassVar[Literal | Neg] = Literal.number(2)
    MILLIS: t.ClassVar[Literal | Neg] = Literal.number(3)
    DECIMILLIS: t.ClassVar[Literal | Neg] = Literal.number(4)
    CENTIMILLIS: t.ClassVar[Literal | Neg] = Literal.number(5)
    MICROS: t.ClassVar[Literal | Neg] = Literal.number(6)
    DECIMICROS: t.ClassVar[Literal | Neg] = Literal.number(7)
    CENTIMICROS: t.ClassVar[Literal | Neg] = Literal.number(8)
    NANOS: t.ClassVar[Literal | Neg] = Literal.number(9)


class UnixToTimeStr(ExpressionBase, Func):
    pass
