"""sqlglot expressions - date, time, and timestamp functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    Expr,
    Expression,
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


class CurrentDate(Expression, Func):
    arg_types = {"this": False}


class CurrentDatetime(Expression, Func):
    arg_types = {"this": False}


class CurrentTime(Expression, Func):
    arg_types = {"this": False}


class CurrentTimestamp(Expression, Func):
    arg_types = {"this": False, "sysdate": False}


class CurrentTimestampLTZ(Expression, Func):
    arg_types = {}


class CurrentTimezone(Expression, Func):
    arg_types = {}


class Localtime(Expression, Func):
    arg_types = {"this": False}


class Localtimestamp(Expression, Func):
    arg_types = {"this": False}


class Systimestamp(Expression, Func):
    arg_types = {"this": False}


class UtcDate(Expression, Func):
    arg_types = {}


class UtcTime(Expression, Func):
    arg_types = {"this": False}


class UtcTimestamp(Expression, Func):
    arg_types = {"this": False}


# Date arithmetic


class AddMonths(Expression, Func):
    arg_types = {"this": True, "expression": True, "preserve_end_of_month": False}


class DateAdd(Expression, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateBin(Expression, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False, "zone": False, "origin": False}


class DateDiff(Expression, Func, TimeUnit):
    _sql_names = ["DATEDIFF", "DATE_DIFF"]
    arg_types = {
        "this": True,
        "expression": True,
        "unit": False,
        "zone": False,
        "big_int": False,
        "date_part_boundary": False,
    }


class DateSub(Expression, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeAdd(Expression, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Expression, Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class MonthsBetween(Expression, Func):
    arg_types = {"this": True, "expression": True, "roundoff": False}


class TimeAdd(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeDiff(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeSub(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampAdd(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampDiff(Expression, Func, TimeUnit):
    _sql_names = ["TIMESTAMPDIFF", "TIMESTAMP_DIFF"]
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampSub(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsAdd(Expression, Func, TimeUnit):
    # return_type is used to correctly cast the arguments of this expression when transpiling it
    arg_types = {"this": True, "expression": True, "unit": False, "return_type": False}

    @property
    def return_type(self) -> DataType:
        return DataType.build(self.args.get("return_type") or DType.DATE)


class TsOrDsDiff(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


# Truncation


class DatetimeTrunc(Expression, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DateTrunc(Expression, Func):
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
    def unit(self) -> Expr:
        return self.args["unit"]


class TimestampTrunc(Expression, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False, "input_type_preserved": False}


class TimeSlice(Expression, Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": True, "kind": False}


class TimeTrunc(Expression, Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


# Date/time extraction


class Day(Expression, Func):
    pass


class DayOfMonth(Expression, Func):
    _sql_names = ["DAY_OF_MONTH", "DAYOFMONTH"]


class DayOfWeek(Expression, Func):
    _sql_names = ["DAY_OF_WEEK", "DAYOFWEEK"]


class DayOfWeekIso(Expression, Func):
    _sql_names = ["DAYOFWEEK_ISO", "ISODOW"]


class DayOfYear(Expression, Func):
    _sql_names = ["DAY_OF_YEAR", "DAYOFYEAR"]


class Dayname(Expression, Func):
    arg_types = {"this": True, "abbreviated": False}


class Extract(Expression, Func):
    arg_types = {"this": True, "expression": True}


class GetExtract(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Hour(Expression, Func):
    pass


class Minute(Expression, Func):
    pass


class Month(Expression, Func):
    pass


class Monthname(Expression, Func):
    arg_types = {"this": True, "abbreviated": False}


class Quarter(Expression, Func):
    pass


class Second(Expression, Func):
    pass


class ToDays(Expression, Func):
    pass


class Week(Expression, Func):
    arg_types = {"this": True, "mode": False}


class WeekOfYear(Expression, Func):
    _sql_names = ["WEEK_OF_YEAR", "WEEKOFYEAR"]


class Year(Expression, Func):
    pass


class YearOfWeek(Expression, Func):
    _sql_names = ["YEAR_OF_WEEK", "YEAROFWEEK"]


class YearOfWeekIso(Expression, Func):
    _sql_names = ["YEAR_OF_WEEK_ISO", "YEAROFWEEKISO"]


# Date/time construction


class Date(Expression, Func):
    arg_types = {"this": False, "zone": False, "expressions": False}
    is_var_len_args = True


class DateFromParts(Expression, Func):
    _sql_names = ["DATE_FROM_PARTS", "DATEFROMPARTS"]
    arg_types = {"year": True, "month": False, "day": False, "allow_overflow": False}


class DateFromUnixDate(Expression, Func):
    pass


class Datetime(Expression, Func):
    arg_types = {"this": True, "expression": False}


class GapFill(Expression, Func):
    arg_types = {
        "this": True,
        "ts_column": True,
        "bucket_width": True,
        "partitioning_columns": False,
        "value_columns": False,
        "origin": False,
        "ignore_nulls": False,
    }


class GenerateDateArray(Expression, Func):
    arg_types = {"start": True, "end": True, "step": False}


class GenerateTimestampArray(Expression, Func):
    arg_types = {"start": True, "end": True, "step": True}


class JustifyDays(Expression, Func):
    pass


class JustifyHours(Expression, Func):
    pass


class JustifyInterval(Expression, Func):
    pass


class LastDay(Expression, Func, TimeUnit):
    _sql_names = ["LAST_DAY", "LAST_DAY_OF_MONTH"]
    arg_types = {"this": True, "unit": False}


class MakeInterval(Expression, Func):
    arg_types = {
        "year": False,
        "month": False,
        "week": False,
        "day": False,
        "hour": False,
        "minute": False,
        "second": False,
    }


class NextDay(Expression, Func):
    arg_types = {"this": True, "expression": True}


class PreviousDay(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Time(Expression, Func):
    arg_types = {"this": False, "zone": False}


class TimeFromParts(Expression, Func):
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


class Timestamp(Expression, Func):
    arg_types = {"this": False, "zone": False, "with_tz": False}


class TimestampFromParts(Expression, Func):
    _sql_names = ["TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
        "milli": False,
        "this": False,
        "expression": False,
    }


class TimestampLtzFromParts(Expression, Func):
    _sql_names = ["TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS"]
    arg_types = TIMESTAMP_PARTS.copy()


class TimestampTzFromParts(Expression, Func):
    _sql_names = ["TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
    }


# Date/time conversion


class ConvertTimezone(Expression, Func):
    arg_types = {
        "source_tz": False,
        "target_tz": True,
        "timestamp": True,
        "options": False,
    }


class DateStrToDate(Expression, Func):
    pass


class DateToDateStr(Expression, Func):
    pass


class DateToDi(Expression, Func):
    pass


class DiToDate(Expression, Func):
    pass


class FromISO8601Timestamp(Expression, Func):
    _sql_names = ["FROM_ISO8601_TIMESTAMP"]


class ParseDatetime(Expression, Func):
    arg_types = {"this": True, "format": False, "zone": False}


class ParseTime(Expression, Func):
    arg_types = {"this": True, "format": True}


class StrToDate(Expression, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class StrToTime(Expression, Func):
    arg_types = {"this": True, "format": True, "zone": False, "safe": False, "target_type": False}


class StrToUnix(Expression, Func):
    arg_types = {"this": False, "format": False}


class TimeStrToDate(Expression, Func):
    pass


class TimeStrToTime(Expression, Func):
    arg_types = {"this": True, "zone": False}


class TimeStrToUnix(Expression, Func):
    pass


class TimeToStr(Expression, Func):
    arg_types = {"this": True, "format": True, "culture": False, "zone": False}


class TimeToTimeStr(Expression, Func):
    pass


class TimeToUnix(Expression, Func):
    pass


class TsOrDiToDi(Expression, Func):
    pass


class TsOrDsToDate(Expression, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToDateStr(Expression, Func):
    pass


class TsOrDsToDatetime(Expression, Func):
    pass


class TsOrDsToTime(Expression, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToTimestamp(Expression, Func):
    pass


class UnixDate(Expression, Func):
    pass


class UnixMicros(Expression, Func):
    pass


class UnixMillis(Expression, Func):
    pass


class UnixSeconds(Expression, Func):
    pass


class UnixToStr(Expression, Func):
    arg_types = {"this": True, "format": False}


class UnixToTime(Expression, Func):
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


class UnixToTimeStr(Expression, Func):
    pass
