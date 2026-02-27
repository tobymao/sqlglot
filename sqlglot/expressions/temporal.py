"""sqlglot expressions - date, time, and timestamp functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
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


class CurrentDate(Func):
    arg_types = {"this": False}


class CurrentDatetime(Func):
    arg_types = {"this": False}


class CurrentTime(Func):
    arg_types = {"this": False}


class CurrentTimestamp(Func):
    arg_types = {"this": False, "sysdate": False}


class CurrentTimestampLTZ(Func):
    arg_types = {}


class CurrentTimezone(Func):
    arg_types = {}


class Localtime(Func):
    arg_types = {"this": False}


class Localtimestamp(Func):
    arg_types = {"this": False}


class Systimestamp(Func):
    arg_types = {"this": False}


class UtcDate(Func):
    arg_types = {}


class UtcTime(Func):
    arg_types = {"this": False}


class UtcTimestamp(Func):
    arg_types = {"this": False}


# Date arithmetic


class AddMonths(Func):
    arg_types = {"this": True, "expression": True, "preserve_end_of_month": False}


class DateAdd(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateBin(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False, "zone": False, "origin": False}


class DateDiff(Func, TimeUnit):
    _sql_names = ["DATEDIFF", "DATE_DIFF"]
    arg_types = {
        "this": True,
        "expression": True,
        "unit": False,
        "zone": False,
        "big_int": False,
        "date_part_boundary": False,
    }


class DateSub(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeAdd(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class MonthsBetween(Func):
    arg_types = {"this": True, "expression": True, "roundoff": False}


class TimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampDiff(Func, TimeUnit):
    _sql_names = ["TIMESTAMPDIFF", "TIMESTAMP_DIFF"]
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsAdd(Func, TimeUnit):
    # return_type is used to correctly cast the arguments of this expression when transpiling it
    arg_types = {"this": True, "expression": True, "unit": False, "return_type": False}

    @property
    def return_type(self) -> DataType:
        return DataType.build(self.args.get("return_type") or DType.DATE)


class TsOrDsDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


# Truncation


class DatetimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DateTrunc(Func):
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


class TimestampTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False, "input_type_preserved": False}


class TimeSlice(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": True, "kind": False}


class TimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


# Date/time extraction


class Day(Func):
    pass


class DayOfMonth(Func):
    _sql_names = ["DAY_OF_MONTH", "DAYOFMONTH"]


class DayOfWeek(Func):
    _sql_names = ["DAY_OF_WEEK", "DAYOFWEEK"]


class DayOfWeekIso(Func):
    _sql_names = ["DAYOFWEEK_ISO", "ISODOW"]


class DayOfYear(Func):
    _sql_names = ["DAY_OF_YEAR", "DAYOFYEAR"]


class Dayname(Func):
    arg_types = {"this": True, "abbreviated": False}


class Extract(Func):
    arg_types = {"this": True, "expression": True}


class GetExtract(Func):
    arg_types = {"this": True, "expression": True}


class Hour(Func):
    pass


class Minute(Func):
    pass


class Month(Func):
    pass


class Monthname(Func):
    arg_types = {"this": True, "abbreviated": False}


class Quarter(Func):
    pass


class Second(Func):
    pass


class ToDays(Func):
    pass


class Week(Func):
    arg_types = {"this": True, "mode": False}


class WeekOfYear(Func):
    _sql_names = ["WEEK_OF_YEAR", "WEEKOFYEAR"]


class Year(Func):
    pass


class YearOfWeek(Func):
    _sql_names = ["YEAR_OF_WEEK", "YEAROFWEEK"]


class YearOfWeekIso(Func):
    _sql_names = ["YEAR_OF_WEEK_ISO", "YEAROFWEEKISO"]


# Date/time construction


class Date(Func):
    arg_types = {"this": False, "zone": False, "expressions": False}
    is_var_len_args = True


class DateFromParts(Func):
    _sql_names = ["DATE_FROM_PARTS", "DATEFROMPARTS"]
    arg_types = {"year": True, "month": False, "day": False, "allow_overflow": False}


class DateFromUnixDate(Func):
    pass


class Datetime(Func):
    arg_types = {"this": True, "expression": False}


class GapFill(Func):
    arg_types = {
        "this": True,
        "ts_column": True,
        "bucket_width": True,
        "partitioning_columns": False,
        "value_columns": False,
        "origin": False,
        "ignore_nulls": False,
    }


class GenerateDateArray(Func):
    arg_types = {"start": True, "end": True, "step": False}


class GenerateTimestampArray(Func):
    arg_types = {"start": True, "end": True, "step": True}


class JustifyDays(Func):
    pass


class JustifyHours(Func):
    pass


class JustifyInterval(Func):
    pass


class LastDay(Func, TimeUnit):
    _sql_names = ["LAST_DAY", "LAST_DAY_OF_MONTH"]
    arg_types = {"this": True, "unit": False}


class MakeInterval(Func):
    arg_types = {
        "year": False,
        "month": False,
        "week": False,
        "day": False,
        "hour": False,
        "minute": False,
        "second": False,
    }


class NextDay(Func):
    arg_types = {"this": True, "expression": True}


class PreviousDay(Func):
    arg_types = {"this": True, "expression": True}


class Time(Func):
    arg_types = {"this": False, "zone": False}


class TimeFromParts(Func):
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


class Timestamp(Func):
    arg_types = {"this": False, "zone": False, "with_tz": False}


class TimestampFromParts(Func):
    _sql_names = ["TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
        "milli": False,
        "this": False,
        "expression": False,
    }


class TimestampLtzFromParts(Func):
    _sql_names = ["TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS"]
    arg_types = TIMESTAMP_PARTS.copy()


class TimestampTzFromParts(Func):
    _sql_names = ["TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
    }


# Date/time conversion


class ConvertTimezone(Func):
    arg_types = {
        "source_tz": False,
        "target_tz": True,
        "timestamp": True,
        "options": False,
    }


class DateStrToDate(Func):
    pass


class DateToDateStr(Func):
    pass


class DateToDi(Func):
    pass


class DiToDate(Func):
    pass


class FromISO8601Timestamp(Func):
    _sql_names = ["FROM_ISO8601_TIMESTAMP"]


class ParseDatetime(Func):
    arg_types = {"this": True, "format": False, "zone": False}


class ParseTime(Func):
    arg_types = {"this": True, "format": True}


class StrToDate(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class StrToTime(Func):
    arg_types = {"this": True, "format": True, "zone": False, "safe": False, "target_type": False}


class StrToUnix(Func):
    arg_types = {"this": False, "format": False}


class TimeStrToDate(Func):
    pass


class TimeStrToTime(Func):
    arg_types = {"this": True, "zone": False}


class TimeStrToUnix(Func):
    pass


class TimeToStr(Func):
    arg_types = {"this": True, "format": True, "culture": False, "zone": False}


class TimeToTimeStr(Func):
    pass


class TimeToUnix(Func):
    pass


class TsOrDiToDi(Func):
    pass


class TsOrDsToDate(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDatetime(Func):
    pass


class TsOrDsToTime(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToTimestamp(Func):
    pass


class UnixDate(Func):
    pass


class UnixMicros(Func):
    pass


class UnixMillis(Func):
    pass


class UnixSeconds(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": False}


class UnixToTime(Func):
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


class UnixToTimeStr(Func):
    pass
