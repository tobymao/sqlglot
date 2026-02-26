"""sqlglot expressions - math, trigonometry, and bitwise functions."""

from __future__ import annotations

from sqlglot.expressions.core import Func, AggFunc


# Trigonometric


class Acos(Func):
    pass


class Acosh(Func):
    pass


class Asin(Func):
    pass


class Asinh(Func):
    pass


class Atan(Func):
    arg_types = {"this": True, "expression": False}


class Atanh(Func):
    pass


class Atan2(Func):
    arg_types = {"this": True, "expression": True}


class Cos(Func):
    pass


class Cosh(Func):
    pass


class Cot(Func):
    pass


class Coth(Func):
    pass


class Csc(Func):
    pass


class Csch(Func):
    pass


class Degrees(Func):
    pass


class Radians(Func):
    pass


class Sec(Func):
    pass


class Sech(Func):
    pass


class Sin(Func):
    pass


class Sinh(Func):
    pass


class Tan(Func):
    pass


class Tanh(Func):
    pass


# Geometric distance / similarity


class CosineDistance(Func):
    arg_types = {"this": True, "expression": True}


class DotProduct(Func):
    arg_types = {"this": True, "expression": True}


class EuclideanDistance(Func):
    arg_types = {"this": True, "expression": True}


class JarowinklerSimilarity(Func):
    arg_types = {"this": True, "expression": True, "case_insensitive": False}


class ManhattanDistance(Func):
    arg_types = {"this": True, "expression": True}


# Basic arithmetic / math


class Abs(Func):
    pass


class Cbrt(Func):
    pass


class Ceil(Func):
    arg_types = {"this": True, "decimals": False, "to": False}
    _sql_names = ["CEIL", "CEILING"]


class Exp(Func):
    pass


class Factorial(Func):
    pass


class Floor(Func):
    arg_types = {"this": True, "decimals": False, "to": False}


class IsInf(Func):
    _sql_names = ["IS_INF", "ISINF"]


class IsNan(Func):
    _sql_names = ["IS_NAN", "ISNAN"]


class Ln(Func):
    pass


class Log(Func):
    arg_types = {"this": True, "expression": False}


class Pi(Func):
    arg_types = {}


class Round(Func):
    arg_types = {
        "this": True,
        "decimals": False,
        "truncate": False,
        "casts_non_integer_decimals": False,
    }


class Sign(Func):
    _sql_names = ["SIGN", "SIGNUM"]


class Sqrt(Func):
    pass


class Trunc(Func):
    arg_types = {"this": True, "decimals": False}
    _sql_names = ["TRUNC", "TRUNCATE"]


# Safe arithmetic


class SafeAdd(Func):
    arg_types = {"this": True, "expression": True}


class SafeDivide(Func):
    arg_types = {"this": True, "expression": True}


class SafeMultiply(Func):
    arg_types = {"this": True, "expression": True}


class SafeNegate(Func):
    pass


class SafeSubtract(Func):
    arg_types = {"this": True, "expression": True}


# Bitwise


class BitwiseAndAgg(AggFunc):
    pass


class BitwiseCount(Func):
    pass


class BitwiseOrAgg(AggFunc):
    pass


class BitwiseXorAgg(AggFunc):
    pass


class BitmapBitPosition(Func):
    pass


class BitmapBucketNumber(Func):
    pass


class BitmapConstructAgg(AggFunc):
    pass


class BitmapCount(Func):
    pass


class BitmapOrAgg(AggFunc):
    pass


class Booland(Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


class Boolnot(Func):
    arg_types = {"this": True, "round_input": False}


class Boolor(Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


class BoolxorAgg(AggFunc):
    pass


class Getbit(Func):
    _sql_names = ["GETBIT", "GET_BIT"]
    # zero_is_msb means the most significant bit is indexed 0
    arg_types = {"this": True, "expression": True, "zero_is_msb": False}
