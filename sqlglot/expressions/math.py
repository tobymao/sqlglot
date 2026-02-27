"""sqlglot expressions - math, trigonometry, and bitwise functions."""

from __future__ import annotations

from sqlglot.expressions.core import ExpressionBase, Func, AggFunc


# Trigonometric


class Acos(ExpressionBase, Func):
    pass


class Acosh(ExpressionBase, Func):
    pass


class Asin(ExpressionBase, Func):
    pass


class Asinh(ExpressionBase, Func):
    pass


class Atan(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class Atanh(ExpressionBase, Func):
    pass


class Atan2(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Cos(ExpressionBase, Func):
    pass


class Cosh(ExpressionBase, Func):
    pass


class Cot(ExpressionBase, Func):
    pass


class Coth(ExpressionBase, Func):
    pass


class Csc(ExpressionBase, Func):
    pass


class Csch(ExpressionBase, Func):
    pass


class Degrees(ExpressionBase, Func):
    pass


class Radians(ExpressionBase, Func):
    pass


class Sec(ExpressionBase, Func):
    pass


class Sech(ExpressionBase, Func):
    pass


class Sin(ExpressionBase, Func):
    pass


class Sinh(ExpressionBase, Func):
    pass


class Tan(ExpressionBase, Func):
    pass


class Tanh(ExpressionBase, Func):
    pass


# Geometric distance / similarity


class CosineDistance(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class DotProduct(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class EuclideanDistance(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class JarowinklerSimilarity(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "case_insensitive": False}


class ManhattanDistance(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


# Basic arithmetic / math


class Abs(ExpressionBase, Func):
    pass


class Cbrt(ExpressionBase, Func):
    pass


class Ceil(ExpressionBase, Func):
    arg_types = {"this": True, "decimals": False, "to": False}
    _sql_names = ["CEIL", "CEILING"]


class Exp(ExpressionBase, Func):
    pass


class Factorial(ExpressionBase, Func):
    pass


class Floor(ExpressionBase, Func):
    arg_types = {"this": True, "decimals": False, "to": False}


class IsInf(ExpressionBase, Func):
    _sql_names = ["IS_INF", "ISINF"]


class IsNan(ExpressionBase, Func):
    _sql_names = ["IS_NAN", "ISNAN"]


class Ln(ExpressionBase, Func):
    pass


class Log(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class Pi(ExpressionBase, Func):
    arg_types = {}


class Round(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "decimals": False,
        "truncate": False,
        "casts_non_integer_decimals": False,
    }


class Sign(ExpressionBase, Func):
    _sql_names = ["SIGN", "SIGNUM"]


class Sqrt(ExpressionBase, Func):
    pass


class Trunc(ExpressionBase, Func):
    arg_types = {"this": True, "decimals": False}
    _sql_names = ["TRUNC", "TRUNCATE"]


# Safe arithmetic


class SafeAdd(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class SafeDivide(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class SafeMultiply(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class SafeNegate(ExpressionBase, Func):
    pass


class SafeSubtract(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


# Bitwise


class BitwiseAndAgg(ExpressionBase, AggFunc):
    pass


class BitwiseCount(ExpressionBase, Func):
    pass


class BitwiseOrAgg(ExpressionBase, AggFunc):
    pass


class BitwiseXorAgg(ExpressionBase, AggFunc):
    pass


class BitmapBitPosition(ExpressionBase, Func):
    pass


class BitmapBucketNumber(ExpressionBase, Func):
    pass


class BitmapConstructAgg(ExpressionBase, AggFunc):
    pass


class BitmapCount(ExpressionBase, Func):
    pass


class BitmapOrAgg(ExpressionBase, AggFunc):
    pass


class Booland(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


class Boolnot(ExpressionBase, Func):
    arg_types = {"this": True, "round_input": False}


class Boolor(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


class BoolxorAgg(ExpressionBase, AggFunc):
    pass


class Getbit(ExpressionBase, Func):
    _sql_names = ["GETBIT", "GET_BIT"]
    # zero_is_msb means the most significant bit is indexed 0
    arg_types = {"this": True, "expression": True, "zero_is_msb": False}
