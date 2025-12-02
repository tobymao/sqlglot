from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator

DATE_PARTS = {"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}

MAX_PRECISION = 38

MAX_SCALE = 37


def _annotate_reverse(self: TypeAnnotator, expression: exp.Reverse) -> exp.Reverse:
    expression = self._annotate_by_args(expression, "this")
    if expression.is_type(exp.DataType.Type.NULL):
        # Snowflake treats REVERSE(NULL) as a VARCHAR
        self._set_type(expression, exp.DataType.Type.VARCHAR)

    return expression


def _annotate_timestamp_from_parts(
    self: TypeAnnotator, expression: exp.TimestampFromParts
) -> exp.TimestampFromParts:
    """Annotate TimestampFromParts with correct type based on arguments.
    TIMESTAMP_FROM_PARTS with time_zone -> TIMESTAMPTZ
    TIMESTAMP_FROM_PARTS without time_zone -> TIMESTAMP (defaults to TIMESTAMP_NTZ)
    """
    if expression.args.get("zone"):
        self._set_type(expression, exp.DataType.Type.TIMESTAMPTZ)
    else:
        self._set_type(expression, exp.DataType.Type.TIMESTAMP)

    return expression


def _annotate_date_or_time_add(self: TypeAnnotator, expression: exp.Expression) -> exp.Expression:
    if (
        expression.this.is_type(exp.DataType.Type.DATE)
        and expression.text("unit").upper() not in DATE_PARTS
    ):
        self._set_type(expression, exp.DataType.Type.TIMESTAMPNTZ)
    else:
        self._annotate_by_args(expression, "this")
    return expression


def _annotate_decode_case(self: TypeAnnotator, expression: exp.DecodeCase) -> exp.DecodeCase:
    """Annotate DecodeCase with the type inferred from return values only.

    DECODE uses the format: DECODE(expr, val1, ret1, val2, ret2, ..., default)
    We only look at the return values (ret1, ret2, ..., default) to determine the type,
    not the comparison values (val1, val2, ...) or the expression being compared.
    """
    expressions = expression.expressions

    # Return values are at indices 2, 4, 6, ... and the last element (if even length)
    # DECODE(expr, val1, ret1, val2, ret2, ..., default)
    return_types = [expressions[i].type for i in range(2, len(expressions), 2)]

    # If the total number of expressions is even, the last one is the default
    # Example:
    #   DECODE(x, 1, 'a', 2, 'b')             -> len=5 (odd), no default
    #   DECODE(x, 1, 'a', 2, 'b', 'default')  -> len=6 (even), has default
    if len(expressions) % 2 == 0:
        return_types.append(expressions[-1].type)

    # Determine the common type from all return values
    last_type = None
    for ret_type in return_types:
        last_type = self._maybe_coerce(last_type or ret_type, ret_type)

    self._set_type(expression, last_type)
    return expression


def _annotate_arg_max_min(self, expression):
    self._set_type(
        expression,
        exp.DataType.Type.ARRAY if expression.args.get("count") else expression.this.type,
    )
    return expression


def _annotate_within_group(self: TypeAnnotator, expression: exp.WithinGroup) -> exp.WithinGroup:
    """Annotate WithinGroup with correct type based on the inner function.

    1) Annotate args first
    2) Check if this is PercentileDisc and if so, re-annotate its type to match the ordered expression's type
    """
    if (
        isinstance(expression.this, exp.PercentileDisc)
        and isinstance(order_expr := expression.expression, exp.Order)
        and len(order_expr.expressions) == 1
        and isinstance(ordered_expr := order_expr.expressions[0], exp.Ordered)
    ):
        self._set_type(expression, ordered_expr.this.type)

    return expression


def _annotate_median(self: TypeAnnotator, expression: exp.Median) -> exp.Median:
    """Annotate MEDIAN function with correct return type.

    Based on Snowflake documentation:
    - If the expr is FLOAT -> annotate as FLOAT
    - If the expr is NUMBER(p, s) -> annotate as NUMBER(min(p+3, 38), min(s+3, 37))
    """
    # First annotate the argument to get its type
    expression = self._annotate_by_args(expression, "this")

    # Get the input type
    input_type = expression.this.type

    if input_type.is_type(exp.DataType.Type.FLOAT):
        # If input is FLOAT, return FLOAT
        self._set_type(expression, exp.DataType.Type.FLOAT)
    else:
        # If input is NUMBER(p, s), return NUMBER(min(p+3, 38), min(s+3, 37))
        exprs = input_type.expressions

        precision_expr = seq_get(exprs, 0)
        precision = precision_expr.this.to_py() if precision_expr else MAX_PRECISION

        scale_expr = seq_get(exprs, 1)
        scale = scale_expr.this.to_py() if scale_expr else 0

        new_precision = min(precision + 3, MAX_PRECISION)
        new_scale = min(scale + 3, MAX_SCALE)

        # Build the new NUMBER type
        new_type = exp.DataType.build(f"NUMBER({new_precision}, {new_scale})", dialect="snowflake")
        self._set_type(expression, new_type)

    return expression


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.AddMonths,
            exp.Ceil,
            exp.DateTrunc,
            exp.Floor,
            exp.Left,
            exp.Pad,
            exp.Right,
            exp.Round,
            exp.Stuff,
            exp.Substring,
            exp.TimeSlice,
            exp.TimestampTrunc,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.ARRAY}
        for expr_type in (
            exp.ApproxTopK,
            exp.ApproxTopKEstimate,
            exp.ArrayAgg,
            exp.ArrayUnionAgg,
            exp.RegexpExtractAll,
            exp.Split,
            exp.StringToArray,
        )
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.BitmapBitPosition,
            exp.BitmapBucketNumber,
            exp.BitmapCount,
            exp.Factorial,
            exp.GroupingId,
            exp.MD5NumberLower64,
            exp.MD5NumberUpper64,
            exp.Rand,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BINARY}
        for expr_type in {
            exp.Base64DecodeBinary,
            exp.BitmapConstructAgg,
            exp.BitmapOrAgg,
            exp.Compress,
            exp.DecompressBinary,
            exp.MD5Digest,
            exp.SHA1Digest,
            exp.SHA2Digest,
            exp.TryBase64DecodeBinary,
            exp.TryHexDecodeBinary,
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BOOLEAN}
        for expr_type in {
            exp.Booland,
            exp.Boolnot,
            exp.Boolor,
            exp.BoolxorAgg,
            exp.EqualNull,
            exp.IsNullValue,
            exp.Search,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DATE}
        for expr_type in {
            exp.NextDay,
            exp.PreviousDay,
        }
    },
    **{
        expr_type: {
            "annotator": lambda self, e: self._set_type(
                e, exp.DataType.build("NUMBER", dialect="snowflake")
            )
        }
        for expr_type in (
            exp.BitwiseAndAgg,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
            exp.RegexpCount,
            exp.RegexpInstr,
        )
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.ApproximateSimilarity,
            exp.Asin,
            exp.Asinh,
            exp.Atan,
            exp.Atan2,
            exp.Atanh,
            exp.Cbrt,
            exp.Cos,
            exp.Cosh,
            exp.Cot,
            exp.Degrees,
            exp.Exp,
            exp.MonthsBetween,
            exp.Normal,
            exp.RegrAvgx,
            exp.RegrAvgy,
            exp.RegrSlope,
            exp.RegrValx,
            exp.RegrValy,
            exp.Sin,
            exp.Sinh,
            exp.Tan,
            exp.Tanh,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.Ascii,
            exp.BitLength,
            exp.ByteLength,
            exp.Getbit,
            exp.Grouping,
            exp.Hour,
            exp.JarowinklerSimilarity,
            exp.Length,
            exp.Levenshtein,
            exp.Minute,
            exp.RtrimmedLength,
            exp.Second,
            exp.StrPosition,
            exp.Unicode,
            exp.WidthBucket,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.OBJECT}
        for expr_type in {
            exp.ObjectAgg,
            exp.ParseIp,
            exp.ParseUrl,
            exp.ApproxTopKAccumulate,
            exp.ApproxTopKCombine,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIME}
        for expr_type in {
            exp.TimeFromParts,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.AIAgg,
            exp.AIClassify,
            exp.AISummarizeAgg,
            exp.Base64DecodeString,
            exp.Base64Encode,
            exp.CheckJson,
            exp.CheckXml,
            exp.Chr,
            exp.Collate,
            exp.Collation,
            exp.DecompressString,
            exp.HexDecodeString,
            exp.HexEncode,
            exp.Initcap,
            exp.MD5,
            exp.Monthname,
            exp.Randstr,
            exp.RegexpExtract,
            exp.RegexpReplace,
            exp.Repeat,
            exp.Replace,
            exp.SHA,
            exp.SHA2,
            exp.Soundex,
            exp.SoundexP123,
            exp.Space,
            exp.SplitPart,
            exp.Translate,
            exp.TryBase64DecodeString,
            exp.TryHexDecodeString,
            exp.Uuid,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARIANT}
        for expr_type in {
            exp.Minhash,
            exp.MinhashCombine,
        }
    },
    exp.ArgMax: {"annotator": _annotate_arg_max_min},
    exp.ArgMin: {"annotator": _annotate_arg_max_min},
    exp.ConcatWs: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.ConvertTimezone: {
        "annotator": lambda self, e: self._set_type(
            e,
            exp.DataType.Type.TIMESTAMPNTZ
            if e.args.get("source_tz")
            else exp.DataType.Type.TIMESTAMPTZ,
        )
    },
    exp.DateAdd: {"annotator": _annotate_date_or_time_add},
    exp.DecodeCase: {"annotator": _annotate_decode_case},
    exp.GreatestIgnoreNulls: {
        "annotator": lambda self, e: self._annotate_by_args(e, "expressions")
    },
    exp.HashAgg: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("NUMBER(19, 0)", dialect="snowflake")
        )
    },
    exp.LeastIgnoreNulls: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.Median: {"annotator": _annotate_median},
    exp.Reverse: {"annotator": _annotate_reverse},
    exp.TimeAdd: {"annotator": _annotate_date_or_time_add},
    exp.TimestampFromParts: {"annotator": _annotate_timestamp_from_parts},
    exp.WithinGroup: {"annotator": _annotate_within_group},
}
