from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator

DATE_PARTS = {"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}


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
    self._annotate_args(expression)

    if expression.args.get("zone"):
        self._set_type(expression, exp.DataType.Type.TIMESTAMPTZ)
    else:
        self._set_type(expression, exp.DataType.Type.TIMESTAMP)

    return expression


def _annotate_date_or_time_add(self: TypeAnnotator, expression: exp.Expression) -> exp.Expression:
    self._annotate_args(expression)

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
    self._annotate_args(expression)

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


def _annotate_arg_max_min(
    self: TypeAnnotator, expression: exp.ArgMax | exp.ArgMin
) -> exp.ArgMax | exp.ArgMin:
    """Annotate ArgMax/ArgMin with type based on argument count.

    When count argument is provided (3 arguments), returns ARRAY of the first argument's type.
    When count is not provided (2 arguments), returns the first argument's type.
    """
    return self._annotate_by_args(expression, "this", array=bool(expression.args.get("count")))


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
            exp.RegexpExtractAll,
            exp.Split,
            exp.StringToArray,
        )
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
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
            "annotator": lambda self, e: self._annotate_with_type(
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
            exp.Chr,
            exp.Collate,
            exp.Collation,
            exp.DecompressString,
            exp.HexDecodeString,
            exp.HexEncode,
            exp.Initcap,
            exp.MD5,
            exp.Monthname,
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
    exp.ArgMax: {"annotator": _annotate_arg_max_min},
    exp.ArgMin: {"annotator": _annotate_arg_max_min},
    exp.ConcatWs: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.ConvertTimezone: {
        "annotator": lambda self, e: self._annotate_with_type(
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
    exp.LeastIgnoreNulls: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.Reverse: {"annotator": _annotate_reverse},
    exp.TimeAdd: {"annotator": _annotate_date_or_time_add},
    exp.TimestampFromParts: {"annotator": _annotate_timestamp_from_parts},
}
