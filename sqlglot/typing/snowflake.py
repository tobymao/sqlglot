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


def _extract_precision_scale(data_type: exp.DataType) -> t.Tuple[t.Optional[int], t.Optional[int]]:
    """Extract precision and scale from a parameterized numeric type."""
    expressions = data_type.expressions
    if not expressions:
        return None, None

    precision = None
    scale = None

    if len(expressions) >= 1:
        p_expr = expressions[0]
        if isinstance(p_expr, exp.DataTypeParam) and isinstance(p_expr.this, exp.Literal):
            precision = int(p_expr.this.this) if p_expr.this.is_int else None

    if len(expressions) >= 2:
        s_expr = expressions[1]
        if isinstance(s_expr, exp.DataTypeParam) and isinstance(s_expr.this, exp.Literal):
            scale = int(s_expr.this.this) if s_expr.this.is_int else None

    return precision, scale


def _compute_nullif_result_type(
    base_type: exp.DataType, p1: int, s1: int, p2: int, s2: int
) -> t.Optional[exp.DataType]:
    """
    Compute result type for NULLIF with two parameterized numeric types.

    Rules:
    - If p1 >= p2 AND s1 >= s2: return type1
    - If p2 >= p1 AND s2 >= s1: return type2
    - Otherwise: return DECIMAL(max(p1, p2) + |s2 - s1|, max(s1, s2))
    """

    if p1 >= p2 and s1 >= s2:
        return base_type.copy()

    if p2 >= p1 and s2 >= s1:
        return exp.DataType(
            this=base_type.this,
            expressions=[
                exp.DataTypeParam(this=exp.Literal.number(p2)),
                exp.DataTypeParam(this=exp.Literal.number(s2)),
            ],
        )

    result_scale = max(s1, s2)
    result_precision = max(p1, p2) + abs(s2 - s1)

    return exp.DataType(
        this=base_type.this,
        expressions=[
            exp.DataTypeParam(this=exp.Literal.number(result_precision)),
            exp.DataTypeParam(this=exp.Literal.number(result_scale)),
        ],
    )


def _annotate_nullif(self: TypeAnnotator, expression: exp.Nullif) -> exp.Nullif:
    """
    Annotate NULLIF with Snowflake-specific type coercion rules for parameterized numeric types.

    When both arguments are parameterized numeric types (e.g., DECIMAL(p, s)):
    - If one type dominates (p1 >= p2 AND s1 >= s2), use that type
    - Otherwise, compute new type with:
        - scale = max(s1, s2)
        - precision = max(p1, p2) + |s2 - s1|
    """

    self._annotate_args(expression)

    this_type = expression.this.type
    expr_type = expression.expression.type

    if not this_type or not expr_type:
        return self._annotate_by_args(expression, "this", "expression")

    # Snowflake specific type coercion for NULLIF with parameterized numeric types
    if (
        this_type.is_type(*exp.DataType.NUMERIC_TYPES)
        and expr_type.is_type(*exp.DataType.NUMERIC_TYPES)
        and this_type.expressions
        and expr_type.expressions
    ):
        p1, s1 = _extract_precision_scale(this_type)
        p2, s2 = _extract_precision_scale(expr_type)

        if p1 is not None and s1 is not None and p2 is not None and s2 is not None:
            result_type = _compute_nullif_result_type(this_type, p1, s1, p2, s2)
            if result_type:
                self._set_type(expression, result_type)
                return expression

    return self._annotate_by_args(expression, "this", "expression")


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
    exp.Nullif: {"annotator": _annotate_nullif},
    exp.Reverse: {"annotator": _annotate_reverse},
    exp.TimeAdd: {"annotator": _annotate_date_or_time_add},
    exp.TimestampFromParts: {"annotator": _annotate_timestamp_from_parts},
}
