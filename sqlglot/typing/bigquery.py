from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA, TIMESTAMP_EXPRESSIONS

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


def _annotate_math_functions(self: TypeAnnotator, expression: exp.Expression) -> exp.Expression:
    """
    Many BigQuery math functions such as CEIL, FLOOR etc follow this return type convention:
    +---------+---------+---------+------------+---------+
    |  INPUT  | INT64   | NUMERIC | BIGNUMERIC | FLOAT64 |
    +---------+---------+---------+------------+---------+
    |  OUTPUT | FLOAT64 | NUMERIC | BIGNUMERIC | FLOAT64 |
    +---------+---------+---------+------------+---------+
    """
    this: exp.Expression = expression.this

    self._set_type(
        expression,
        exp.DataType.Type.DOUBLE if this.is_type(*exp.DataType.INTEGER_TYPES) else this.type,
    )
    return expression


def _annotate_by_args_with_coerce(
    self: TypeAnnotator, expression: exp.Expression
) -> exp.Expression:
    """
    +------------+------------+------------+-------------+---------+
    | INPUT      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
    +------------+------------+------------+-------------+---------+
    | INT64      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
    | NUMERIC    | NUMERIC    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
    | BIGNUMERIC | BIGNUMERIC | BIGNUMERIC | BIGNUMERIC  | FLOAT64 |
    | FLOAT64    | FLOAT64    | FLOAT64    | FLOAT64     | FLOAT64 |
    +------------+------------+------------+-------------+---------+
    """
    self._set_type(expression, self._maybe_coerce(expression.this.type, expression.expression.type))
    return expression


def _annotate_by_args_approx_top(self: TypeAnnotator, expression: exp.ApproxTopK) -> exp.ApproxTopK:
    struct_type = exp.DataType(
        this=exp.DataType.Type.STRUCT,
        expressions=[expression.this.type, exp.DataType(this=exp.DataType.Type.BIGINT)],
        nested=True,
    )
    self._set_type(
        expression,
        exp.DataType(this=exp.DataType.Type.ARRAY, expressions=[struct_type], nested=True),
    )

    return expression


def _annotate_concat(self: TypeAnnotator, expression: exp.Concat) -> exp.Concat:
    annotated = self._annotate_by_args(expression, "expressions")

    # Args must be BYTES or types that can be cast to STRING, return type is either BYTES or STRING
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
    if not annotated.is_type(exp.DataType.Type.BINARY, exp.DataType.Type.UNKNOWN):
        self._set_type(annotated, exp.DataType.Type.VARCHAR)

    return annotated


def _annotate_array(self: TypeAnnotator, expression: exp.Array) -> exp.Array:
    array_args = expression.expressions

    # BigQuery behaves as follows:
    #
    # SELECT t, TYPEOF(t) FROM (SELECT 'foo') AS t            -- foo, STRUCT<STRING>
    # SELECT ARRAY(SELECT 'foo'), TYPEOF(ARRAY(SELECT 'foo')) -- foo, ARRAY<STRING>
    # ARRAY(SELECT ... UNION ALL SELECT ...) -- ARRAY<type from coerced projections>
    if len(array_args) == 1:
        unnested = array_args[0].unnest()
        projection_type: t.Optional[exp.DataType | exp.DataType.Type] = None

        # Handle ARRAY(SELECT ...) - single SELECT query
        if isinstance(unnested, exp.Select):
            if (
                (query_type := unnested.meta.get("query_type")) is not None
                and query_type.is_type(exp.DataType.Type.STRUCT)
                and len(query_type.expressions) == 1
                and isinstance(col_def := query_type.expressions[0], exp.ColumnDef)
                and (col_type := col_def.kind) is not None
                and not col_type.is_type(exp.DataType.Type.UNKNOWN)
            ):
                projection_type = col_type

        # Handle ARRAY(SELECT ... UNION ALL SELECT ...) - set operations
        elif isinstance(unnested, exp.SetOperation):
            # Get all column types for the SetOperation
            col_types = self._get_setop_column_types(unnested)
            # For ARRAY constructor, there should only be one projection
            # https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
            if col_types and unnested.left.selects:
                first_col_name = unnested.left.selects[0].alias_or_name
                projection_type = col_types.get(first_col_name)

        # If we successfully determine a projection type and it's not UNKNOWN, wrap it in ARRAY
        if projection_type and not projection_type.is_type(exp.DataType.Type.UNKNOWN):
            element_type = (
                projection_type.copy()
                if isinstance(projection_type, exp.DataType)
                else exp.DataType(this=projection_type)
            )
            array_type = exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[element_type],
                nested=True,
            )
            return self._set_type(expression, array_type)

    return self._annotate_by_args(expression, "expressions", array=True)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"annotator": lambda self, e: _annotate_math_functions(self, e)}
        for expr_type in {
            exp.Avg,
            exp.Ceil,
            exp.Exp,
            exp.Floor,
            exp.Ln,
            exp.Log,
            exp.Round,
            exp.Sqrt,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Abs,
            exp.ArgMax,
            exp.ArgMin,
            exp.DateTrunc,
            exp.DatetimeTrunc,
            exp.FirstValue,
            exp.GroupConcat,
            exp.IgnoreNulls,
            exp.JSONExtract,
            exp.Lead,
            exp.Left,
            exp.Lower,
            exp.NthValue,
            exp.Pad,
            exp.PercentileDisc,
            exp.RegexpExtract,
            exp.RegexpReplace,
            exp.Repeat,
            exp.Replace,
            exp.RespectNulls,
            exp.Reverse,
            exp.Right,
            exp.SafeNegate,
            exp.Sign,
            exp.Substring,
            exp.TimestampTrunc,
            exp.Translate,
            exp.Trim,
            exp.Upper,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.Ascii,
            exp.BitwiseAndAgg,
            exp.BitwiseCount,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
            exp.ByteLength,
            exp.DenseRank,
            exp.FarmFingerprint,
            exp.Grouping,
            exp.LaxInt64,
            exp.Length,
            exp.Ntile,
            exp.Rank,
            exp.RangeBucket,
            exp.RegexpInstr,
            exp.RowNumber,
            exp.Unicode,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BINARY}
        for expr_type in {
            exp.ByteString,
            exp.CodePointsToBytes,
            exp.MD5Digest,
            exp.SHA,
            exp.SHA2,
            exp.SHA1Digest,
            exp.SHA2Digest,
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BOOLEAN}
        for expr_type in {
            exp.IsInf,
            exp.IsNan,
            exp.JSONBool,
            exp.LaxBool,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DATETIME}
        for expr_type in {
            exp.ParseDatetime,
            exp.TimestampFromParts,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.Acos,
            exp.Acosh,
            exp.Asin,
            exp.Asinh,
            exp.Atan,
            exp.Atan2,
            exp.Atanh,
            exp.Cbrt,
            exp.Corr,
            exp.CosineDistance,
            exp.Cot,
            exp.Coth,
            exp.CovarPop,
            exp.CovarSamp,
            exp.Csc,
            exp.Csch,
            exp.CumeDist,
            exp.EuclideanDistance,
            exp.Float64,
            exp.LaxFloat64,
            exp.PercentRank,
            exp.Rand,
            exp.Sec,
            exp.Sech,
            exp.Sin,
            exp.Sinh,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.JSON}
        for expr_type in {
            exp.JSONArray,
            exp.JSONArrayAppend,
            exp.JSONArrayInsert,
            exp.JSONObject,
            exp.JSONRemove,
            exp.JSONSet,
            exp.JSONStripNulls,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIME}
        for expr_type in {
            exp.ParseTime,
            exp.TimeFromParts,
            exp.TimeTrunc,
            exp.TsOrDsToTime,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.CodePointsToString,
            exp.Format,
            exp.JSONExtractScalar,
            exp.JSONType,
            exp.LaxString,
            exp.LowerHex,
            exp.MD5,
            exp.NetHost,
            exp.Normalize,
            exp.SafeConvertBytesToString,
            exp.Soundex,
            exp.Uuid,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: _annotate_by_args_with_coerce(self, e)}
        for expr_type in {
            exp.PercentileCont,
            exp.SafeAdd,
            exp.SafeDivide,
            exp.SafeMultiply,
            exp.SafeSubtract,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this", array=True)}
        for expr_type in {
            exp.ApproxQuantiles,
            exp.JSONExtractArray,
            exp.RegexpExtractAll,
            exp.Split,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIMESTAMPTZ} for expr_type in TIMESTAMP_EXPRESSIONS
    },
    exp.DateFromUnixDate: {"returns": exp.DataType.Type.DATE},
    exp.ParseBignumeric: {"returns": exp.DataType.Type.BIGDECIMAL},
    exp.ParseNumeric: {"returns": exp.DataType.Type.DECIMAL},
    exp.ApproxTopK: {"annotator": lambda self, e: _annotate_by_args_approx_top(self, e)},
    exp.ApproxTopSum: {"annotator": lambda self, e: _annotate_by_args_approx_top(self, e)},
    exp.Array: {"annotator": _annotate_array},
    exp.ArrayConcat: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions")
    },
    exp.Concat: {"annotator": _annotate_concat},
    exp.GenerateTimestampArray: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("ARRAY<TIMESTAMP>", dialect="bigquery")
        )
    },
    exp.JSONFormat: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.Type.JSON if e.args.get("to_json") else exp.DataType.Type.VARCHAR
        )
    },
    exp.JSONKeysAtDepth: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("ARRAY<VARCHAR>", dialect="bigquery")
        )
    },
    exp.JSONValueArray: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("ARRAY<VARCHAR>", dialect="bigquery")
        )
    },
    exp.Lag: {"annotator": lambda self, e: self._annotate_by_args(e, "this", "default")},
    exp.ToCodePoints: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.build("ARRAY<BIGINT>", dialect="bigquery")
        )
    },
}
