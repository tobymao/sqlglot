import typing as t

from sqlglot import exp
from sqlglot.helper import subclasses

ExpressionMetadataType = t.Dict[type[exp.Expression], t.Dict[str, t.Any]]

TIMESTAMP_EXPRESSIONS = {
    exp.CurrentTimestamp,
    exp.StrToTime,
    exp.TimeStrToTime,
    exp.TimestampAdd,
    exp.TimestampSub,
    exp.UnixToTime,
}

EXPRESSION_METADATA: ExpressionMetadataType = {
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_binary(e)}
        for expr_type in subclasses(exp.__name__, exp.Binary)
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_unary(e)}
        for expr_type in subclasses(exp.__name__, (exp.Unary, exp.Alias))
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BIGINT}
        for expr_type in {
            exp.ApproxDistinct,
            exp.ArraySize,
            exp.CountIf,
            exp.Int64,
            exp.UnixDate,
            exp.UnixSeconds,
            exp.UnixMicros,
            exp.UnixMillis,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BINARY}
        for expr_type in {
            exp.FromBase32,
            exp.FromBase64,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.BOOLEAN}
        for expr_type in {
            exp.All,
            exp.Any,
            exp.Between,
            exp.Boolean,
            exp.Contains,
            exp.EndsWith,
            exp.Exists,
            exp.In,
            exp.IsInf,
            exp.IsNan,
            exp.LogicalAnd,
            exp.LogicalOr,
            exp.RegexpLike,
            exp.StartsWith,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DATE}
        for expr_type in {
            exp.CurrentDate,
            exp.Date,
            exp.DateFromParts,
            exp.DateStrToDate,
            exp.DiToDate,
            exp.LastDay,
            exp.StrToDate,
            exp.TimeStrToDate,
            exp.TsOrDsToDate,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DATETIME}
        for expr_type in {
            exp.CurrentDatetime,
            exp.Datetime,
            exp.DatetimeAdd,
            exp.DatetimeSub,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.DOUBLE}
        for expr_type in {
            exp.ApproxQuantile,
            exp.Avg,
            exp.Cbrt,
            exp.Cos,
            exp.Cot,
            exp.Exp,
            exp.Kurtosis,
            exp.Ln,
            exp.Log,
            exp.Pi,
            exp.Pow,
            exp.Quantile,
            exp.Radians,
            exp.Round,
            exp.SafeDivide,
            exp.Sin,
            exp.Sqrt,
            exp.Stddev,
            exp.StddevPop,
            exp.StddevSamp,
            exp.Tan,
            exp.ToDouble,
            exp.Variance,
            exp.VariancePop,
            exp.Skewness,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.INT}
        for expr_type in {
            exp.Ascii,
            exp.BitLength,
            exp.Ceil,
            exp.DatetimeDiff,
            exp.Getbit,
            exp.TimestampDiff,
            exp.TimeDiff,
            exp.Unicode,
            exp.DateToDi,
            exp.Levenshtein,
            exp.Length,
            exp.Sign,
            exp.StrPosition,
            exp.TsOrDiToDi,
            exp.Quarter,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.INTERVAL}
        for expr_type in {
            exp.Interval,
            exp.JustifyDays,
            exp.JustifyHours,
            exp.JustifyInterval,
            exp.MakeInterval,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.JSON}
        for expr_type in {
            exp.ParseJSON,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIME}
        for expr_type in {
            exp.CurrentTime,
            exp.Localtime,
            exp.Time,
            exp.TimeAdd,
            exp.TimeSub,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIMESTAMPLTZ}
        for expr_type in {
            exp.TimestampLtzFromParts,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.TIMESTAMPTZ}
        for expr_type in {
            exp.CurrentTimestampLTZ,
            exp.TimestampTzFromParts,
        }
    },
    **{expr_type: {"returns": exp.DataType.Type.TIMESTAMP} for expr_type in TIMESTAMP_EXPRESSIONS},
    **{
        expr_type: {"returns": exp.DataType.Type.TINYINT}
        for expr_type in {
            exp.Day,
            exp.DayOfMonth,
            exp.DayOfWeek,
            exp.DayOfWeekIso,
            exp.DayOfYear,
            exp.Month,
            exp.Week,
            exp.WeekOfYear,
            exp.Year,
            exp.YearOfWeek,
            exp.YearOfWeekIso,
        }
    },
    **{
        expr_type: {"returns": exp.DataType.Type.VARCHAR}
        for expr_type in {
            exp.ArrayToString,
            exp.Concat,
            exp.ConcatWs,
            exp.Chr,
            exp.CurrentCatalog,
            exp.Dayname,
            exp.DateToDateStr,
            exp.DPipe,
            exp.GroupConcat,
            exp.Initcap,
            exp.Lower,
            exp.MD5,
            exp.SHA,
            exp.SHA2,
            exp.Substring,
            exp.String,
            exp.TimeToStr,
            exp.TimeToTimeStr,
            exp.Trim,
            exp.ToBase32,
            exp.ToBase64,
            exp.TsOrDsToDateStr,
            exp.UnixToStr,
            exp.UnixToTimeStr,
            exp.Upper,
            exp.RawString,
            exp.Space,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Abs,
            exp.AnyValue,
            exp.ArrayConcatAgg,
            exp.ArrayReverse,
            exp.ArraySlice,
            exp.Filter,
            exp.HavingMax,
            exp.LastValue,
            exp.Limit,
            exp.Order,
            exp.SortArray,
            exp.Window,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions")}
        for expr_type in {
            exp.ArrayConcat,
            exp.Coalesce,
            exp.Greatest,
            exp.Least,
            exp.Max,
            exp.Min,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_array_element(e)}
        for expr_type in {
            exp.ArrayFirst,
            exp.ArrayLast,
        }
    },
    exp.Anonymous: {"annotator": lambda self, e: self._set_type(e, self.schema.get_udf_type(e))},
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_timeunit(e)}
        for expr_type in {
            exp.DateAdd,
            exp.DateSub,
            exp.DateTrunc,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._set_type(e, e.args["to"])}
        for expr_type in {
            exp.Cast,
            exp.TryCast,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_map(e)}
        for expr_type in {
            exp.Map,
            exp.VarMap,
        }
    },
    exp.Array: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions", array=True)},
    exp.ArrayAgg: {"annotator": lambda self, e: self._annotate_by_args(e, "this", array=True)},
    exp.Bracket: {"annotator": lambda self, e: self._annotate_bracket(e)},
    exp.Case: {
        "annotator": lambda self, e: self._annotate_by_args(
            e, *[if_expr.args["true"] for if_expr in e.args["ifs"]], "default"
        )
    },
    exp.Count: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.Type.BIGINT if e.args.get("big_int") else exp.DataType.Type.INT
        )
    },
    exp.DateDiff: {
        "annotator": lambda self, e: self._set_type(
            e, exp.DataType.Type.BIGINT if e.args.get("big_int") else exp.DataType.Type.INT
        )
    },
    exp.DataType: {"annotator": lambda self, e: self._set_type(e, e.copy())},
    exp.Div: {"annotator": lambda self, e: self._annotate_div(e)},
    exp.Distinct: {"annotator": lambda self, e: self._annotate_by_args(e, "expressions")},
    exp.Dot: {"annotator": lambda self, e: self._annotate_dot(e)},
    exp.Explode: {"annotator": lambda self, e: self._annotate_explode(e)},
    exp.Extract: {"annotator": lambda self, e: self._annotate_extract(e)},
    exp.HexString: {
        "annotator": lambda self, e: self._set_type(
            e,
            exp.DataType.Type.BIGINT if e.args.get("is_integer") else exp.DataType.Type.BINARY,
        )
    },
    exp.GenerateSeries: {
        "annotator": lambda self, e: self._annotate_by_args(e, "start", "end", "step", array=True)
    },
    exp.GenerateDateArray: {
        "annotator": lambda self, e: self._set_type(e, exp.DataType.build("ARRAY<DATE>"))
    },
    exp.GenerateTimestampArray: {
        "annotator": lambda self, e: self._set_type(e, exp.DataType.build("ARRAY<TIMESTAMP>"))
    },
    exp.If: {"annotator": lambda self, e: self._annotate_by_args(e, "true", "false")},
    exp.Literal: {"annotator": lambda self, e: self._annotate_literal(e)},
    exp.Null: {"returns": exp.DataType.Type.NULL},
    exp.Nullif: {"annotator": lambda self, e: self._annotate_by_args(e, "this", "expression")},
    exp.PropertyEQ: {"annotator": lambda self, e: self._annotate_by_args(e, "expression")},
    exp.Struct: {"annotator": lambda self, e: self._annotate_struct(e)},
    exp.Sum: {
        "annotator": lambda self, e: self._annotate_by_args(e, "this", "expressions", promote=True)
    },
    exp.Timestamp: {
        "annotator": lambda self, e: self._set_type(
            e,
            exp.DataType.Type.TIMESTAMPTZ if e.args.get("with_tz") else exp.DataType.Type.TIMESTAMP,
        )
    },
    exp.ToMap: {"annotator": lambda self, e: self._annotate_to_map(e)},
    exp.Unnest: {"annotator": lambda self, e: self._annotate_unnest(e)},
    exp.Subquery: {"annotator": lambda self, e: self._annotate_subquery(e)},
}
