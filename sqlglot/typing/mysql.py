from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.typing import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot.optimizer.annotate_types import TypeAnnotator


COMPRESS_LONGBLOB_TYPES = {
    exp.DType.TEXT,
    exp.DType.MEDIUMTEXT,
    exp.DType.LONGTEXT,
    exp.DType.BLOB,
    exp.DType.MEDIUMBLOB,
    exp.DType.LONGBLOB,
    exp.DType.JSON,
}


COMPRESS_VARBINARY_TYPES = {
    exp.DType.CHAR,
    exp.DType.VARCHAR,
    exp.DType.BINARY,
    exp.DType.VARBINARY,
    exp.DType.TINYBLOB,
    exp.DType.ENUM,
    exp.DType.INT,
    exp.DType.BIGINT,
    exp.DType.DECIMAL,
    exp.DType.DOUBLE,
    exp.DType.DATE,
    exp.DType.DATETIME,
}


def _annotate_reverse(self: TypeAnnotator, expression: exp.Reverse) -> exp.Reverse:
    if expression.this.is_type(exp.DType.BINARY, exp.DType.VARBINARY, exp.DType.UNKNOWN):
        self._annotate_by_args(expression, "this")
    else:
        self._set_type(expression, exp.DType.VARCHAR)

    return expression


def _annotate_truncate(self: TypeAnnotator, expression: exp.Trunc) -> exp.Expr:
    if expression.this.is_type(*exp.DataType.TEXT_TYPES):
        return self._set_type(expression, exp.DType.DOUBLE)

    return self._annotate_by_args(expression, "this")


def _annotate_regexp_replace(self: TypeAnnotator, expression: exp.RegexpReplace) -> exp.Expr:
    args = (expression.this, expression.expression, expression.args.get("replacement"))

    has_binary = False
    for arg in args:
        if arg is not None:
            if arg.is_type(exp.DType.UNKNOWN):
                return self._set_type(expression, exp.DType.UNKNOWN)
            has_binary = has_binary or arg.is_type(*exp.DataType.BINARY_TYPES)

    return self._set_type(expression, exp.DType.LONGBLOB if has_binary else exp.DType.LONGTEXT)


def _annotate_compress(self: TypeAnnotator, expression: exp.Compress) -> exp.Expr:
    this = expression.this

    if this.is_type(*COMPRESS_VARBINARY_TYPES):
        return self._set_type(expression, exp.DType.VARBINARY)

    if this.is_type(*COMPRESS_LONGBLOB_TYPES):
        return self._set_type(expression, exp.DType.LONGBLOB)

    if this.is_type(exp.DType.TINYTEXT):
        return self._set_type(expression, exp.DType.BLOB)

    return self._set_type(expression, exp.DType.UNKNOWN)


def _annotate_bit_func(self: TypeAnnotator, expression: exp.Expression) -> exp.Expr:
    this = expression.this

    if this.is_type(exp.DType.UNKNOWN):
        return self._set_type(expression, exp.DType.UNKNOWN)

    if this.is_type(*exp.DataType.BINARY_TYPES):
        return self._set_type(expression, exp.DType.VARBINARY)

    return self._set_type(expression, exp.DType.UBIGINT)


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        expr_type: {"returns": exp.DType.DOUBLE}
        for expr_type in {
            exp.Atan2,
            exp.MatchAgainst,
            exp.StDistance,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DATETIME}
        for expr_type in {
            exp.CurrentTimestamp,
            exp.ConvertTimezone,
            exp.Localtime,
            exp.Localtimestamp,
            exp.UtcTimestamp,
        }
    },
    **{
        expr_type: {"returns": exp.DType.DATE}
        for expr_type in {
            exp.UtcDate,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARCHAR}
        for expr_type in {
            exp.Elt,
            exp.Hex,
            exp.NumberToStr,  # format()
            exp.Replace,
            exp.Stuff,  # insert function
            exp.SubstringIndex,
            exp.RegexpSubstr,
            exp.Collation,
            exp.JSONType,
            exp.Uuid,
        }
    },
    **{
        expr_type: {"returns": exp.DType.INT}
        for expr_type in {
            exp.Month,
            exp.Second,
            exp.Week,
            exp.Minute,
        }
    },
    **{
        expr_type: {"returns": exp.DType.BIGINT}
        for expr_type in {
            exp.RegexpInstr,
            exp.Grouping,
        }
    },
    **{
        expr_type: {"returns": exp.DType.TIME}
        for expr_type in {
            exp.TimeFromParts,
            exp.UtcTime,
        }
    },
    **{
        expr_type: {"returns": exp.DType.VARBINARY}
        for expr_type in {
            exp.Unhex,
        }
    },
    **{
        expr_type: {"returns": exp.DType.JSON}
        for expr_type in {
            exp.JSONObjectAgg,
            exp.JSONObject,
            exp.JSONExtract,
            exp.JSONKeys,
            exp.JSONArrayAppend,
            exp.JSONArrayInsert,
            exp.JSONRemove,
            exp.JSONSet,
        }
    },
    **{
        expr_type: {"returns": exp.DType.LONGTEXT}
        for expr_type in {
            exp.CurrentRole,
        }
    },
    **{
        expr_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for expr_type in {
            exp.Pad,
            exp.Left,
            exp.Right,
            exp.Lead,
        }
    },
    **{
        expr_type: {"annotator": _annotate_bit_func}
        for expr_type in {
            exp.BitwiseAndAgg,
            exp.BitwiseXorAgg,
            exp.BitwiseOrAgg,
        }
    },
    exp.Reverse: {"annotator": _annotate_reverse},
    exp.Trunc: {"annotator": _annotate_truncate},
    exp.RegexpReplace: {"annotator": _annotate_regexp_replace},
    exp.Compress: {"annotator": _annotate_compress},
}
