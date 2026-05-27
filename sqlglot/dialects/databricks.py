from __future__ import annotations

from copy import deepcopy
from collections import defaultdict

from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.generators.databricks import DatabricksGenerator
from sqlglot.parsers.databricks import DatabricksParser
from sqlglot.tokens import TokenType
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.typing.spark import EXPRESSION_METADATA as SPARK_EXPRESSION_METADATA


def _string_promotes(values: list[exp.Expression]) -> bool:
    """
    Whether a least-common-type function string-promotes given its value arguments.

    Databricks resolves COALESCE/IF/CASE via Spark's findWiderCommonType string
    promotion: when an argument is text and the rest are non-boolean/non-binary
    atomics, the common type is text. boolean+text and binary+text have no common
    type (query-time DATATYPE_MISMATCH), so we defer those to numeric widening.
    """
    return any(v.is_type(*exp.DataType.TEXT_TYPES) for v in values) and not any(
        v.is_type(exp.DType.BOOLEAN, exp.DType.BINARY) for v in values
    )


def _annotate_coalesce(self: TypeAnnotator, e: exp.Coalesce) -> exp.Coalesce:
    if _string_promotes([v for v in (e.this, *e.expressions) if v]):
        self._set_type(e, exp.DType.TEXT)
        return e
    return self._annotate_by_args(e, "this", "expressions", promote=True)


def _annotate_if(self: TypeAnnotator, e: exp.If) -> exp.If:
    if _string_promotes([v for v in (e.args.get("true"), e.args.get("false")) if v]):
        self._set_type(e, exp.DType.TEXT)
        return e
    return self._annotate_by_args(e, "true", "false", promote=True)


def _annotate_case(self: TypeAnnotator, e: exp.Case) -> exp.Case:
    thens = [if_expr.args["true"] for if_expr in e.args["ifs"]]
    if _string_promotes([v for v in (*thens, e.args.get("default")) if v]):
        self._set_type(e, exp.DType.TEXT)
        return e
    return self._annotate_by_args(e, *thens, "default")


class Databricks(Spark):
    SAFE_DIVISION = False
    COPY_PARAMS_ARE_CSV = False

    COERCES_TO = defaultdict(set, deepcopy(TypeAnnotator.COERCES_TO))
    for text_type in exp.DataType.TEXT_TYPES:
        COERCES_TO[text_type] |= {
            *exp.DataType.NUMERIC_TYPES,
            *exp.DataType.TEMPORAL_TYPES,
            exp.DType.BINARY,
            exp.DType.BOOLEAN,
            exp.DType.INTERVAL,
        }

    EXPRESSION_METADATA = {
        **SPARK_EXPRESSION_METADATA,
        exp.Coalesce: {"annotator": _annotate_coalesce},
        exp.If: {"annotator": _annotate_if},
        exp.Case: {"annotator": _annotate_case},
    }

    class JSONPathTokenizer(Spark.JSONPathTokenizer):
        IDENTIFIERS = ["`", '"']

    class Tokenizer(Spark.Tokenizer):
        KEYWORDS = {
            **Spark.Tokenizer.KEYWORDS,
            "STREAM": TokenType.STREAM,
            "VOID": TokenType.VOID,
        }

    Parser = DatabricksParser

    Generator = DatabricksGenerator
