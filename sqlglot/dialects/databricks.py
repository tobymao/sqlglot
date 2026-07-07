from __future__ import annotations

from copy import deepcopy
from collections import defaultdict

from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.generators.databricks import DatabricksGenerator
from sqlglot.parsers.databricks import DatabricksParser
from sqlglot.tokens import TokenType
from sqlglot.optimizer.annotate_types import TypeAnnotator


class Databricks(Spark):
    SAFE_DIVISION = False
    COPY_PARAMS_ARE_CSV = False

    # Inherits Spark's strict TIME_MAPPING; the padded inverse isn't inherited because the
    # metaclass recomputes INVERSE_TIME_MAPPING from each class's __dict__.
    INVERSE_TIME_MAPPING = {"%m": "MM", "%d": "dd"}

    COERCES_TO = defaultdict(set, deepcopy(TypeAnnotator.COERCES_TO))
    for text_type in exp.DataType.TEXT_TYPES:
        COERCES_TO[text_type] |= {
            *exp.DataType.NUMERIC_TYPES,
            *exp.DataType.TEMPORAL_TYPES,
            exp.DType.BINARY,
            exp.DType.BOOLEAN,
            exp.DType.INTERVAL,
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
