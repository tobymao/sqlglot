from __future__ import annotations

from copy import deepcopy
from collections import defaultdict

from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.generators.databricks import DatabricksGenerator
from sqlglot.parsers.databricks import DatabricksParser
from sqlglot.tokens import TokenType
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.typing.databricks import EXPRESSION_METADATA


class Databricks(Spark):
    SAFE_DIVISION = False
    COPY_PARAMS_ARE_CSV = False
    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    COERCES_TO = defaultdict(set, deepcopy(TypeAnnotator.COERCES_TO))
    for text_type in exp.DataType.TEXT_TYPES:
        COERCES_TO[text_type] |= {
            *exp.DataType.NUMERIC_TYPES,
            *exp.DataType.TEMPORAL_TYPES,
            exp.DType.BINARY,
            exp.DType.BOOLEAN,
            exp.DType.INTERVAL,
        }

    def can_quote(self, identifier: exp.Identifier, identify: str | bool = "safe") -> bool:
        parent = identifier.parent
        return super().can_quote(identifier, identify) or (
            identifier.name.upper() == "CURRENT_PATH"
            and isinstance(parent, exp.Column)
            and not parent.is_star
            and identifier is parent.parts[0]
        )

    class JSONPathTokenizer(Spark.JSONPathTokenizer):
        IDENTIFIERS = ["`", '"']

    class Tokenizer(Spark.Tokenizer):
        KEYWORDS = {
            **Spark.Tokenizer.KEYWORDS,
            "?::": TokenType.QDCOLON,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "OPTIMIZE": TokenType.OPTIMIZE,
            "STREAM": TokenType.STREAM,
            "VOID": TokenType.VOID,
        }

    Parser = DatabricksParser

    Generator = DatabricksGenerator
