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
        if super().can_quote(identifier, identify):
            return True

        parent = identifier.parent
        name = identifier.name.upper()
        if name in ("NULL", "TRUE", "FALSE"):
            if isinstance(parent, exp.Select) and identifier.arg_key == "expressions":
                return True
            # The first part can be parsed as a literal in both Column and Dot references.
            return (isinstance(parent, exp.Column) and identifier is parent.parts[0]) or (
                isinstance(parent, exp.Dot) and identifier is parent.this
            )

        if name not in DatabricksParser.RESERVED_TABLE_ALIASES:
            return False

        if isinstance(parent, exp.TableAlias):
            return identifier is parent.this and not isinstance(parent.parent, exp.CTE)

        # The as_ builder can attach an alias identifier directly to the relation.
        return identifier.arg_key == "alias" and isinstance(
            parent, (exp.Table, exp.Subquery, exp.UDTF)
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
