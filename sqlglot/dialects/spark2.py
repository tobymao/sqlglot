from __future__ import annotations

from sqlglot.dialects.hive import Hive
from sqlglot.generators.spark2 import Spark2Generator
from sqlglot.parsers.spark2 import Spark2Parser
from sqlglot.tokens import TokenType
from sqlglot.typing.spark2 import EXPRESSION_METADATA


class Spark2(Hive):
    ALTER_TABLE_SUPPORTS_CASCADE = False

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://spark.apache.org/docs/latest/api/sql/index.html#initcap
    # https://docs.databricks.com/aws/en/sql/language-manual/functions/initcap
    # https://github.com/apache/spark/blob/master/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L859-L905
    INITCAP_DEFAULT_DELIMITER_CHARS = " "

    class Tokenizer(Hive.Tokenizer):
        HEX_STRINGS = [("X'", "'"), ("x'", "'")]

        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
        }

    Parser = Spark2Parser

    Generator = Spark2Generator
