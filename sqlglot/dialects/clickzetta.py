from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.tokens import Tokenizer

class ClickZetta(Spark):
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    DPIPE_IS_STRING_CONCAT = True
    STRICT_STRING_CONCAT = False
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = True
    NORMALIZE_FUNCTIONS = "upper"
    LOG_BASE_FIRST = None
    NULL_ORDERING = "nulls_are_small"
    TYPED_DIVISION = False
    SAFE_DIVISION = True
    CONCAT_COALESCE = False

    class Tokenizer(Spark.Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
        }

    class Parser(Spark.Parser):
        pass

    class Generator(Spark.Generator):

        TYPE_MAPPING = {
            **Spark.Generator.TYPE_MAPPING,
            exp.DataType.Type.MEDIUMTEXT: "STRING",
            exp.DataType.Type.LONGTEXT: "STRING",
            exp.DataType.Type.VARIANT: "STRING",
            exp.DataType.Type.ENUM: "STRING",
            exp.DataType.Type.ENUM16: "STRING",
            exp.DataType.Type.ENUM8: "STRING",
            # mysql unsigned types
            exp.DataType.Type.UINT: "INT",
            exp.DataType.Type.UTINYINT: "TINYINT",
            exp.DataType.Type.USMALLINT: "SMALLINT",
            exp.DataType.Type.UMEDIUMINT: "INT",
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UDECIMAL: "DECIMAL",
            # postgres serial types
            exp.DataType.Type.BIGSERIAL: "BIGINT",
            exp.DataType.Type.SERIAL: "INT",
            exp.DataType.Type.SMALLSERIAL: "SMALLINT",
            exp.DataType.Type.BIGDECIMAL: "DECIMAL",
        }

        PROPERTIES_LOCATION = {
            **Spark.Generator.PROPERTIES_LOCATION,
        }

        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
        }
