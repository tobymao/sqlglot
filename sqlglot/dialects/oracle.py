from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.transforms import unaliased_group_sql
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class Oracle(Dialect):
    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Group: unaliased_group_sql,
        }

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "TOP": TokenType.TOP,
        }
