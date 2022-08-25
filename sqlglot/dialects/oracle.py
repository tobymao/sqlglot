from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, no_ilike_sql
from sqlglot import transforms
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
            **transforms.UNALIAS_GROUP,
            exp.ILike: no_ilike_sql,
        }

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "TOP": TokenType.TOP,
        }
