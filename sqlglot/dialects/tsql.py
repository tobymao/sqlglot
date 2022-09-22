from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class TSQL(Dialect):
    null_ordering = "nulls_are_small"
    time_format = "'yyyy-mm-dd hh:mm:ss'"

    class Tokenizer(Tokenizer):
        IDENTIFIERS = ['"', ("[", "]")]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "BIT": TokenType.BOOLEAN,
            "REAL": TokenType.FLOAT,
            "NTEXT": TokenType.TEXT,
            "SMALLDATETIME": TokenType.DATETIME,
            "DATETIMEOFFSET": TokenType.TIMESTAMPTZ,
            "TIME": TokenType.TIMESTAMP,
            "VARBINARY": TokenType.BINARY,
            "IMAGE": TokenType.IMAGE,
            "MONEY": TokenType.MONEY,
            "SMALLMONEY": TokenType.SMALLMONEY,
            "ROWVERSION": TokenType.ROWVERSION,
            "SQL_VARIANT": TokenType.SQL_VARIANT,
            "UNIQUEIDENTIFIER": TokenType.UNIQUEIDENTIFIER,
            "XML": TokenType.XML,
        }

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.DECIMAL: "NUMERIC",
        }
