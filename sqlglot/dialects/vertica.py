from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.generator import Generator


class Vertica(Dialect):
    # Ignoring mypy errors for forward declarations
    class Tokenizer(Tokenizer):  # type: ignore
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "VARCHAR": TokenType.STRING,
        }

    # Ignoring mypy errors for forward declarations
    class Generator(Generator):  # type: ignore
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
        }

    TYPE_MAPPING = {
        exp.DataType.Type.TINYINT: "INT64",
        exp.DataType.Type.SMALLINT: "INT64",
        exp.DataType.Type.INT: "INT64",
        exp.DataType.Type.BIGINT: "INT64",
        exp.DataType.Type.DECIMAL: "NUMERIC",
        exp.DataType.Type.FLOAT: "FLOAT64",
        exp.DataType.Type.DOUBLE: "FLOAT64",
        exp.DataType.Type.BOOLEAN: "BOOL",
        exp.DataType.Type.TEXT: "STRING",
        exp.DataType.Type.DATE: "DATE",
        exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
    }
