from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlglot.tokens import Tokenizer, TokenType
    from sqlglot.generator import Generator


class Vertica(Dialect):
    # Ignoring mypy errors for forward declarations
    class Tokenizer(Tokenizer):  # type: ignore
        QUOTES = [
            "'",
            '"',
        ]  # Strings can be delimited by either single or double quotes
        IDENTIFIERS = ["`"]  # Identifiers can be delimited by backticks

        # Include Vertica specific keywords
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "VARCHAR": TokenType.STRING,
            "DATE": TokenType.DATE,
            "TIMESTAMP": TokenType.TIMESTAMP,
        }

    # Ignoring mypy errors for forward declarations
    class Generator(Generator):  # type: ignore
        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
        }

    # Mapping SqlGlot data types to Vertica data types
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
